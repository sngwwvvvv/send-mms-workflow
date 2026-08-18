from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import timedelta
from typing import Sequence
from zoneinfo import ZoneInfo

from .api import (
    AmbiguousPostOutcome,
    ExplicitApiFailure,
    MessageRecord,
    SendResponse,
    SensClient,
    TransientLookupError,
)
from .coordination import RUN_SETTINGS, RunCoordinator
from .event_log import (
    list_to_log_dict,
    message_result_to_log_dict,
    safe_error_dict,
    send_to_log_dict,
)
from .preflight import ApprovedWork
from .results import ResultFormatError, ResultRow
from .inputs import MESSAGE_BODY, MESSAGE_CONTENT, MESSAGE_SUBJECT


SEOUL = ZoneInfo("Asia/Seoul")
_SEND_ACTIONS = frozenset(
    {"RESUME_RESERVATION", "START_FRESH", "RETRY_EXPLICIT"}
)
_WORK_ACTIONS = _SEND_ACTIONS | frozenset({"HOLD_AMBIGUOUS", "RECONCILE"})
_SPLIT_MMS_CONTENT = "."
_SPLIT_LMS_CONTENT = (
    "[개업소연 안내]\n\n"
    "안녕하세요.\n"
    "국세청에서의 오랜 경험을 바탕으로 호연회계법인에서 새로운 출발을 하게 된 윤성중 세무사입니다.\n\n"
    "그동안 보내주신 관심에 감사드리며, 앞으로도 많은 응원과 격려 부탁드립니다.\n\n"
    "뜻깊은 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다."
)


@dataclass(frozen=True)
class PipelineResult:
    row: ResultRow
    needs_post: bool = False
    retry_not_before: float | None = None
    stage_success: bool = False


import sys
from typing import Literal

def _mask(number: str) -> str:
    return f"****{number[-4:]}" if len(number) >= 4 else number


def _log_single_line(receiving_number: str, progress: str, status: str) -> None:
    sys.stderr.write(
        f"[MMS 단건] {_mask(receiving_number)} | 진행상황: {progress} | 상태: {status}\n"
    )
    sys.stderr.flush()


class RecipientPipeline:
    def __init__(
        self,
        api: SensClient,
        coordinator: RunCoordinator,
        content_type: str,
    ):
        if type(content_type) is not str or content_type not in {"COMM", "AD"}:
            raise ResultFormatError("approved content type invalid")
        self.api = api
        self.coordinator = coordinator
        self.content_type = content_type

    def run(
        self,
        row: ResultRow,
        work: ApprovedWork,
        file_ids: Sequence[str] = (),
    ) -> PipelineResult:
        if not self._exact_equal(row.receiving_number, work.receiving_number):
            raise ResultFormatError("approved work recipient mismatch")
        action = work.action
        if type(action) is not str or action not in _WORK_ACTIONS:
            raise ResultFormatError("approved work action invalid")
        if action == "HOLD_AMBIGUOUS":
            return PipelineResult(row)
        if action == "RECONCILE":
            if not self._is_legal_reconciliation(row):
                return PipelineResult(row)
            return self._reconcile_single(row, work)

        if not self._is_legal_send(row, work):
            return PipelineResult(row)
        return self._send_single(row, work, tuple(file_ids))

    @classmethod
    def _is_pending_base(cls, row: ResultRow) -> bool:
        return (
            type(row.delivery_status) is str
            and row.delivery_status == "PENDING_CONFIRMATION"
            and type(row.is_sent) is str
            and row.is_sent == ""
            and row.error is None
            and cls._nonempty_exact_string(row.delivery_id)
            and type(row.attempts) is int
            and row.attempts >= 0
        )

    @classmethod
    def _is_legal_send(cls, row: ResultRow, work: ApprovedWork) -> bool:
        if not cls._is_pending_base(row):
            return False
        action = work.action
        if action in {"START_FRESH", "RESUME_RESERVATION"}:
            return (
                row.attempts == 0
                and type(row.request_id) is str
                and row.request_id == ""
                and type(row.message_id) is str
                and row.message_id == ""
            )
        return (
            action == "RETRY_EXPLICIT"
            and work.allow_retry_after_explicit_failure is True
            and 0 < row.attempts < RUN_SETTINGS.max_attempts
            and cls._nonempty_exact_string(row.request_id)
            and cls._nonempty_exact_string(row.message_id)
        )

    @classmethod
    def _is_legal_reconciliation(cls, row: ResultRow) -> bool:
        return (
            cls._is_pending_base(row)
            and 0 < row.attempts <= RUN_SETTINGS.max_attempts
            and cls._nonempty_exact_string(row.request_id)
            and type(row.message_id) is str
            and (row.message_id == "" or bool(row.message_id.strip()))
        )

    @staticmethod
    def _state(row: ResultRow) -> dict:
        return {
            "delivery_status": row.delivery_status,
            "is_sent": row.is_sent,
            "attempts": row.attempts,
            "request_id": row.request_id,
            "message_id": row.message_id,
            "error": row.error,
        }

    def _checkpoint(self, previous: ResultRow, row: ResultRow) -> ResultRow:
        self.coordinator.commit(row, expected=previous)
        self.coordinator.write(
            "RESULT_STATE_CHANGED",
            delivery_id=row.delivery_id,
            state=self._state(row),
        )
        return row

    def _execute_stage(
        self,
        stage: Literal["MMS", "LMS"],
        row: ResultRow,
        work: ApprovedWork,
        file_ids: tuple[str, ...],
        *,
        content: str | None = None,
        retry_not_before: float | None = None,
        subject: str | None = None,
    ) -> PipelineResult:
        current = row
        if work.action == "RETRY_EXPLICIT" and retry_not_before is None:
            retry_not_before = work.retry_not_before
            if retry_not_before is None:
                retry_not_before = (
                    self.coordinator.clock.monotonic()
                    + RUN_SETTINGS.retry_delay_seconds
                )
            elif type(retry_not_before) not in {int, float}:
                raise ResultFormatError("approved retry deadline invalid")

        while current.attempts < RUN_SETTINGS.max_attempts:
            if retry_not_before is not None:
                self._wait_for_retry(retry_not_before)
            self.coordinator.before_api_call()
            attempt_row = replace(
                current,
                delivery_status="PENDING_CONFIRMATION",
                is_sent="",
                attempts=current.attempts + 1,
                request_id="",
                message_id="",
                error=None,
            )
            current = self._checkpoint(current, attempt_row)
            self.coordinator.write(
                "SEND_ATTEMPT_STARTED",
                delivery_id=current.delivery_id,
                attempt=current.attempts,
                stage=stage,
            )
            stage_str = "MMS 1단계" if stage == "MMS" else "LMS 2단계"
            desc_str = "사진" if stage == "MMS" else "본문"
            sys.stderr.write(f"[{stage_str}] {_mask(current.receiving_number)}: {desc_str} 발송 요청 (시도 {current.attempts}/3)\n")
            sys.stderr.flush()

            self.coordinator.before_api_call()
            started_at = self._seoul_time(self.coordinator.clock.now())
            attempt_started = self.coordinator.clock.monotonic()
            try:
                if stage == "MMS":
                    response = self.api.send_mms(
                        current.receiving_number,
                        file_ids,
                        content_type=self.content_type,
                        content=content if content is not None else " ",
                    )
                else:
                    response = self.api.send_lms(
                        current.receiving_number,
                        content_type=self.content_type,
                        content=content if content is not None else MESSAGE_BODY,
                        subject=subject,
                    )
            except ExplicitApiFailure as failure:
                if failure.http_status == 429:
                    self.coordinator.record_429()
                outcome = self._explicit_failure(
                    current,
                    work,
                    failure.status,
                    failure.message,
                    stage,
                )
                self.coordinator.write(
                    "SEND_RESPONSE",
                    delivery_id=outcome.row.delivery_id,
                    attempt=outcome.row.attempts,
                    stage=stage,
                    api="send",
                    http_status=failure.http_status,
                    response=failure.response or None,
                    error=safe_error_dict(failure.status, failure.message),
                )
                if not outcome.needs_post:
                    return outcome
                self._log_retry(outcome.row, stage)
                current = outcome.row
                retry_not_before = outcome.retry_not_before
                continue
            except AmbiguousPostOutcome:
                deadline = attempt_started + RUN_SETTINGS.confirmation_timeout_seconds
                self._log_ambiguous(current, stage)
                return self._recover_ambiguous(current, work, started_at, deadline, stage)

            if not self._is_exact_acceptance(response):
                deadline = attempt_started + RUN_SETTINGS.confirmation_timeout_seconds
                self._log_ambiguous(current, stage)
                return self._recover_ambiguous(current, work, started_at, deadline, stage)

            deadline = (
                self.coordinator.clock.monotonic()
                + RUN_SETTINGS.confirmation_timeout_seconds
            )
            accepted = replace(current, request_id=response.request_id)
            current = self._checkpoint(current, accepted)
            self.coordinator.write(
                "SEND_RESPONSE",
                delivery_id=current.delivery_id,
                attempt=current.attempts,
                stage=stage,
                api="send",
                http_status=response.http_status,
                request_id=current.request_id,
                response=send_to_log_dict(response),
            )
            outcome = self._resolve_request(current, work, deadline, stage)
            if not outcome.needs_post:
                return outcome
            self._log_retry(outcome.row, stage)
            current = outcome.row
            retry_not_before = outcome.retry_not_before
        return PipelineResult(current)

    def _wait_for_retry(self, deadline: float) -> None:
        self.coordinator.raise_if_stopped()
        remaining = deadline - self.coordinator.clock.monotonic()
        if remaining > 0:
            self.coordinator.clock.sleep(remaining)
        self.coordinator.raise_if_stopped()

    @staticmethod
    def _is_exact_acceptance(response) -> bool:
        return (
            type(response) is SendResponse
            and type(response.http_status) is int
            and 200 <= response.http_status < 300
            and type(response.status_code) is str
            and response.status_code == "202"
            and RecipientPipeline._nonempty_exact_string(response.request_id)
        )

    def _reconcile(self, row: ResultRow, work: ApprovedWork) -> PipelineResult:
        if not self._nonempty_exact_string(row.request_id):
            return PipelineResult(row)
        deadline = (
            self.coordinator.clock.monotonic()
            + RUN_SETTINGS.confirmation_timeout_seconds
        )
        current = row
        if not self._nonempty_exact_string(current.message_id):
            if type(current.message_id) is not str or current.message_id:
                return PipelineResult(current)
            try:
                response = self.api.list_by_request(current.request_id)
            except TransientLookupError as failure:
                self._record_lookup_failure(current, "list", failure, stage="MMS")
                return PipelineResult(current)

            matches = [
                record
                for record in response.messages
                if self._request_match_any_type(record, current)
            ]
            if len(matches) == 1:
                current = replace(current, message_id=matches[0].message_id)
                current = self._checkpoint(row, current)
            stage = matches[0].message_type if len(matches) == 1 else "MMS"
            self._log_list_response(current, response, stage)
            if not self._nonempty_exact_string(current.message_id):
                return PipelineResult(current)

        try:
            response = self.api.get_message(current.message_id)
        except TransientLookupError as failure:
            self._record_lookup_failure(current, "get", failure, stage="MMS")
            return PipelineResult(current)

        stage = response.message.message_type
        if stage not in {"MMS", "LMS"}:
            return PipelineResult(current)

        return self._poll_message(current, work, deadline, stage, initial_response=response)

    def _recover_ambiguous(
        self,
        row: ResultRow,
        work: ApprovedWork,
        started_at,
        deadline: float,
        stage: Literal["MMS", "LMS"],
    ) -> PipelineResult:
        if not self._before_lookup(deadline):
            return PipelineResult(row)
        start = (started_at - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        end = (
            self._seoul_time(self.coordinator.clock.now()) + timedelta(seconds=30)
        ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            response = self.api.list_by_time_and_recipient(
                start,
                end,
                row.receiving_number,
                message_type=stage,
            )
        except TransientLookupError as failure:
            self._record_lookup_failure(row, "list", failure, stage)
            return PipelineResult(row)

        matches = [
            record
            for record in response.messages
            if self._time_match(record, row, stage)
        ]
        current = row
        if len(matches) == 1:
            with_request = replace(row, request_id=matches[0].request_id)
            current = self._checkpoint(row, with_request)
            with_message = replace(current, message_id=matches[0].message_id)
            current = self._checkpoint(current, with_message)
        self._log_list_response(current, response, stage)
        if len(matches) != 1:
            return PipelineResult(current)
        return self._poll_message(current, work, deadline, stage)

    def _resolve_request(
        self,
        row: ResultRow,
        work: ApprovedWork,
        deadline: float,
        stage: Literal["MMS", "LMS"],
    ) -> PipelineResult:
        current = row
        while not self._nonempty_exact_string(current.message_id):
            if not self._before_lookup(deadline):
                return PipelineResult(current)
            try:
                response = self.api.list_by_request(current.request_id)
            except TransientLookupError as failure:
                self._record_lookup_failure(current, "list", failure, stage)
                if not self._sleep_for_poll(deadline):
                    return PipelineResult(current)
                continue

            matches = [
                record
                for record in response.messages
                if self._request_match(record, current, stage)
            ]
            previous = current
            if len(matches) == 1:
                current = replace(current, message_id=matches[0].message_id)
                current = self._checkpoint(previous, current)
            self._log_list_response(current, response, stage)
            if current.message_id:
                return self._poll_message(current, work, deadline, stage)
            if not self._sleep_for_poll(deadline):
                return PipelineResult(current)
        return self._poll_message(current, work, deadline, stage)

    def _poll_message(
        self,
        row: ResultRow,
        work: ApprovedWork,
        deadline: float,
        stage: Literal["MMS", "LMS"],
        initial_response: MessageResultResponse | None = None,
    ) -> PipelineResult:
        current = row
        use_initial = initial_response is not None
        while True:
            if use_initial:
                response = initial_response
                use_initial = False
            else:
                if not self._before_lookup(deadline):
                    return PipelineResult(current)
                try:
                    response = self.api.get_message(current.message_id)
                except TransientLookupError as failure:
                    self._record_lookup_failure(current, "get", failure, stage)
                    if not self._sleep_for_poll(deadline):
                        return PipelineResult(current)
                    continue

            message = response.message
            correlated = self._get_match(message, current)
            outcome = None
            if (
                correlated
                and type(message.status) is str
                and message.status == "COMPLETED"
                and type(message.status_name) is str
                and message.status_name == "success"
            ):
                if stage == "MMS":
                    self.coordinator.write(
                        "STAGE_COMPLETED",
                        delivery_id=current.delivery_id,
                        attempt=current.attempts,
                        stage="MMS",
                    )
                    sys.stderr.write(f"[MMS 1단계] {_mask(current.receiving_number)}: 통신사 성공 확인 (COMPLETED)\n")
                    sys.stderr.flush()
                    outcome = PipelineResult(current, stage_success=True)
                else:
                    outcome = self._sent(current)
                    sys.stderr.write(f"[LMS 2단계] {_mask(current.receiving_number)}: 최종 전송 완료 [SENT]\n")
                    sys.stderr.flush()
            elif (
                correlated
                and type(message.status) is str
                and message.status == "COMPLETED"
                and type(message.status_name) is str
                and message.status_name == "fail"
            ):
                outcome = self._explicit_failure(
                    current,
                    work,
                    message.status_code,
                    message.status_message,
                    stage,
                )
            elif not (
                correlated
                and type(message.status) is str
                and message.status in {"READY", "PROCESSING"}
            ):
                outcome = PipelineResult(current)

            event_row = outcome.row if outcome is not None else current
            self.coordinator.write(
                "DELIVERY_POLL_RESPONSE",
                delivery_id=event_row.delivery_id,
                attempt=event_row.attempts,
                stage=stage,
                api="get",
                http_status=response.http_status,
                request_id=current.request_id,
                message_id=current.message_id,
                response=message_result_to_log_dict(response),
            )
            if outcome is not None:
                return outcome
            if not self._sleep_for_poll(deadline):
                break
        return PipelineResult(current)

    def _sent(self, row: ResultRow) -> PipelineResult:
        sent = replace(
            row,
            delivery_status="SENT",
            is_sent="true",
            error=None,
        )
        return PipelineResult(self._checkpoint(row, sent), stage_success=True)

    def _explicit_failure(
        self,
        row: ResultRow,
        work: ApprovedWork,
        status,
        message,
        stage: Literal["MMS", "LMS"],
    ) -> PipelineResult:
        if row.attempts >= RUN_SETTINGS.max_attempts:
            failed = replace(
                row,
                delivery_status="FAILED",
                is_sent="false",
                error=safe_error_dict(status, message),
            )
            if stage == "MMS":
                sys.stderr.write(f"[MMS 1단계] {_mask(row.receiving_number)}: 실패 (LMS 취소) [FAILED]\n")
            else:
                sys.stderr.write(f"[LMS 2단계] {_mask(row.receiving_number)}: 실패 [FAILED]\n")
            sys.stderr.flush()
            return PipelineResult(self._checkpoint(row, failed))
        if not work.allow_retry_after_explicit_failure:
            return PipelineResult(row)
        return PipelineResult(
            row,
            needs_post=True,
            retry_not_before=(
                self.coordinator.clock.monotonic()
                + RUN_SETTINGS.retry_delay_seconds
            ),
        )

    def _before_lookup(self, deadline: float) -> bool:
        if self.coordinator.clock.monotonic() >= deadline:
            return False
        self.coordinator.before_api_call()
        return self.coordinator.clock.monotonic() < deadline

    def _sleep_for_poll(self, deadline: float) -> bool:
        remaining = deadline - self.coordinator.clock.monotonic()
        if remaining <= 0:
            return False
        self.coordinator.clock.sleep(
            min(RUN_SETTINGS.poll_interval_seconds, remaining)
        )
        return self.coordinator.clock.monotonic() < deadline

    def _record_lookup_failure(
        self,
        row: ResultRow,
        api: str,
        failure: TransientLookupError,
        stage: Literal["MMS", "LMS"],
    ) -> None:
        if failure.http_status == 429:
            self.coordinator.record_429()
        self.coordinator.write(
            "API_LOOKUP_ERROR",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            stage=stage,
            api=api,
            http_status=failure.http_status,
            request_id=row.request_id or None,
            message_id=row.message_id or None,
            error=safe_error_dict(
                "TRANSIENT_LOOKUP_ERROR",
                "lookup temporarily unavailable",
            ),
        )

    def _log_retry(self, row: ResultRow, stage: Literal["MMS", "LMS"]) -> None:
        self.coordinator.write(
            "RETRY_SCHEDULED",
            delivery_id=row.delivery_id,
            attempt=row.attempts + 1,
            stage=stage,
        )

    def _log_ambiguous(self, row: ResultRow, stage: Literal["MMS", "LMS"]) -> None:
        self.coordinator.write(
            "SEND_AMBIGUOUS_OUTCOME",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            stage=stage,
            api="send",
            error=safe_error_dict(
                "AMBIGUOUS_POST_OUTCOME",
                "send result could not be confirmed",
            ),
        )

    def _log_list_response(self, row, response, stage: Literal["MMS", "LMS"]) -> None:
        self.coordinator.write(
            "MESSAGE_LIST_RESPONSE",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            stage=stage,
            api="list",
            http_status=response.http_status,
            request_id=row.request_id or None,
            message_id=row.message_id or None,
            response=list_to_log_dict(response),
        )

    @staticmethod
    def _exact_equal(actual, expected) -> bool:
        return (
            type(actual) is str
            and type(expected) is str
            and actual == expected
        )

    @staticmethod
    def _nonempty_exact_string(value) -> bool:
        return type(value) is str and bool(value.strip())

    @classmethod
    def _time_match(cls, record: MessageRecord, row: ResultRow, stage: str) -> bool:
        return (
            cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type == stage
            and cls._nonempty_exact_string(record.request_id)
            and cls._nonempty_exact_string(record.message_id)
        )

    @classmethod
    def _request_match(cls, record: MessageRecord, row: ResultRow, stage: str) -> bool:
        return (
            cls._exact_equal(record.request_id, row.request_id)
            and cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type == stage
            and cls._nonempty_exact_string(record.message_id)
        )

    @classmethod
    def _request_match_any_type(cls, record: MessageRecord, row: ResultRow) -> bool:
        return (
            cls._exact_equal(record.request_id, row.request_id)
            and cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type in {"MMS", "LMS"}
            and cls._nonempty_exact_string(record.message_id)
        )

    @classmethod
    def _get_match(cls, record: MessageRecord, row: ResultRow) -> bool:
        return (
            cls._exact_equal(record.request_id, row.request_id)
            and cls._exact_equal(record.message_id, row.message_id)
            and cls._exact_equal(record.to, row.receiving_number)
        )

    @staticmethod
    def _seoul_time(value):
        if value.tzinfo is None:
            return value.replace(tzinfo=SEOUL)
        return value.astimezone(SEOUL)

    def _send_single(
        self,
        row: ResultRow,
        work: ApprovedWork,
        file_ids: tuple[str, ...],
    ) -> PipelineResult:
        current = row
        retry_not_before = None
        if work.action == "RETRY_EXPLICIT":
            retry_not_before = work.retry_not_before
            if retry_not_before is None:
                retry_not_before = (
                    self.coordinator.clock.monotonic()
                    + RUN_SETTINGS.retry_delay_seconds
                )
            elif type(retry_not_before) not in {int, float}:
                raise ResultFormatError("approved retry deadline invalid")
        while current.attempts < RUN_SETTINGS.max_attempts:
            if retry_not_before is not None:
                self._wait_for_retry(retry_not_before)
            self.coordinator.before_api_call()
            attempt_row = replace(
                current,
                delivery_status="PENDING_CONFIRMATION",
                is_sent="",
                attempts=current.attempts + 1,
                request_id="",
                message_id="",
                error=None,
            )
            current = self._checkpoint(current, attempt_row)
            self.coordinator.write(
                "SEND_ATTEMPT_STARTED",
                delivery_id=current.delivery_id,
                attempt=current.attempts,
            )
            _log_single_line(
                current.receiving_number,
                "전송 요청",
                f"{current.attempts}/3",
            )
            self.coordinator.before_api_call()
            started_at = self._seoul_time(self.coordinator.clock.now())
            attempt_started = self.coordinator.clock.monotonic()
            try:
                response = self.api.send_one(
                    current.receiving_number,
                    file_ids,
                    content_type=self.content_type,
                    content=MESSAGE_CONTENT,
                    subject=MESSAGE_SUBJECT,
                )
            except ExplicitApiFailure as failure:
                if failure.http_status == 429:
                    self.coordinator.record_429()
                outcome = self._explicit_failure_single(
                    current,
                    work,
                    failure.status,
                    failure.message,
                )
                self.coordinator.write(
                    "SEND_RESPONSE",
                    delivery_id=outcome.row.delivery_id,
                    attempt=outcome.row.attempts,
                    api="send",
                    http_status=failure.http_status,
                    response=failure.response or None,
                    error=safe_error_dict(failure.status, failure.message),
                )
                if not outcome.needs_post:
                    return outcome
                self._log_retry_single(outcome.row)
                current = outcome.row
                retry_not_before = outcome.retry_not_before
                continue
            except AmbiguousPostOutcome:
                deadline = attempt_started + RUN_SETTINGS.confirmation_timeout_seconds
                self._log_ambiguous_single(current)
                return self._recover_ambiguous_single(current, work, started_at, deadline)

            if not self._is_exact_acceptance(response):
                deadline = attempt_started + RUN_SETTINGS.confirmation_timeout_seconds
                self._log_ambiguous_single(current)
                return self._recover_ambiguous_single(current, work, started_at, deadline)

            deadline = (
                self.coordinator.clock.monotonic()
                + RUN_SETTINGS.confirmation_timeout_seconds
            )
            accepted = replace(current, request_id=response.request_id)
            current = self._checkpoint(current, accepted)
            self.coordinator.write(
                "SEND_RESPONSE",
                delivery_id=current.delivery_id,
                attempt=current.attempts,
                api="send",
                http_status=response.http_status,
                request_id=current.request_id,
                response=send_to_log_dict(response),
            )
            _log_single_line(
                current.receiving_number,
                "접수 확인",
                response.status_code,
            )
            outcome = self._resolve_request_single(current, work, deadline)
            if not outcome.needs_post:
                return outcome
            self._log_retry_single(outcome.row)
            current = outcome.row
            retry_not_before = outcome.retry_not_before
        return PipelineResult(current)

    def _reconcile_single(self, row: ResultRow, work: ApprovedWork) -> PipelineResult:
        if not self._nonempty_exact_string(row.request_id):
            return PipelineResult(row)
        deadline = (
            self.coordinator.clock.monotonic()
            + RUN_SETTINGS.confirmation_timeout_seconds
        )
        if self._nonempty_exact_string(row.message_id):
            return self._poll_message_single(row, work, deadline)
        if type(row.message_id) is not str or row.message_id:
            return PipelineResult(row)
        return self._resolve_request_single(row, work, deadline)

    def _recover_ambiguous_single(
        self,
        row: ResultRow,
        work: ApprovedWork,
        started_at,
        deadline: float,
    ) -> PipelineResult:
        if not self._before_lookup(deadline):
            return PipelineResult(row)
        start = (started_at - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        end = (
            self._seoul_time(self.coordinator.clock.now()) + timedelta(seconds=30)
        ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            response = self.api.list_by_time_and_recipient(
                start,
                end,
                row.receiving_number,
                message_type="MMS",
            )
        except TransientLookupError as failure:
            self._record_lookup_failure_single(row, "list", failure)
            return PipelineResult(row)

        matches = [
            record
            for record in response.messages
            if self._time_match_single(record, row)
        ]
        current = row
        if len(matches) == 1:
            with_request = replace(row, request_id=matches[0].request_id)
            current = self._checkpoint(row, with_request)
            with_message = replace(current, message_id=matches[0].message_id)
            current = self._checkpoint(current, with_message)
        self._log_list_response_single(current, response)
        if len(matches) != 1:
            return PipelineResult(current)
        return self._poll_message_single(current, work, deadline)

    def _resolve_request_single(
        self,
        row: ResultRow,
        work: ApprovedWork,
        deadline: float,
    ) -> PipelineResult:
        current = row
        while not self._nonempty_exact_string(current.message_id):
            if not self._before_lookup(deadline):
                return PipelineResult(current)
            _log_single_line(current.receiving_number, "접수 확인", "조회 중")
            try:
                response = self.api.list_by_request(current.request_id)
            except TransientLookupError as failure:
                self._record_lookup_failure_single(current, "list", failure)
                if not self._sleep_for_poll(deadline):
                    return PipelineResult(current)
                continue

            matches = [
                record
                for record in response.messages
                if self._request_match_single(record, current)
            ]
            previous = current
            if len(matches) == 1:
                current = replace(current, message_id=matches[0].message_id)
                current = self._checkpoint(previous, current)
            self._log_list_response_single(current, response)
            if current.message_id:
                return self._poll_message_single(current, work, deadline)
            if not self._sleep_for_poll(deadline):
                return PipelineResult(current)
        return self._poll_message_single(current, work, deadline)

    def _poll_message_single(
        self,
        row: ResultRow,
        work: ApprovedWork,
        deadline: float,
    ) -> PipelineResult:
        current = row
        while self.coordinator.clock.monotonic() < deadline:
            if not self._before_lookup(deadline):
                return PipelineResult(current)
            _log_single_line(current.receiving_number, "최종 확인", "조회 중")
            try:
                response = self.api.get_message(current.message_id)
            except TransientLookupError as failure:
                self._record_lookup_failure_single(current, "get", failure)
                if not self._sleep_for_poll(deadline):
                    return PipelineResult(current)
                continue

            message = response.message
            correlated = self._get_match(message, current)
            outcome = None
            if (
                correlated
                and type(message.status) is str
                and message.status == "COMPLETED"
                and type(message.status_name) is str
                and message.status_name == "success"
            ):
                outcome = self._sent_single(current)
            elif (
                correlated
                and type(message.status) is str
                and message.status == "COMPLETED"
                and type(message.status_name) is str
                and message.status_name == "fail"
            ):
                outcome = self._explicit_failure_single(
                    current,
                    work,
                    message.status_code,
                    message.status_message,
                )
            elif not (
                correlated
                and type(message.status) is str
                and message.status in {"READY", "PROCESSING"}
            ):
                outcome = PipelineResult(current)

            event_row = outcome.row if outcome is not None else current
            self.coordinator.write(
                "DELIVERY_POLL_RESPONSE",
                delivery_id=event_row.delivery_id,
                attempt=event_row.attempts,
                api="get",
                http_status=response.http_status,
                request_id=current.request_id,
                message_id=current.message_id,
                response=message_result_to_log_dict(response),
            )
            if correlated and type(message.status) is str:
                if message.status in {"READY", "PROCESSING"}:
                    _log_single_line(
                        current.receiving_number,
                        "최종 확인",
                        message.status,
                    )
            if outcome is not None:
                return outcome
            if not self._sleep_for_poll(deadline):
                break
        return PipelineResult(current)

    def _sent_single(self, row: ResultRow) -> PipelineResult:
        sent = replace(
            row,
            delivery_status="SENT",
            is_sent="true",
            error=None,
        )
        _log_single_line(row.receiving_number, "완료", "SENT")
        return PipelineResult(self._checkpoint(row, sent))

    def _explicit_failure_single(
        self,
        row: ResultRow,
        work: ApprovedWork,
        status,
        message,
    ) -> PipelineResult:
        if row.attempts >= RUN_SETTINGS.max_attempts:
            failed = replace(
                row,
                delivery_status="FAILED",
                is_sent="false",
                error=safe_error_dict(status, message),
            )
            _log_single_line(row.receiving_number, "완료", "FAILED")
            return PipelineResult(self._checkpoint(row, failed))
        if not work.allow_retry_after_explicit_failure:
            return PipelineResult(row)
        return PipelineResult(
            row,
            needs_post=True,
            retry_not_before=(
                self.coordinator.clock.monotonic()
                + RUN_SETTINGS.retry_delay_seconds
            ),
        )

    def _record_lookup_failure_single(
        self,
        row: ResultRow,
        api: str,
        failure: TransientLookupError,
    ) -> None:
        if failure.http_status == 429:
            self.coordinator.record_429()
        _log_single_line(row.receiving_number, "조회 중", "실패")
        self.coordinator.write(
            "API_LOOKUP_ERROR",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            api=api,
            http_status=failure.http_status,
            request_id=row.request_id or None,
            message_id=row.message_id or None,
            error=safe_error_dict(
                "TRANSIENT_LOOKUP_ERROR",
                "lookup temporarily unavailable",
            ),
        )

    def _log_retry_single(self, row: ResultRow) -> None:
        _log_single_line(row.receiving_number, "재시도 대기", f"{row.attempts + 1}/3")
        self.coordinator.write(
            "RETRY_SCHEDULED",
            delivery_id=row.delivery_id,
            attempt=row.attempts + 1,
        )

    def _log_ambiguous_single(self, row: ResultRow) -> None:
        _log_single_line(row.receiving_number, "접수 확인 불가", "미확정")
        self.coordinator.write(
            "SEND_AMBIGUOUS_OUTCOME",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            api="send",
            error=safe_error_dict(
                "AMBIGUOUS_POST_OUTCOME",
                "send result could not be confirmed",
            ),
        )

    def _log_list_response_single(self, row, response) -> None:
        _log_single_line(row.receiving_number, "접수 확인", "조회 중")
        self.coordinator.write(
            "MESSAGE_LIST_RESPONSE",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            api="list",
            http_status=response.http_status,
            request_id=row.request_id or None,
            message_id=row.message_id or None,
            response=list_to_log_dict(response),
        )

    @classmethod
    def _time_match_single(cls, record: MessageRecord, row: ResultRow) -> bool:
        return (
            cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type == "MMS"
            and cls._nonempty_exact_string(record.request_id)
            and cls._nonempty_exact_string(record.message_id)
        )

    @classmethod
    def _request_match_single(cls, record: MessageRecord, row: ResultRow) -> bool:
        return (
            cls._exact_equal(record.request_id, row.request_id)
            and cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type == "MMS"
            and cls._nonempty_exact_string(record.message_id)
        )
