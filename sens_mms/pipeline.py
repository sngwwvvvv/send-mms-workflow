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


SEOUL = ZoneInfo("Asia/Seoul")
_SEND_ACTIONS = frozenset(
    {"RESUME_RESERVATION", "START_FRESH", "RETRY_EXPLICIT"}
)
_WORK_ACTIONS = _SEND_ACTIONS | frozenset({"HOLD_AMBIGUOUS", "RECONCILE"})


@dataclass(frozen=True)
class PipelineResult:
    row: ResultRow
    needs_post: bool = False
    retry_not_before: float | None = None


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
            return self._reconcile(row, work)
        if not self._is_legal_send(row, work):
            return PipelineResult(row)
        return self._send(row, work, tuple(file_ids))

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

    def _send(
        self,
        row: ResultRow,
        work: ApprovedWork,
        file_ids: tuple[str, ...],
    ) -> PipelineResult:
        current = row
        retry_not_before = (
            self.coordinator.clock.monotonic()
            + RUN_SETTINGS.retry_delay_seconds
            if work.action == "RETRY_EXPLICIT"
            else None
        )
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
            self.coordinator.raise_if_stopped()
            started_at = self._seoul_time(self.coordinator.clock.now())
            attempt_started = self.coordinator.clock.monotonic()
            try:
                response = self.api.send_one(
                    current.receiving_number,
                    file_ids,
                    content_type=self.content_type,
                )
            except ExplicitApiFailure as failure:
                if failure.http_status == 429:
                    self.coordinator.record_429()
                outcome = self._explicit_failure(
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
                self._log_retry(outcome.row)
                current = outcome.row
                retry_not_before = outcome.retry_not_before
                continue
            except AmbiguousPostOutcome:
                deadline = attempt_started + RUN_SETTINGS.confirmation_timeout_seconds
                self._log_ambiguous(current)
                return self._recover_ambiguous(current, work, started_at, deadline)

            if not self._is_exact_acceptance(response):
                deadline = attempt_started + RUN_SETTINGS.confirmation_timeout_seconds
                self._log_ambiguous(current)
                return self._recover_ambiguous(current, work, started_at, deadline)

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
            outcome = self._resolve_request(current, work, deadline)
            if not outcome.needs_post:
                return outcome
            self._log_retry(outcome.row)
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
        if self._nonempty_exact_string(row.message_id):
            return self._poll_message(row, work, deadline)
        if type(row.message_id) is not str or row.message_id:
            return PipelineResult(row)
        return self._resolve_request(row, work, deadline)

    def _recover_ambiguous(
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
            self._record_lookup_failure(row, "list", failure)
            return PipelineResult(row)

        matches = [
            record
            for record in response.messages
            if self._time_match(record, row)
        ]
        current = row
        if len(matches) == 1:
            with_request = replace(row, request_id=matches[0].request_id)
            current = self._checkpoint(row, with_request)
            with_message = replace(current, message_id=matches[0].message_id)
            current = self._checkpoint(current, with_message)
        self._log_list_response(current, response)
        if len(matches) != 1:
            return PipelineResult(current)
        return self._poll_message(current, work, deadline)

    def _resolve_request(
        self,
        row: ResultRow,
        work: ApprovedWork,
        deadline: float,
    ) -> PipelineResult:
        current = row
        while not self._nonempty_exact_string(current.message_id):
            if not self._before_lookup(deadline):
                return PipelineResult(current)
            try:
                response = self.api.list_by_request(current.request_id)
            except TransientLookupError as failure:
                self._record_lookup_failure(current, "list", failure)
                if not self._sleep_for_poll(deadline):
                    return PipelineResult(current)
                continue

            matches = [
                record
                for record in response.messages
                if self._request_match(record, current)
            ]
            previous = current
            if len(matches) == 1:
                current = replace(current, message_id=matches[0].message_id)
                current = self._checkpoint(previous, current)
            self._log_list_response(current, response)
            if current.message_id:
                return self._poll_message(current, work, deadline)
            if not self._sleep_for_poll(deadline):
                return PipelineResult(current)
        return self._poll_message(current, work, deadline)

    def _poll_message(
        self,
        row: ResultRow,
        work: ApprovedWork,
        deadline: float,
    ) -> PipelineResult:
        current = row
        while self.coordinator.clock.monotonic() < deadline:
            if not self._before_lookup(deadline):
                return PipelineResult(current)
            try:
                response = self.api.get_message(current.message_id)
            except TransientLookupError as failure:
                self._record_lookup_failure(current, "get", failure)
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
                outcome = self._sent(current)
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
        return PipelineResult(self._checkpoint(row, sent))

    def _explicit_failure(
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
    ) -> None:
        if failure.http_status == 429:
            self.coordinator.record_429()
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

    def _log_retry(self, row: ResultRow) -> None:
        self.coordinator.write(
            "RETRY_SCHEDULED",
            delivery_id=row.delivery_id,
            attempt=row.attempts + 1,
        )

    def _log_ambiguous(self, row: ResultRow) -> None:
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

    def _log_list_response(self, row, response) -> None:
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
    def _time_match(cls, record: MessageRecord, row: ResultRow) -> bool:
        return (
            cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type == "MMS"
            and cls._nonempty_exact_string(record.request_id)
            and cls._nonempty_exact_string(record.message_id)
        )

    @classmethod
    def _request_match(cls, record: MessageRecord, row: ResultRow) -> bool:
        return (
            cls._exact_equal(record.request_id, row.request_id)
            and cls._exact_equal(record.to, row.receiving_number)
            and type(record.message_type) is str
            and record.message_type == "MMS"
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
