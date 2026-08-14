from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from .api import AmbiguousPostOutcome, ExplicitApiFailure, TransientLookupError
from .event_log import (
    list_to_log_dict,
    message_result_to_log_dict,
    safe_error_dict,
    send_to_log_dict,
)
from .inputs import load_recipients
from .preflight import build_preflight
from .results import (
    LiveExecutionLock,
    ResultFormatError,
    ResultRow,
    ResultStore,
    collect_delivery_ids,
)


SEOUL = ZoneInfo("Asia/Seoul")


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def now(self) -> datetime: ...
    def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True)
class RunSummary:
    total: int
    sent: int
    failed: int
    pending: int
    result_snapshot: Path
    event_log: Path


class ApprovalTokenMismatch(ValueError):
    pass


class Workflow:
    def __init__(
        self,
        root: Path,
        config,
        store: ResultStore,
        api,
        clock: Clock,
        event_log,
        delivery_id_factory,
        *,
        resend_failed: bool = False,
    ):
        self.root = Path(root)
        self.config = config
        self.store = store
        self.api = api
        self.clock = clock
        self.event_log = event_log
        self.delivery_id_factory = delivery_id_factory
        self.resend_failed = resend_failed

    def current_token(self) -> str:
        return build_preflight(
            self.root,
            self.config,
            self.store,
            resend_failed=self.resend_failed,
        ).approval_token

    def run_live(self, approval_token: str) -> RunSummary:
        started = False
        try:
            with LiveExecutionLock(self.store.path.parent / ".live-execution.lock"):
                self.event_log.write("WORKFLOW_STARTED")
                started = True
                return self._run_after_started(approval_token)
        except Exception:
            if started:
                try:
                    self.event_log.write("WORKFLOW_ABORTED")
                except Exception:
                    pass
            raise

    def _run_after_started(self, approval_token: str) -> RunSummary:
        report = build_preflight(
            self.root,
            self.config,
            self.store,
            resend_failed=self.resend_failed,
        )
        if approval_token != report.approval_token:
            raise ApprovalTokenMismatch("approval token mismatch")

        resend_reservations = (
            self._prepare_resend(report) if self.resend_failed else {}
        )
        self._record_validation_failures()
        pending_retries = self._reconcile_pending()

        file_ids = []
        if report.eligible_numbers or pending_retries:
            for image in report.approved_images:
                file_ids.append(self.api.upload_bytes(image.name, image.data))

        for recipient in report.eligible_numbers:
            reserved = resend_reservations.get(recipient)
            if reserved is None:
                self._send_new(recipient, file_ids)
            else:
                self._send_new(
                    recipient,
                    file_ids,
                    delivery_id=reserved.delivery_id,
                )
        for row in pending_retries:
            self._schedule_retry(row, row.attempts + 1)
            self._send_new(
                recipient=row.receiving_number,
                file_ids=file_ids,
                attempts_start=row.attempts,
                delivery_id=row.delivery_id,
            )

        completed_at = self._seoul_time(self.clock.now())
        snapshot = self.store.snapshot(completed_at).resolve()
        snapshot_rows = ResultStore(snapshot).load().values()
        counts = self._summarize(snapshot_rows)
        self.event_log.write(
            "RESULT_SNAPSHOT_WRITTEN",
            result_snapshot=str(snapshot),
        )
        summary_fields = {
            "total": counts[0],
            "sent": counts[1],
            "failed": counts[2],
            "pending_confirmation": counts[3],
        }
        self.event_log.write(
            "WORKFLOW_COMPLETED",
            summary=summary_fields,
            completed_at=self._seoul_iso(completed_at),
            result_snapshot=str(snapshot),
        )
        return RunSummary(
            total=counts[0],
            sent=counts[1],
            failed=counts[2],
            pending=counts[3],
            result_snapshot=snapshot,
            event_log=Path(self.event_log.path),
        )

    def _prepare_resend(self, report):
        fresh_rows = ResultStore(self.store.path).load()
        for candidate in report.eligible_rows:
            if fresh_rows.get(candidate.receiving_number) != candidate:
                raise ResultFormatError(
                    "resend snapshot does not match current checkpoint"
                )

        existing_ids = collect_delivery_ids(self.store.path.parent)
        reservations = {}
        for candidate in report.eligible_rows:
            row = self._new_reservation(candidate.receiving_number, existing_ids)
            existing_ids.add(row.delivery_id)
            reservations[candidate.receiving_number] = row

        publish_rows = ResultStore(self.store.path).load()
        for candidate in report.eligible_rows:
            if publish_rows.get(candidate.receiving_number) != candidate:
                raise ResultFormatError(
                    "resend snapshot does not match current checkpoint"
                )
        self.store.rows = publish_rows
        for row in reservations.values():
            self.store.upsert(row)
        self.store.write_atomic()
        for row in reservations.values():
            self.event_log.write(
                "DELIVERY_ID_ASSIGNED",
                delivery_id=row.delivery_id,
            )
            self._log_state(row)
        return reservations

    @staticmethod
    def _summarize(rows):
        rows = tuple(rows)
        return (
            len(rows),
            sum(row.delivery_status == "SENT" for row in rows),
            sum(row.delivery_status == "FAILED" for row in rows),
            sum(row.delivery_status == "PENDING_CONFIRMATION" for row in rows),
        )

    @staticmethod
    def _seoul_time(value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=SEOUL)
        return value.astimezone(SEOUL)

    @classmethod
    def _seoul_iso(cls, value: datetime) -> str:
        return cls._seoul_time(value).isoformat(timespec="milliseconds")

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

    def _persist(self, row: ResultRow) -> None:
        self.store.upsert(row)
        self.store.write_atomic()

    def _log_state(self, row: ResultRow) -> None:
        self.event_log.write(
            "RESULT_STATE_CHANGED",
            delivery_id=row.delivery_id,
            state=self._state(row),
        )

    def _checkpoint(self, row: ResultRow) -> None:
        self._persist(row)
        self._log_state(row)

    def _reserve(self, recipient: str) -> ResultRow:
        row = self._new_reservation(
            recipient,
            collect_delivery_ids(self.store.path.parent),
        )
        self._persist(row)
        self.event_log.write("DELIVERY_ID_ASSIGNED", delivery_id=row.delivery_id)
        self._log_state(row)
        return row

    def _new_reservation(self, recipient: str, existing_ids) -> ResultRow:
        delivery_id = self.delivery_id_factory(existing_ids)
        if delivery_id in existing_ids:
            raise ResultFormatError("delivery ID generator returned duplicate data")
        row = ResultRow(
            receiving_number=recipient,
            delivery_id=delivery_id,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=0,
            request_id="",
            message_id="",
            error=None,
        )
        row.validate()
        return row

    def _record_validation_failures(self) -> None:
        recipients = load_recipients(self.root / "receiving_numbers.csv")
        for failure in recipients.failures:
            self._checkpoint(
                ResultRow(
                    receiving_number=failure.receiving_number,
                    delivery_id="",
                    delivery_status="FAILED",
                    is_sent="false",
                    attempts=0,
                    request_id="",
                    message_id="",
                    error={
                        "status": failure.error_status,
                        "message": failure.error_message,
                    },
                )
            )

    def _send_new(
        self,
        recipient: str,
        file_ids: list[str],
        attempts_start: int = 0,
        delivery_id: str | None = None,
    ) -> None:
        if delivery_id is None:
            row = self._reserve(recipient)
        else:
            row = ResultRow(
                receiving_number=recipient,
                delivery_id=delivery_id,
                delivery_status="PENDING_CONFIRMATION",
                is_sent="",
                attempts=attempts_start,
                request_id="",
                message_id="",
                error=None,
            )

        attempts = attempts_start
        while attempts < 3:
            attempts += 1
            row = replace(
                row,
                delivery_status="PENDING_CONFIRMATION",
                is_sent="",
                attempts=attempts,
                request_id="",
                message_id="",
                error=None,
            )
            self._checkpoint(row)
            self.event_log.write(
                "SEND_ATTEMPT_STARTED",
                delivery_id=row.delivery_id,
                attempt=attempts,
            )
            started_at = self._seoul_time(self.clock.now())
            deadline = self.clock.monotonic() + 600
            try:
                response = self.api.send_one(recipient, file_ids)
            except ExplicitApiFailure as exc:
                if attempts == 3:
                    row = self._failed_row(row, exc)
                    self._checkpoint(row)
                self.event_log.write(
                    "SEND_RESPONSE",
                    delivery_id=row.delivery_id,
                    attempt=attempts,
                    api="send",
                    http_status=exc.http_status,
                    response=exc.response,
                    error=safe_error_dict(exc.status, exc.message),
                )
                if attempts == 3:
                    return
                self._schedule_retry(row, attempts + 1)
                continue
            except AmbiguousPostOutcome:
                self.event_log.write(
                    "SEND_AMBIGUOUS_OUTCOME",
                    delivery_id=row.delivery_id,
                    attempt=attempts,
                    api="send",
                    error=safe_error_dict(
                        "AMBIGUOUS_POST_OUTCOME",
                        "send result could not be confirmed",
                    ),
                )
                self._recover_ambiguous_post(row, started_at, deadline)
                return

            row = replace(row, request_id=response.request_id)
            self._checkpoint(row)
            self.event_log.write(
                "SEND_RESPONSE",
                delivery_id=row.delivery_id,
                attempt=row.attempts,
                api="send",
                http_status=response.http_status,
                request_id=response.request_id,
                response=send_to_log_dict(response),
            )
            outcome, failure, row = self._resolve_request(row, deadline)
            if outcome in ("SENT", "PENDING_CONFIRMATION"):
                return
            if attempts == 3:
                return
            self._schedule_retry(row, attempts + 1)

    def _reconcile_pending(self) -> list[ResultRow]:
        retry_rows = []
        rows = list(self.store.load().values())
        for row in rows:
            if row.delivery_status != "PENDING_CONFIRMATION":
                continue
            if not row.delivery_id:
                delivery_id = self.delivery_id_factory(
                    collect_delivery_ids(self.store.path.parent)
                )
                row = replace(row, delivery_id=delivery_id)
                self._persist(row)
                self.event_log.write(
                    "DELIVERY_ID_ASSIGNED",
                    delivery_id=delivery_id,
                )
                self._log_state(row)
            deadline = self.clock.monotonic() + 600
            if row.message_id:
                outcome, failure, row = self._poll_message(row, deadline)
            elif row.request_id:
                outcome, failure, row = self._resolve_request(row, deadline)
            else:
                continue
            if outcome == "EXPLICIT_FAILURE":
                if row.attempts < 3:
                    retry_rows.append(row)
        return retry_rows

    def _recover_ambiguous_post(self, row, started_at, deadline) -> None:
        start = (started_at - timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        end = (
            self._seoul_time(self.clock.now()) + timedelta(seconds=30)
        ).strftime("%Y-%m-%d %H:%M:%S")
        try:
            response = self.api.list_by_time_and_recipient(
                start, end, row.receiving_number
            )
        except TransientLookupError:
            self._log_lookup_error(row, "list")
            return
        matches = [
            message
            for message in response.messages
            if message.to == row.receiving_number and message.request_id
        ]
        if len(matches) == 1:
            match = matches[0]
            row = replace(
                row,
                request_id=match.request_id,
                message_id=match.message_id,
            )
            self._checkpoint(row)
        self.event_log.write(
            "MESSAGE_LIST_RESPONSE",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            api="list",
            http_status=response.http_status,
            request_id=row.request_id or None,
            message_id=row.message_id or None,
            response=list_to_log_dict(response),
        )
        if len(matches) != 1:
            return
        self._resolve_request(row, deadline)

    def _resolve_request(self, row, deadline):
        while not row.message_id:
            if self.clock.monotonic() >= deadline:
                return "PENDING_CONFIRMATION", None, row
            try:
                response = self.api.list_by_request(row.request_id)
            except TransientLookupError:
                self._log_lookup_error(row, "list")
                return "PENDING_CONFIRMATION", None, row
            matches = [
                message
                for message in response.messages
                if message.to == row.receiving_number
                and message.request_id == row.request_id
            ]
            if len(matches) == 1 and matches[0].message_id:
                row = replace(row, message_id=matches[0].message_id)
                self._checkpoint(row)
            self.event_log.write(
                "MESSAGE_LIST_RESPONSE",
                delivery_id=row.delivery_id,
                attempt=row.attempts,
                api="list",
                http_status=response.http_status,
                request_id=row.request_id,
                message_id=row.message_id or None,
                response=list_to_log_dict(response),
            )
            if row.message_id:
                break
            remaining = deadline - self.clock.monotonic()
            if remaining <= 0:
                return "PENDING_CONFIRMATION", None, row
            self.clock.sleep(min(30, remaining))
        return self._poll_message(row, deadline)

    def _poll_message(self, row, deadline):
        while self.clock.monotonic() < deadline:
            try:
                response = self.api.get_message(row.message_id)
            except TransientLookupError:
                self._log_lookup_error(row, "get")
                return "PENDING_CONFIRMATION", None, row
            message = response.message
            correlated = (
                message.request_id == row.request_id
                and message.message_id == row.message_id
                and message.to == row.receiving_number
            )
            outcome = None
            failure = None
            if correlated and message.status == "COMPLETED" and message.status_name == "success":
                row = replace(
                    row,
                    delivery_status="SENT",
                    is_sent="true",
                    error=None,
                )
                self._checkpoint(row)
                outcome = "SENT"
            elif correlated and message.status == "COMPLETED" and message.status_name == "fail":
                failure = ExplicitApiFailure(
                    message.status_code or "UNKNOWN",
                    message.status_message or "delivery failed",
                )
                if row.attempts >= 3:
                    row = self._failed_row(row, failure)
                    self._checkpoint(row)
                outcome = "EXPLICIT_FAILURE"
            elif not correlated or message.status not in ("READY", "PROCESSING"):
                outcome = "PENDING_CONFIRMATION"

            self.event_log.write(
                "DELIVERY_POLL_RESPONSE",
                delivery_id=row.delivery_id,
                attempt=row.attempts,
                api="get",
                http_status=response.http_status,
                request_id=row.request_id,
                message_id=row.message_id,
                response=message_result_to_log_dict(response),
            )
            if outcome is not None:
                return outcome, failure, row

            remaining = deadline - self.clock.monotonic()
            if remaining <= 0:
                break
            self.clock.sleep(min(30, remaining))
        return "PENDING_CONFIRMATION", None, row

    @staticmethod
    def _failed_row(row: ResultRow, failure: ExplicitApiFailure) -> ResultRow:
        return replace(
            row,
            delivery_status="FAILED",
            is_sent="false",
            error=safe_error_dict(failure.status, failure.message),
        )

    def _schedule_retry(self, row: ResultRow, upcoming_attempt: int) -> None:
        self.event_log.write(
            "RETRY_SCHEDULED",
            delivery_id=row.delivery_id,
            attempt=upcoming_attempt,
        )
        self.clock.sleep(30)

    def _log_lookup_error(
        self,
        row: ResultRow,
        api: str,
    ) -> None:
        self.event_log.write(
            "API_LOOKUP_ERROR",
            delivery_id=row.delivery_id,
            attempt=row.attempts,
            api=api,
            request_id=row.request_id or None,
            message_id=row.message_id or None,
            error=safe_error_dict(
                "TRANSIENT_LOOKUP_ERROR",
                "lookup temporarily unavailable",
            ),
        )
