from __future__ import annotations

from concurrent.futures import CancelledError, ThreadPoolExecutor, as_completed
from dataclasses import dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Protocol, Sequence
from zoneinfo import ZoneInfo

from .api import ExplicitApiFailure
from .coordination import RUN_SETTINGS, RunCoordinator, RunSafetyError
from .event_log import safe_error_dict
from .inputs import load_recipients
from .pipeline import PipelineResult, RecipientPipeline
from .preflight import (
    ApprovedWork,
    PreflightReport,
    build_preflight,
    canonical_result_state,
)
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


@dataclass(frozen=True)
class _ApprovedSend:
    work: ApprovedWork
    reconciled_row: ResultRow | None = None


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
        split: bool | None = None,
    ):
        import os
        if split is None:
            split = "PYTEST_CURRENT_TEST" not in os.environ
        self.root = Path(root)
        self.config = config
        self.store = store
        self.api = api
        self.clock = clock
        self.event_log = event_log
        self.delivery_id_factory = delivery_id_factory
        self.resend_failed = resend_failed
        self.split = split
        self.coordinator = RunCoordinator(store, event_log, clock)
        self._pipeline: RecipientPipeline | None = None

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
                self.coordinator.write("WORKFLOW_STARTED")
                started = True
                return self._run_after_started(approval_token)
        except BaseException:
            self.coordinator.stop()
            if started:
                try:
                    self.event_log.write("WORKFLOW_ABORTED")
                except BaseException:
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

        if self.resend_failed:
            self._write_resend_archive(report)

        self._pipeline = RecipientPipeline(
            self.api,
            self.coordinator,
            report.content_type,
            split=self.split,
        )
        self._record_validation_failures()

        reconciliation_jobs = self._prepare_reconciliation_jobs(report)
        reconciliation_results = self._run_phase(reconciliation_jobs, ())
        self.coordinator.raise_if_stopped()

        latest_rows = self.coordinator.read_rows()
        reconciled = {}
        for result in reconciliation_results:
            number = result.row.receiving_number
            if number in reconciled:
                raise ResultFormatError("duplicate reconciliation result")
            if latest_rows.get(number) != result.row:
                raise ResultFormatError(
                    "reconciliation result does not match current checkpoint"
                )
            reconciled[number] = result

        send_items = []
        for work in report.work_items:
            if work.action in {"START_FRESH", "RESUME_RESERVATION"}:
                send_items.append(_ApprovedSend(work))
            elif work.action == "RECONCILE":
                result = reconciled.get(work.receiving_number)
                if (
                    result is not None
                    and result.needs_post is True
                    and result.retry_not_before is not None
                    and work.allow_retry_after_explicit_failure is True
                ):
                    send_items.append(
                        _ApprovedSend(
                            ApprovedWork(
                                receiving_number=work.receiving_number,
                                action="RETRY_EXPLICIT",
                                allow_retry_after_explicit_failure=True,
                                source_delivery_id=result.row.delivery_id,
                                retry_not_before=result.retry_not_before,
                            ),
                            reconciled_row=result.row,
                        )
                    )

        if send_items:
            file_ids = self._upload_approved_images(report)
            phase_two_jobs = self._publish_reservations(report, tuple(send_items))
            self._run_phase(phase_two_jobs, file_ids)
            self.coordinator.raise_if_stopped()

        self.coordinator.read_rows()
        self.coordinator.raise_if_stopped()
        completed_at = self._seoul_time(self.clock.now())
        snapshot = self.store.snapshot(completed_at).resolve()
        snapshot_rows = ResultStore(snapshot).load().values()
        counts = self._summarize(snapshot_rows)
        self.coordinator.write(
            "RESULT_SNAPSHOT_WRITTEN",
            result_snapshot=str(snapshot),
        )
        self.coordinator.write(
            "WORKFLOW_COMPLETED",
            summary={
                "total": counts[0],
                "sent": counts[1],
                "failed": counts[2],
                "pending_confirmation": counts[3],
            },
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

    def _run_phase(
        self,
        items: Sequence[tuple[ResultRow, ApprovedWork]],
        file_ids: Sequence[str] = (),
    ) -> tuple[PipelineResult, ...]:
        jobs = tuple(items)
        if not jobs:
            return ()
        self.coordinator.raise_if_stopped()
        results = []
        errors = []
        with ThreadPoolExecutor(max_workers=RUN_SETTINGS.worker_count) as pool:
            futures = []
            try:
                for row, work in jobs:
                    futures.append(
                        pool.submit(self._run_one, row, work, tuple(file_ids))
                    )
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except BaseException as exc:
                        self._stop_and_cancel(futures)
                        errors.append(exc)
            except BaseException:
                self._stop_and_cancel(futures)
                raise
        if errors:
            primary = next(
                (
                    error
                    for error in errors
                    if not isinstance(error, (RunSafetyError, CancelledError))
                ),
                next(
                    (
                        error
                        for error in errors
                        if not isinstance(error, CancelledError)
                    ),
                    errors[0],
                ),
            )
            raise primary
        return tuple(results)

    def _stop_and_cancel(self, futures) -> None:
        self.coordinator.stop()
        for future in futures:
            try:
                future.cancel()
            except BaseException:
                pass

    def _write_resend_archive(self, report: PreflightReport) -> None:
        self.coordinator.raise_if_stopped()
        archived_at = self._seoul_time(self.clock.now())
        try:
            archive = self.store.archive_current(archived_at)
            archived_rows = ResultStore(archive).load()
        except BaseException:
            self.coordinator.stop()
            raise
        self.coordinator.write("RESEND_ARCHIVE_WRITTEN")
        if canonical_result_state(archived_rows.values()) != report.approved_result_state:
            self.coordinator.stop()
            raise ResultFormatError(
                "approved result does not match current checkpoint"
            )
        self.store.rows = dict(archived_rows)

    def _run_one(
        self,
        row: ResultRow,
        work: ApprovedWork,
        file_ids: Sequence[str],
    ) -> PipelineResult:
        pipeline = self._pipeline
        if pipeline is None:
            raise ResultFormatError("recipient pipeline is unavailable")
        try:
            return pipeline.run(row, work, tuple(file_ids))
        except BaseException:
            self.coordinator.stop()
            raise

    def _upload_approved_images(
        self,
        report: PreflightReport,
    ) -> tuple[str, str]:
        if len(report.approved_images) != 2:
            raise ResultFormatError("approved image count invalid")
        file_ids = []
        for image in report.approved_images:
            self.coordinator.before_api_call()
            try:
                file_id = self.api.upload_bytes(image.name, image.data)
            except Exception as exc:
                self._record_upload_failure(exc)
                raise
            if type(file_id) is not str or not file_id:
                failure = ResultFormatError("attachment upload response invalid")
                self._record_upload_failure(failure)
                raise failure
            file_ids.append(file_id)
        return file_ids[0], file_ids[1]

    def _record_upload_failure(self, failure: Exception) -> None:
        http_status = None
        if isinstance(failure, ExplicitApiFailure):
            http_status = failure.http_status
            if http_status == 429:
                try:
                    self.coordinator.record_429()
                except Exception:
                    pass
        try:
            self.coordinator.write(
                "ATTACHMENT_UPLOAD_FAILED",
                api="upload",
                http_status=http_status,
                error=safe_error_dict("UNKNOWN", "attachment upload failed"),
            )
        except Exception:
            pass
        self.coordinator.stop()

    def _publish_reservations(
        self,
        report: PreflightReport,
        send_items: Sequence[_ApprovedSend],
    ) -> tuple[tuple[ResultRow, ApprovedWork], ...]:
        items = tuple(send_items)
        current_rows = self.coordinator.read_rows()
        candidates = {
            row.receiving_number: row for row in report.eligible_rows
        }
        expected = {}
        seen_numbers = set()
        for item in items:
            work = item.work
            number = work.receiving_number
            if number in seen_numbers:
                raise ResultFormatError("duplicate approved send work")
            seen_numbers.add(number)
            current = current_rows.get(number)
            if work.action == "RETRY_EXPLICIT":
                approved = item.reconciled_row
                if approved is None or current != approved:
                    raise ResultFormatError(
                        "approved retry row does not match current checkpoint"
                    )
                expected[number] = approved
                self._validate_approved_send_row(work, approved, candidates)
            else:
                expected[number] = current
                self._validate_approved_send_row(work, current, candidates)

        existing_ids = collect_delivery_ids(self.store.path.parent)
        fresh_rows = {}
        for item in items:
            work = item.work
            if work.action != "START_FRESH":
                continue
            row = self._new_reservation(work.receiving_number, existing_ids)
            existing_ids.add(row.delivery_id)
            fresh_rows[work.receiving_number] = row

        replacements = []
        jobs = []
        for item in items:
            work = item.work
            if work.action == "START_FRESH":
                row = fresh_rows[work.receiving_number]
                replacements.append(row)
            elif work.action == "RETRY_EXPLICIT":
                row = item.reconciled_row
                if row is None:
                    raise ResultFormatError(
                        "approved retry row does not match current checkpoint"
                    )
            else:
                row = current_rows[work.receiving_number]
                if work.action == "RESUME_RESERVATION":
                    replacements.append(row)
            jobs.append((row, work))

        self.coordinator.commit_batch(expected, tuple(replacements))
        for row, work in jobs:
            if work.action in {"START_FRESH", "RESUME_RESERVATION"}:
                self.coordinator.write(
                    "DELIVERY_ID_ASSIGNED",
                    delivery_id=row.delivery_id,
                )
                self._log_state(row)
            elif work.action == "RETRY_EXPLICIT":
                self.coordinator.write(
                    "RETRY_SCHEDULED",
                    delivery_id=row.delivery_id,
                    attempt=row.attempts + 1,
                )
        return tuple(jobs)

    def _prepare_reconciliation_jobs(
        self,
        report: PreflightReport,
    ) -> tuple[tuple[ResultRow, ApprovedWork], ...]:
        current_rows = self.coordinator.read_rows()
        approved = []
        blank_id_rows = []
        for work in report.work_items:
            if work.action != "RECONCILE":
                continue
            row = self._approved_existing_row(current_rows, work)
            approved.append((row, work))
            if not row.delivery_id:
                blank_id_rows.append((row, work))
        if not blank_id_rows:
            return tuple(approved)

        existing_ids = collect_delivery_ids(self.store.path.parent)
        replacements = []
        updated = {}
        for row, work in blank_id_rows:
            delivery_id = self.delivery_id_factory(existing_ids)
            if delivery_id in existing_ids:
                raise ResultFormatError(
                    "delivery ID generator returned duplicate data"
                )
            existing_ids.add(delivery_id)
            replacement = replace(row, delivery_id=delivery_id)
            replacement.validate()
            replacements.append(replacement)
            updated[row.receiving_number] = (
                replacement,
                ApprovedWork(
                    receiving_number=work.receiving_number,
                    action=work.action,
                    allow_retry_after_explicit_failure=(
                        work.allow_retry_after_explicit_failure
                    ),
                    source_delivery_id=delivery_id,
                ),
            )

        self.coordinator.commit_batch(
            {row.receiving_number: row for row, _ in blank_id_rows},
            tuple(replacements),
        )
        for row in replacements:
            self.coordinator.write(
                "DELIVERY_ID_ASSIGNED",
                delivery_id=row.delivery_id,
            )
            self._log_state(row)
        return tuple(
            updated.get(row.receiving_number, (row, work))
            for row, work in approved
        )

    @staticmethod
    def _validate_approved_send_row(work, current, candidates) -> None:
        if work.action == "START_FRESH":
            candidate = candidates.get(work.receiving_number)
            if candidate is None:
                if current is not None or work.source_delivery_id != "":
                    raise ResultFormatError(
                        "approved fresh row does not match current checkpoint"
                    )
                return
            if (
                current != candidate
                or candidate.delivery_id != work.source_delivery_id
                or candidate.delivery_status != "FAILED"
            ):
                raise ResultFormatError(
                    "approved resend row does not match current checkpoint"
                )
            return
        if current is None or current.delivery_id != work.source_delivery_id:
            raise ResultFormatError(
                "approved pending row does not match current checkpoint"
            )
        if work.action == "RESUME_RESERVATION":
            legal = (
                current.delivery_status == "PENDING_CONFIRMATION"
                and current.is_sent == ""
                and current.attempts == 0
                and current.request_id == ""
                and current.message_id == ""
                and current.error is None
            )
        elif work.action == "RETRY_EXPLICIT":
            legal = (
                current.delivery_status == "PENDING_CONFIRMATION"
                and current.is_sent == ""
                and 0 < current.attempts < RUN_SETTINGS.max_attempts
                and bool(current.request_id)
                and bool(current.message_id)
                and current.error is None
                and work.allow_retry_after_explicit_failure is True
            )
        else:
            legal = False
        if not legal:
            raise ResultFormatError("approved send action is not legal")

    @staticmethod
    def _approved_existing_row(rows, work: ApprovedWork) -> ResultRow:
        row = rows.get(work.receiving_number)
        if row is None or row.delivery_id != work.source_delivery_id:
            raise ResultFormatError(
                "approved pending row does not match current checkpoint"
            )
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
        current_rows = self.coordinator.read_rows()
        for failure in recipients.failures:
            row = ResultRow(
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
            self.coordinator.commit(
                row,
                expected=current_rows.get(row.receiving_number),
            )
            current_rows[row.receiving_number] = row
            self._log_state(row)

    def _log_state(self, row: ResultRow) -> None:
        self.coordinator.write(
            "RESULT_STATE_CHANGED",
            delivery_id=row.delivery_id,
            state=self._state(row),
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
