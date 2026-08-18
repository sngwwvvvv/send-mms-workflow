from __future__ import annotations

from dataclasses import replace
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
import threading
import unittest

from sens_mms.api import (
    AmbiguousPostOutcome,
    ExplicitApiFailure,
    MessageListResponse,
    MessageRecord,
    MessageResultResponse,
    SendResponse,
    TransientLookupError,
)
from sens_mms.coordination import RunCoordinator, RunSafetyError
from sens_mms.pipeline import PipelineResult, RecipientPipeline
from sens_mms.preflight import ApprovedWork
from sens_mms.results import ResultFormatError, ResultRow, ResultStore
from sens_mms.inputs import MESSAGE_BODY, split_message_body


NUMBER = "01000000001"
OTHER = "01000000002"
DELIVERY_ID = "23456789ABCDEFGH"


class ManualClock:
    def __init__(self):
        self.value = 0.0
        self.sleeps = []
        self.wall = datetime(2026, 8, 18, 10, 0, 0)

    def monotonic(self):
        return self.value

    def now(self):
        return self.wall

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.value += seconds


class BlockingRetryClock(ManualClock):
    def __init__(self):
        super().__init__()
        self.retry_sleep_started = threading.Event()
        self.release_retry = threading.Event()
        self._lock = threading.Lock()

    def monotonic(self):
        with self._lock:
            return self.value

    def sleep(self, seconds):
        if (
            threading.current_thread().name == "retry-worker"
            and seconds == 10
            and not self.retry_sleep_started.is_set()
        ):
            self.retry_sleep_started.set()
            if not self.release_retry.wait(2):
                raise AssertionError("retry worker was not released")
        with self._lock:
            self.sleeps.append((threading.current_thread().name, seconds))
            self.value += seconds


class RecordingEventLog:
    path = Path("fake-delivery.jsonl")

    def __init__(self, *, fail_event=None, fail_occurrence=1):
        self.events = []
        self.fail_event = fail_event
        self.fail_occurrence = fail_occurrence
        self._seen = 0

    def write(self, event, **fields):
        if event == self.fail_event:
            self._seen += 1
            if self._seen == self.fail_occurrence:
                raise OSError("event unavailable")
        self.events.append((event, fields))


class AdvancingEventLog(RecordingEventLog):
    def __init__(self):
        super().__init__()
        self.clock = None

    def write(self, event, **fields):
        super().write(event, **fields)
        if event == "SEND_RESPONSE":
            self.clock.value += 5


class GateInjectingEventLog(RecordingEventLog):
    def __init__(self):
        super().__init__()
        self.coordinator = None
        self.injected = False

    def write(self, event, **fields):
        super().write(event, **fields)
        if event == "SEND_ATTEMPT_STARTED" and not self.injected:
            self.injected = True
            self.coordinator.record_429()


class FailingStore(ResultStore):
    def __init__(self, path, fail_replace):
        super().__init__(path)
        self.fail_replace = fail_replace
        self.replace_count = 0

    def replace_rows_atomic(self, expected, replacements):
        self.replace_count += 1
        if self.replace_count == self.fail_replace:
            raise OSError("checkpoint unavailable")
        return super().replace_rows_atomic(expected, replacements)


class NotifyingCoordinator(RunCoordinator):
    def __init__(self, store, event_log, clock):
        super().__init__(store, event_log, clock)
        self.unrelated_gate_entered = threading.Event()

    def before_api_call(self):
        if threading.current_thread().name == "unrelated-worker":
            self.unrelated_gate_entered.set()
        return super().before_api_call()


class SharedFailureApi:
    def __init__(self, clock):
        self.clock = clock
        self.calls = []
        self._lock = threading.Lock()
        self.unrelated_called = threading.Event()

    def send_one(self, to, file_ids, *, content_type):
        with self._lock:
            self.calls.append((to, self.clock.monotonic()))
        if threading.current_thread().name == "unrelated-worker":
            self.unrelated_called.set()
        raise ExplicitApiFailure("400", "fixed fake failure", http_status=400)

    def send_mms(self, to, file_ids, *, content_type, content=" "):
        return self.send_one(to, file_ids, content_type=content_type)

    def send_lms(self, to, *, content_type, content=MESSAGE_BODY):
        with self._lock:
            self.calls.append((to, self.clock.monotonic()))
        if threading.current_thread().name == "unrelated-worker":
            self.unrelated_called.set()
        raise ExplicitApiFailure("400", "fixed fake failure", http_status=400)


class ScriptedPipelineApi:
    def __init__(self, *, sends=(), lists=(), gets=(), time_lists=()):
        self.sends = list(sends)
        self.lists = list(lists)
        self.gets = list(gets)
        self.time_lists = list(time_lists)
        self.calls = []

    @staticmethod
    def _next(script):
        item = script.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    def send_one(self, to, file_ids, *, content_type):
        self.calls.append(("send", to, tuple(file_ids), content_type))
        return self._next(self.sends)

    def send_mms(self, to, file_ids, *, content_type, content=" "):
        self.calls.append(("send_mms", to, tuple(file_ids), content_type, content))
        return self._next(self.sends)

    def send_lms(self, to, *, content_type, content=MESSAGE_BODY):
        self.calls.append(("send_lms", to, content_type, content))
        return self._next(self.sends)

    def list_by_request(self, request_id):
        self.calls.append(("list", request_id))
        return self._next(self.lists)

    def list_by_time_and_recipient(
        self, request_start_time, request_end_time, to, *, message_type="MMS"
    ):
        self.calls.append(
            ("time_list", request_start_time, request_end_time, to, message_type)
        )
        return self._next(self.time_lists)

    def get_message(self, message_id):
        self.calls.append(("get", message_id))
        return self._next(self.gets)


class TimestampingPipelineApi(ScriptedPipelineApi):
    def __init__(self, *, clock=None, **kwargs):
        super().__init__(**kwargs)
        self.clock = clock
        self.send_times = []

    def send_one(self, to, file_ids, *, content_type):
        self.send_times.append(self.clock.monotonic())
        return super().send_one(to, file_ids, content_type=content_type)

    def send_mms(self, to, file_ids, *, content_type, content=" "):
        self.send_times.append(self.clock.monotonic())
        return super().send_mms(to, file_ids, content_type=content_type, content=content)

    def send_lms(self, to, *, content_type, content=MESSAGE_BODY):
        self.send_times.append(self.clock.monotonic())
        return super().send_lms(to, content_type=content_type, content=content)


def send_response(request_id="request-1"):
    return SendResponse(202, request_id, "2026-08-18T10:00:00.000", "202", "success")


def message(
    status,
    status_name="",
    *,
    request_id="request-1",
    message_id="message-1",
    to=NUMBER,
    message_type="MMS",
    status_code="0",
    status_message="",
):
    return MessageRecord(
        request_id=request_id,
        message_id=message_id,
        to=to,
        request_time="2026-08-18 10:00:00",
        complete_time="2026-08-18 10:00:01" if status == "COMPLETED" else "",
        telco_code="SKT",
        status=status,
        status_code=status_code,
        status_name=status_name,
        status_message=status_message,
        message_type=message_type,
    )


def list_response(*messages):
    return MessageListResponse(200, "202", "success", tuple(messages), 100, 0, len(messages), False)


def result_response(record):
    return MessageResultResponse(200, "200", "success", record)


def reservation(*, attempts=0, request_id="", message_id=""):
    return ResultRow(
        NUMBER,
        DELIVERY_ID,
        "PENDING_CONFIRMATION",
        "",
        attempts,
        request_id,
        message_id,
        None,
    )


def work(action="RESUME_RESERVATION", allow_retry=True):
    return ApprovedWork(NUMBER, action, allow_retry, DELIVERY_ID)


def state_boundaries(events):
    return [
        (
            fields["state"]["delivery_status"],
            fields["state"]["attempts"],
            fields["state"]["request_id"],
            fields["state"]["message_id"],
        )
        for event, fields in events
        if event == "RESULT_STATE_CHANGED"
    ]


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / "results" / "result.csv"

    def make_pipeline(self, api, row, *, event_log=None, store=None, split=False):
        base = ResultStore(self.path)
        base.upsert(row)
        base.write_atomic()
        store = store or ResultStore(self.path)
        clock = ManualClock()
        log = event_log or RecordingEventLog()
        coordinator = RunCoordinator(store, log, clock)
        return RecipientPipeline(api, coordinator, content_type="COMM", split=split), clock, log, coordinator

    def persisted(self):
        return ResultStore(self.path).load()[NUMBER]

    def test_accepted_processing_success_checkpoints_every_boundary(self):
        """Catches a send/list/get transition that skips its durable crash boundary."""
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(list_response(message("PROCESSING")),),
            gets=(
                result_response(message("PROCESSING")),
                result_response(message("COMPLETED", "success")),
            ),
        )
        pipeline, clock, log, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual([call[0] for call in api.calls], ["send", "list", "get", "get"])
        self.assertEqual(api.calls[0][2:], (("file-1", "file-2"), "COMM"))
        self.assertEqual(clock.sleeps, [1])
        self.assertEqual(
            state_boundaries(log.events),
            [
                ("PENDING_CONFIRMATION", 1, "", ""),
                ("PENDING_CONFIRMATION", 1, "request-1", ""),
                ("PENDING_CONFIRMATION", 1, "request-1", "message-1"),
                ("SENT", 1, "request-1", "message-1"),
            ],
        )
        self.assertEqual(self.persisted(), result.row)

    def test_attempt_state_event_failure_leaves_ambiguous_row_and_starts_no_post(self):
        """Catches POST beginning after the attempt checkpoint cannot be logged."""
        row = reservation()
        log = RecordingEventLog(fail_event="RESULT_STATE_CHANGED")
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, coordinator = self.make_pipeline(api, row, event_log=log)

        with self.assertRaisesRegex(OSError, "event unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))
        self.assertEqual(api.calls, [])
        self.assertEqual((self.persisted().attempts, self.persisted().request_id), (1, ""))
        with self.assertRaises(RunSafetyError):
            coordinator.raise_if_stopped()

    def test_429_published_during_attempt_durability_is_rechecked_before_post(self):
        """Catches POST bypassing a 429 observed after its first gate check."""
        log = GateInjectingEventLog()
        api = TimestampingPipelineApi(
            sends=(send_response(),),
            lists=(list_response(message("READY")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, clock, _, coordinator = self.make_pipeline(
            api,
            reservation(),
            event_log=log,
        )
        log.coordinator = coordinator
        api.clock = clock

        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(api.send_times, [10])
        self.assertEqual(clock.sleeps, [10])

    def test_send_attempt_started_event_failure_preserves_attempt_and_starts_no_post(self):
        """Catches a failed SEND_ATTEMPT_STARTED write being swallowed before POST."""
        row = reservation()
        log = RecordingEventLog(fail_event="SEND_ATTEMPT_STARTED")
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(list_response(message("READY")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, _, _, coordinator = self.make_pipeline(api, row, event_log=log)

        with self.assertRaisesRegex(OSError, "event unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))

        persisted = self.persisted()
        self.assertEqual(api.calls, [])
        self.assertEqual(
            (
                persisted.delivery_status,
                persisted.attempts,
                persisted.request_id,
                persisted.message_id,
            ),
            ("PENDING_CONFIRMATION", 1, "", ""),
        )
        with self.assertRaises(RunSafetyError):
            coordinator.raise_if_stopped()

    def test_send_response_event_failure_preserves_request_and_starts_no_lookup(self):
        """Catches a failed SEND_RESPONSE write allowing list/get after acceptance."""
        row = reservation()
        log = RecordingEventLog(fail_event="SEND_RESPONSE")
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(list_response(message("READY")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, _, _, coordinator = self.make_pipeline(api, row, event_log=log)

        with self.assertRaisesRegex(OSError, "event unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))

        persisted = self.persisted()
        self.assertEqual([call[0] for call in api.calls], ["send"])
        self.assertEqual(
            (
                persisted.delivery_status,
                persisted.attempts,
                persisted.request_id,
                persisted.message_id,
            ),
            ("PENDING_CONFIRMATION", 1, "request-1", ""),
        )
        with self.assertRaises(RunSafetyError):
            coordinator.raise_if_stopped()

    def test_stop_after_reservation_starts_no_post_and_preserves_attempt_zero(self):
        """Catches a worker continuing after reservation publication globally stopped."""
        row = reservation()
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, coordinator = self.make_pipeline(api, row)
        coordinator.stop()

        with self.assertRaises(RunSafetyError):
            pipeline.run(row, work(), ("file-1", "file-2"))
        self.assertEqual(api.calls, [])
        self.assertEqual(self.persisted(), row)

    def test_attempt_checkpoint_failure_preserves_reservation_and_starts_no_post(self):
        """Catches POST beginning when the attempt increment was not durable."""
        row = reservation()
        failing = FailingStore(self.path, fail_replace=1)
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, row, store=failing)

        with self.assertRaisesRegex(OSError, "checkpoint unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))
        self.assertEqual(api.calls, [])
        self.assertEqual(self.persisted(), row)

    def test_request_checkpoint_failure_stops_before_list_and_keeps_blank_id_ambiguous(self):
        """Catches lookup or resend proceeding after accepted request ID persistence fails."""
        row = reservation()
        failing = FailingStore(self.path, fail_replace=2)
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, row, store=failing)

        with self.assertRaisesRegex(OSError, "checkpoint unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))
        self.assertEqual([call[0] for call in api.calls], ["send"])
        self.assertEqual((self.persisted().attempts, self.persisted().request_id), (1, ""))

    def test_message_checkpoint_failure_stops_before_get(self):
        """Catches GET beginning before the correlated message ID is durable."""
        row = reservation()
        failing = FailingStore(self.path, fail_replace=3)
        api = ScriptedPipelineApi(
            sends=(send_response(),), lists=(list_response(message("READY")),)
        )
        pipeline, _, _, _ = self.make_pipeline(api, row, store=failing)

        with self.assertRaisesRegex(OSError, "checkpoint unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))
        self.assertEqual([call[0] for call in api.calls], ["send", "list"])
        self.assertEqual((self.persisted().request_id, self.persisted().message_id), ("request-1", ""))

    def test_final_checkpoint_failure_does_not_log_a_false_terminal_response(self):
        """Catches a terminal poll event being written before the terminal row is durable."""
        row = reservation()
        failing = FailingStore(self.path, fail_replace=4)
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(list_response(message("READY")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, _, log, _ = self.make_pipeline(api, row, store=failing)

        with self.assertRaisesRegex(OSError, "checkpoint unavailable"):
            pipeline.run(row, work(), ("file-1", "file-2"))
        self.assertEqual(self.persisted().delivery_status, "PENDING_CONFIRMATION")
        self.assertNotIn("DELIVERY_POLL_RESPONSE", [event for event, _ in log.events])

    def test_state_event_failures_stop_after_each_durable_correlation_boundary(self):
        """Catches an event failure allowing the next API call after a durable boundary."""
        cases = (
            (2, ["send"], ("request-1", "", "PENDING_CONFIRMATION")),
            (3, ["send", "list"], ("request-1", "message-1", "PENDING_CONFIRMATION")),
            (4, ["send", "list", "get"], ("request-1", "message-1", "SENT")),
        )
        for occurrence, expected_calls, expected_state in cases:
            with self.subTest(occurrence=occurrence):
                row = reservation()
                log = RecordingEventLog(
                    fail_event="RESULT_STATE_CHANGED",
                    fail_occurrence=occurrence,
                )
                api = ScriptedPipelineApi(
                    sends=(send_response(),),
                    lists=(list_response(message("READY")),),
                    gets=(result_response(message("COMPLETED", "success")),),
                )
                pipeline, _, _, coordinator = self.make_pipeline(
                    api,
                    row,
                    event_log=log,
                )
                with self.assertRaisesRegex(OSError, "event unavailable"):
                    pipeline.run(row, work(), ("file-1", "file-2"))
                self.assertEqual([call[0] for call in api.calls], expected_calls)
                persisted = self.persisted()
                self.assertEqual(
                    (persisted.request_id, persisted.message_id, persisted.delivery_status),
                    expected_state,
                )
                with self.assertRaises(RunSafetyError):
                    coordinator.raise_if_stopped()

    def test_first_explicit_post_failure_retries_after_exactly_ten_seconds(self):
        """Catches explicit non-acceptance being treated as ambiguous or retried too early."""
        api = ScriptedPipelineApi(
            sends=(
                ExplicitApiFailure("400", "unsafe upstream text", http_status=400),
                send_response(),
            ),
            lists=(list_response(message("COMPLETED", "success")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, clock, _, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual([call[0] for call in api.calls], ["send", "send", "list", "get"])
        self.assertEqual(clock.sleeps, [10])
        self.assertEqual(result.row.attempts, 2)

    def test_three_explicit_post_failures_checkpoint_failed_with_only_sanitized_error(self):
        """Catches a fourth POST or durable retention of API-controlled error prose."""
        api = ScriptedPipelineApi(
            sends=tuple(
                ExplicitApiFailure(str(code), f"private-{code}", http_status=code)
                for code in (400, 401, 402)
            )
        )
        pipeline, clock, log, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual([call[0] for call in api.calls], ["send", "send", "send"])
        self.assertEqual(clock.sleeps, [10, 10])
        self.assertEqual(result.row.delivery_status, "FAILED")
        self.assertEqual(result.row.error, {"status": "402", "message": "redacted"})
        final_state = max(
            index for index, item in enumerate(log.events)
            if item[0] == "RESULT_STATE_CHANGED" and item[1]["state"]["delivery_status"] == "FAILED"
        )
        final_response = max(index for index, item in enumerate(log.events) if item[0] == "SEND_RESPONSE")
        self.assertLess(final_state, final_response)

    def test_reconcile_completed_failure_requests_post_only_when_approved(self):
        """Catches pending reconciliation silently POSTing or bypassing approval."""
        row = reservation(attempts=1, request_id="request-1", message_id="message-1")
        for allowed, expected in ((True, True), (False, False)):
            with self.subTest(allowed=allowed):
                api = ScriptedPipelineApi(
                    gets=(result_response(message("COMPLETED", "fail", status_code="47", status_message="secret")),)
                )
                pipeline, clock, _, _ = self.make_pipeline(api, row)
                result = pipeline.run(row, work("RECONCILE", allowed))
                self.assertEqual(result.needs_post, expected)
                self.assertEqual(result.retry_not_before, 10.0 if allowed else None)
                self.assertEqual([call[0] for call in api.calls], ["get"])
                self.assertEqual(clock.sleeps, [])

    def test_reconcile_third_completed_failure_becomes_failed_without_post(self):
        """Catches restart reconciliation exceeding the three-POST ceiling."""
        row = reservation(attempts=3, request_id="request-1", message_id="message-1")
        api = ScriptedPipelineApi(
            gets=(result_response(message("COMPLETED", "fail", status_code="99", status_message="secret")),)
        )
        pipeline, _, _, _ = self.make_pipeline(api, row)
        result = pipeline.run(row, work("RECONCILE", True))

        self.assertEqual([call[0] for call in api.calls], ["get"])
        self.assertEqual(result.row.delivery_status, "FAILED")
        self.assertFalse(result.needs_post)
        self.assertEqual(result.row.error, {"status": "99", "message": "redacted"})

    def test_ambiguous_post_is_never_reposted_and_uses_at_most_one_typed_time_lookup(self):
        """Catches an uncertain POST entering the explicit-failure retry path."""
        api = ScriptedPipelineApi(
            sends=(AmbiguousPostOutcome("response lost"),),
            time_lists=(list_response(),),
        )
        pipeline, clock, log, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "PENDING_CONFIRMATION")
        self.assertEqual([call[0] for call in api.calls], ["send", "time_list"])
        self.assertEqual(api.calls[1][-2:], (NUMBER, "MMS"))
        self.assertEqual(clock.sleeps, [])
        serialized = repr(log.events)
        self.assertNotIn("response lost", serialized)

    def test_malformed_typed_acceptance_is_ambiguous_not_explicit_retry(self):
        """Catches malformed 2xx acceptance being retried as a definite failure."""
        malformed = replace(send_response(), request_id="")
        api = ScriptedPipelineApi(sends=(malformed,), time_lists=(list_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual([call[0] for call in api.calls], ["send", "time_list"])
        self.assertEqual(result.row.delivery_status, "PENDING_CONFIRMATION")
        self.assertEqual(result.row.request_id, "")

    def test_whitespace_accepted_request_id_is_ambiguous_and_never_retried(self):
        """Catches whitespace-only request IDs being durably treated as acceptance."""
        malformed = replace(send_response(), request_id="   ")
        api = ScriptedPipelineApi(sends=(malformed,), time_lists=(list_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual([call[0] for call in api.calls], ["send", "time_list"])
        self.assertEqual(result.row.request_id, "")
        self.assertEqual(result.row.attempts, 1)

    def test_positive_attempt_blank_ids_hold_without_any_api_call(self):
        """Catches a crash-boundary ambiguous row being automatically reposted."""
        row = reservation(attempts=1)
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, row)
        result = pipeline.run(row, work("HOLD_AMBIGUOUS", False), ("file-1", "file-2"))
        self.assertEqual(result, PipelineResult(row))
        self.assertEqual(api.calls, [])

    def test_retry_action_cannot_repost_positive_attempt_blank_ids(self):
        """Catches forged retry approval bypassing the blank-correlation crash boundary."""
        row = reservation(attempts=1)
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, row)
        result = pipeline.run(
            row,
            work("RETRY_EXPLICIT", True),
            ("file-1", "file-2"),
        )
        self.assertEqual(result, PipelineResult(row))
        self.assertEqual(api.calls, [])

    def test_retry_action_waits_ten_seconds_before_the_next_post(self):
        """Catches a reconciled explicit failure starting its retry immediately."""
        row = reservation(
            attempts=1,
            request_id="request-1",
            message_id="message-1",
        )
        api = ScriptedPipelineApi(
            sends=(send_response("request-2"),),
            lists=(
                list_response(
                    message(
                        "PROCESSING",
                        request_id="request-2",
                        message_id="message-2",
                    )
                ),
            ),
            gets=(
                result_response(
                    message(
                        "COMPLETED",
                        "success",
                        request_id="request-2",
                        message_id="message-2",
                    )
                ),
            ),
        )
        pipeline, clock, _, _ = self.make_pipeline(api, row)
        result = pipeline.run(
            row,
            work("RETRY_EXPLICIT", True),
            ("file-1", "file-2"),
        )

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(clock.sleeps, [10])
        self.assertEqual([call[0] for call in api.calls], ["send", "list", "get"])

    def test_retry_action_waits_only_to_carried_absolute_deadline(self):
        """Catches phase-two retry reconstructing a fresh ten-second delay."""
        row = reservation(
            attempts=1,
            request_id="request-1",
            message_id="message-1",
        )
        api = ScriptedPipelineApi(
            sends=(send_response("request-2"),),
            lists=(
                list_response(
                    message(
                        "PROCESSING",
                        request_id="request-2",
                        message_id="message-2",
                    )
                ),
            ),
            gets=(
                result_response(
                    message(
                        "COMPLETED",
                        "success",
                        request_id="request-2",
                        message_id="message-2",
                    )
                ),
            ),
        )
        pipeline, clock, _, _ = self.make_pipeline(api, row)
        clock.value = 7
        approved = ApprovedWork(
            NUMBER,
            "RETRY_EXPLICIT",
            True,
            DELIVERY_ID,
            retry_not_before=10.0,
        )

        result = pipeline.run(row, approved, ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(clock.sleeps, [3])
        self.assertEqual(clock.monotonic(), 10)

    def test_invalid_action_state_combinations_never_post_or_change_the_row(self):
        """Catches terminal or ineligible rows being rewritten and posted."""
        sent = replace(
            reservation(attempts=1, request_id="request-1", message_id="message-1"),
            delivery_status="SENT",
            is_sent="true",
        )
        failed = replace(
            reservation(attempts=1, request_id="request-1", message_id="message-1"),
            delivery_status="FAILED",
            is_sent="false",
            error={"status": "400", "message": "redacted"},
        )
        correlated = reservation(
            attempts=1,
            request_id="request-1",
            message_id="message-1",
        )
        cases = (
            ("sent", sent, work("START_FRESH", True)),
            ("failed", failed, work("RESUME_RESERVATION", True)),
            ("attempt-zero-retry", reservation(), work("RETRY_EXPLICIT", True)),
            ("positive-start", correlated, work("START_FRESH", True)),
            ("positive-resume", correlated, work("RESUME_RESERVATION", True)),
        )
        for label, row, approved in cases:
            with self.subTest(case=label):
                api = ScriptedPipelineApi(
                    sends=(
                        ExplicitApiFailure("400", "must not run", http_status=400),
                    )
                )
                pipeline, _, _, _ = self.make_pipeline(api, row)
                result = pipeline.run(row, approved, ("file-1", "file-2"))
                self.assertEqual(result, PipelineResult(row))
                self.assertEqual(api.calls, [])
                self.assertEqual(self.persisted(), row)

    def test_recipient_retry_wait_does_not_gate_an_unrelated_worker(self):
        """Catches a local retry deadline being published as a run-global gate."""
        retry_row = reservation(
            attempts=2,
            request_id="request-1",
            message_id="message-1",
        )
        other_id = "23456789ABCDEFGJ"
        unrelated_row = replace(
            reservation(),
            receiving_number=OTHER,
            delivery_id=other_id,
        )
        base = ResultStore(self.path)
        base.rows = {
            retry_row.receiving_number: retry_row,
            unrelated_row.receiving_number: unrelated_row,
        }
        base.write_atomic()
        clock = BlockingRetryClock()
        coordinator = NotifyingCoordinator(
            ResultStore(self.path),
            RecordingEventLog(),
            clock,
        )
        api = SharedFailureApi(clock)
        pipeline = RecipientPipeline(api, coordinator, content_type="COMM")
        errors = []

        def run_retry():
            try:
                pipeline.run(
                    retry_row,
                    work("RETRY_EXPLICIT", True),
                    ("file-1", "file-2"),
                )
            except BaseException as exc:
                errors.append(exc)

        def run_unrelated():
            try:
                pipeline.run(
                    unrelated_row,
                    ApprovedWork(OTHER, "RESUME_RESERVATION", False, other_id),
                    ("file-1", "file-2"),
                )
            except BaseException as exc:
                errors.append(exc)

        retry_thread = threading.Thread(target=run_retry, name="retry-worker")
        unrelated_thread = threading.Thread(
            target=run_unrelated,
            name="unrelated-worker",
        )
        retry_thread.start()
        self.assertTrue(clock.retry_sleep_started.wait(1))
        unrelated_thread.start()
        self.assertTrue(coordinator.unrelated_gate_entered.wait(1))
        try:
            self.assertTrue(
                api.unrelated_called.wait(1),
                "unrelated POST was blocked by another recipient's retry delay",
            )
        finally:
            clock.release_retry.set()
            retry_thread.join(2)
            unrelated_thread.join(2)
        self.assertFalse(retry_thread.is_alive())
        self.assertFalse(unrelated_thread.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual(
            [called_at for number, called_at in api.calls if number == OTHER],
            [0.0],
        )

    def test_retry_and_longer_rate_gate_end_at_later_deadline_not_sum(self):
        """Catches a ten-second retry being added after a twenty-second rate gate."""
        row = reservation(
            attempts=2,
            request_id="request-1",
            message_id="message-1",
        )
        api = ScriptedPipelineApi(
            sends=(ExplicitApiFailure("400", "fixed", http_status=400),),
        )
        pipeline, clock, _, coordinator = self.make_pipeline(api, row)
        coordinator.record_429()
        coordinator.record_429()
        result = pipeline.run(
            row,
            work("RETRY_EXPLICIT", True),
            ("file-1", "file-2"),
        )

        self.assertEqual(result.row.attempts, 3)
        self.assertEqual(clock.monotonic(), 20)
        self.assertEqual([call[0] for call in api.calls], ["send"])

    def test_ambiguous_match_checkpoints_request_before_message_id(self):
        """Catches ambiguous recovery losing the request ID if message persistence fails."""
        api = ScriptedPipelineApi(
            sends=(AmbiguousPostOutcome("lost"),),
            time_lists=(list_response(message("PROCESSING")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, _, log, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(
            state_boundaries(log.events),
            [
                ("PENDING_CONFIRMATION", 1, "", ""),
                ("PENDING_CONFIRMATION", 1, "request-1", ""),
                ("PENDING_CONFIRMATION", 1, "request-1", "message-1"),
                ("SENT", 1, "request-1", "message-1"),
            ],
        )

    def test_ambiguous_recovery_adopts_only_one_exact_recipient_mms_match(self):
        """Catches loose time correlation adopting another message or duplicate match."""
        valid = message("PROCESSING")
        for records, adopted in (
            ((valid,), True),
            ((valid, replace(valid)), False),
            ((replace(valid, to=OTHER),), False),
            ((replace(valid, message_type="SMS"),), False),
            ((replace(valid, request_id=""),), False),
            ((replace(valid, message_id=""),), False),
        ):
            with self.subTest(records=records):
                api = ScriptedPipelineApi(
                    sends=(AmbiguousPostOutcome("lost"),),
                    time_lists=(list_response(*records),),
                    gets=(result_response(message("COMPLETED", "success")),) if adopted else (),
                )
                pipeline, _, _, _ = self.make_pipeline(api, reservation())
                result = pipeline.run(reservation(), work(), ("file-1", "file-2"))
                self.assertEqual(bool(result.row.message_id), adopted)
                self.assertEqual([call[0] for call in api.calls].count("time_list"), 1)

    def test_request_list_requires_exact_request_recipient_type_and_unique_message_id(self):
        """Catches list correlation accepting a wrong, duplicate, or blank identity."""
        valid = message("READY")
        first = list_response(
            replace(valid, request_id="request-other"),
            replace(valid, to=OTHER),
            replace(valid, message_type="SMS"),
            replace(valid, message_id=""),
            valid,
            replace(valid),
        )
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(first, list_response(valid)),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, clock, _, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual([call[0] for call in api.calls], ["send", "list", "list", "get"])
        self.assertEqual(clock.sleeps, [1])

    def test_get_requires_exact_stored_request_message_and_recipient(self):
        """Catches a terminal result for another correlation being adopted."""
        row = reservation(attempts=1, request_id="request-1", message_id="message-1")
        variants = (
            replace(message("COMPLETED", "success"), request_id="request-other"),
            replace(message("COMPLETED", "success"), message_id="message-other"),
            replace(message("COMPLETED", "success"), to=OTHER),
        )
        for record in variants:
            with self.subTest(record=record):
                api = ScriptedPipelineApi(gets=(result_response(record),))
                pipeline, _, _, _ = self.make_pipeline(api, row)
                result = pipeline.run(row, work("RECONCILE", True))
                self.assertEqual(result.row.delivery_status, "PENDING_CONFIRMATION")
                self.assertFalse(result.needs_post)

    def test_transient_list_and_get_errors_retry_each_second_in_same_window(self):
        """Catches transient lookup errors ending confirmation or incrementing attempts."""
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(
                TransientLookupError("private-list"),
                list_response(message("PROCESSING")),
            ),
            gets=(
                TransientLookupError("private-get"),
                result_response(message("COMPLETED", "success")),
            ),
        )
        pipeline, clock, log, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(result.row.attempts, 1)
        self.assertEqual(clock.sleeps, [1, 1])
        errors = [fields for event, fields in log.events if event == "API_LOOKUP_ERROR"]
        self.assertEqual(len(errors), 2)
        self.assertNotIn("private-list", repr(log.events))
        self.assertNotIn("private-get", repr(log.events))

    def test_get_429_records_global_gate_and_retries_without_raw_error(self):
        """Catches GET 429 bypassing the shared gate or leaking exception text."""
        row = reservation(attempts=1, request_id="request-1", message_id="message-1")
        api = ScriptedPipelineApi(
            gets=(
                TransientLookupError("private-429", http_status=429),
                result_response(message("COMPLETED", "success")),
            )
        )
        pipeline, clock, log, _ = self.make_pipeline(api, row)
        result = pipeline.run(row, work("RECONCILE", True))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(clock.sleeps, [1, 9])
        self.assertNotIn("private-429", repr(log.events))

    def test_every_repeated_ready_and_processing_get_is_logged(self):
        """Catches polling telemetry being collapsed or omitted."""
        row = reservation(attempts=1, request_id="request-1", message_id="message-1")
        api = ScriptedPipelineApi(
            gets=(
                result_response(message("READY")),
                result_response(message("READY")),
                result_response(message("PROCESSING")),
                result_response(message("COMPLETED", "success")),
            )
        )
        pipeline, clock, log, _ = self.make_pipeline(api, row)
        result = pipeline.run(row, work("RECONCILE", True))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(clock.sleeps, [1, 1, 1])
        self.assertEqual(
            sum(event == "DELIVERY_POLL_RESPONSE" for event, _ in log.events), 4
        )

    def test_rate_gate_that_crosses_confirmation_deadline_starts_no_late_get(self):
        """Catches an API call beginning after a global 429 wait consumed the window."""
        row = reservation(attempts=1, request_id="request-1", message_id="message-1")
        api = ScriptedPipelineApi(
            gets=tuple(
                TransientLookupError("rate", http_status=429) for _ in range(7)
            )
        )
        pipeline, clock, _, _ = self.make_pipeline(api, row)
        result = pipeline.run(row, work("RECONCILE", True))

        self.assertEqual(result.row.delivery_status, "PENDING_CONFIRMATION")
        self.assertEqual([call[0] for call in api.calls].count("get"), 7)
        self.assertGreaterEqual(clock.monotonic(), 120)

    def test_acceptance_logging_latency_consumes_the_same_confirmation_window(self):
        """Catches durable response logging extending confirmation beyond 120 seconds."""
        log = AdvancingEventLog()
        api = ScriptedPipelineApi(
            sends=(send_response(),),
            lists=(list_response(message("PROCESSING")),),
            gets=tuple(TransientLookupError("temporary") for _ in range(130)),
        )
        pipeline, clock, _, _ = self.make_pipeline(
            api,
            reservation(),
            event_log=log,
        )
        log.clock = clock
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "PENDING_CONFIRMATION")
        self.assertEqual([call[0] for call in api.calls].count("get"), 115)
        self.assertEqual(clock.monotonic(), 120)

    def test_retry_and_rate_deadlines_merge_at_later_deadline_not_sum(self):
        """Catches a POST 429 adding the ten-second retry delay to the rate gate."""
        api = ScriptedPipelineApi(
            sends=(
                ExplicitApiFailure("429", "private", http_status=429),
                send_response(),
            ),
            lists=(list_response(message("COMPLETED", "success")),),
            gets=(result_response(message("COMPLETED", "success")),),
        )
        pipeline, clock, _, _ = self.make_pipeline(api, reservation())
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(clock.sleeps, [10])
        self.assertEqual([call[0] for call in api.calls][:2], ["send", "send"])

    def test_invalid_work_action_is_rejected_before_api(self):
        """Catches unapproved action text falling through to a POST path."""
        row = reservation()
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, row)
        invalid = ApprovedWork(NUMBER, "NOT_APPROVED", True, DELIVERY_ID)
        with self.assertRaisesRegex(ResultFormatError, "approved work action invalid"):
            pipeline.run(row, invalid, ("file-1", "file-2"))
        self.assertEqual(api.calls, [])

    def test_work_recipient_mismatch_is_rejected_before_api(self):
        """Catches a work item authorizing a different recipient's row."""
        row = reservation()
        api = ScriptedPipelineApi(sends=(send_response(),))
        pipeline, _, _, _ = self.make_pipeline(api, row)
        mismatched = ApprovedWork(OTHER, "RESUME_RESERVATION", True, DELIVERY_ID)
        with self.assertRaisesRegex(ResultFormatError, "approved work recipient mismatch"):
            pipeline.run(row, mismatched, ("file-1", "file-2"))
        self.assertEqual(api.calls, [])

    def test_split_dispatch_two_stage_success(self):
        """Verify that split dispatch sends empty MMS, then resets attempts, then sends LMS with content."""
        api = ScriptedPipelineApi(
            sends=(
                send_response(request_id="mms-req"),
                send_response(request_id="lms-req"),
            ),
            lists=(
                list_response(message("PROCESSING", request_id="mms-req", message_id="mms-msg")),
                list_response(message("PROCESSING", request_id="lms-req", message_id="lms-msg", message_type="LMS")),
            ),
            gets=(
                result_response(message("COMPLETED", "success", request_id="mms-req", message_id="mms-msg")),
                result_response(message("COMPLETED", "success", request_id="lms-req", message_id="lms-msg", message_type="LMS")),
            ),
        )
        pipeline, clock, log, _ = self.make_pipeline(api, reservation(), split=True)
        result = pipeline.run(reservation(), work(), ("file-1", "file-2"))

        title, lms_body = split_message_body(MESSAGE_BODY)

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(result.row.is_sent, "true")
        self.assertEqual(result.row.attempts, 1) # LMS attempts
        self.assertEqual(result.row.request_id, "lms-req")
        self.assertEqual(result.row.message_id, "lms-msg")
        
        self.assertEqual(
            [call[0] for call in api.calls],
            ["send_mms", "list", "get", "send_lms", "list", "get"]
        )
        self.assertEqual(api.calls[0][2:], (("file-1", "file-2"), "COMM", title))
        self.assertEqual(api.calls[3][2:], ("COMM", lms_body))

    def test_split_dispatch_reconciliation_mms_success(self):
        """Verify that reconciliation resolves a successful MMS stage and automatically launches the LMS stage."""
        title, lms_body = split_message_body(MESSAGE_BODY)
        api = ScriptedPipelineApi(
            sends=(
                send_response(request_id="lms-req"),
            ),
            lists=(
                list_response(message("PROCESSING", request_id="lms-req", message_id="lms-msg", message_type="LMS")),
            ),
            gets=(
                result_response(message("COMPLETED", "success", request_id="mms-req", message_id="mms-msg", message_type="MMS")),
                result_response(message("COMPLETED", "success", request_id="lms-req", message_id="lms-msg", message_type="LMS")),
            ),
        )
        row = replace(
            reservation(),
            delivery_status="PENDING_CONFIRMATION",
            attempts=1,
            request_id="mms-req",
            message_id="mms-msg",
        )
        pipeline, clock, log, _ = self.make_pipeline(api, row, split=True)
        result = pipeline.run(row, work("RECONCILE", True))

        self.assertEqual(result.row.delivery_status, "SENT")
        self.assertEqual(result.row.is_sent, "true")
        self.assertEqual(result.row.attempts, 1) # LMS attempts
        self.assertEqual(result.row.request_id, "lms-req")
        self.assertEqual(result.row.message_id, "lms-msg")

        self.assertEqual(
            [call[0] for call in api.calls],
            ["get", "send_lms", "list", "get"]
        )
        self.assertEqual(api.calls[1][2:], ("COMM", lms_body))


if __name__ == "__main__":
    unittest.main()
