import csv
from dataclasses import replace
from datetime import datetime, timedelta
import io
import json
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import sens_mms.workflow as workflow_module
from sens_mms.api import (
    AmbiguousPostOutcome,
    ApiResponse,
    ExplicitApiFailure,
    MessageListResponse,
    MessageRecord,
    MessageResultResponse,
    SendResponse,
    SensClient,
    TransientLookupError,
)
from sens_mms.config import Config
from sens_mms.coordination import RUN_SETTINGS, RunCoordinator
from sens_mms.event_log import EventLogError, JsonlEventLog
from sens_mms.inputs import MESSAGE_BODY
from sens_mms.results import ResultFormatError, ResultRow, ResultStore
from sens_mms.workflow import Workflow


RECIPIENT = "01012345678"
DELIVERY_ID_1 = "7K3M9Q2X8C4N6P7R"
DELIVERY_ID_2 = "8L4N2R3Y9D5P7Q6S"
DELIVERY_ID_3 = "9M5P3S4Z2E6Q8R7T"
DELIVERY_ID_4 = "AAAAAAAAAAAAAAAA"
FAKE_ACCESS_KEY = "FAKE_ACCESS_KEY_SHOULD_NOT_LOG"
FAKE_SECRET_KEY = "FAKE_SECRET_KEY_SHOULD_NOT_LOG"
FAKE_SERVICE_ID = "FAKE_SERVICE_ID_SHOULD_NOT_LOG"
SENDER = "0212345678"
RAW_URL = "https://sens.example/messages?recipient=01012345678&secret=fake"


def tiny_jpeg(width=10, height=10):
    return (
        b"\xff\xd8\xff\xc0\x00\x11\x08"
        + height.to_bytes(2, "big")
        + width.to_bytes(2, "big")
        + b"\x03\x01\x11\x00\x02\x11\x00\x03\x11\x00\xff\xd9"
    )


def make_root(numbers=(RECIPIENT,)):
    root = Path(tempfile.mkdtemp())
    (root / "receiving_numbers.csv").write_text(
        "number\n" + "\n".join(numbers) + "\n", encoding="utf-8"
    )
    image_dir = root / "mms_img"
    image_dir.mkdir()
    (image_dir / "mms_01_intro.jpg").write_bytes(tiny_jpeg())
    (image_dir / "mms_02_details.jpg").write_bytes(tiny_jpeg())
    return root


def message(
    status,
    status_name="",
    status_code="",
    status_message="",
    *,
    request_id="request-1",
    message_id="message-1",
    to=RECIPIENT,
    message_type="MMS",
):
    return MessageRecord(
        request_id=request_id,
        message_id=message_id,
        to=to,
        request_time="2026-08-13 10:00:00",
        complete_time=(
            "" if status != "COMPLETED" else "2026-08-13 10:00:30"
        ),
        telco_code="",
        status=status,
        status_code=status_code,
        status_name=status_name,
        status_message=status_message,
        message_type=message_type,
    )


def send_response(request_id="request-1"):
    return SendResponse(
        http_status=202,
        request_id=request_id,
        request_time="2026-08-13 10:00:00",
        status_code="202",
        status_name="success",
    )


def list_response(*messages):
    return MessageListResponse(
        http_status=200,
        status_code="202",
        status_name="success",
        messages=tuple(messages),
        page_size=100,
        page_index=0,
        item_count=len(messages),
        has_more=False,
    )


def result_response(record):
    return MessageResultResponse(
        http_status=200,
        status_code="200",
        status_name="success",
        message=record,
    )


class FakeClock:
    def __init__(self, on_sleep=None):
        self.elapsed = 0.0
        self.base = datetime(2026, 8, 13, 10, 0, 0)
        self.sleeps = []
        self.on_sleep = on_sleep

    def monotonic(self):
        return self.elapsed

    def now(self):
        return self.base + timedelta(seconds=self.elapsed)

    def sleep(self, seconds):
        if self.on_sleep is not None:
            self.on_sleep(seconds)
        self.sleeps.append(seconds)
        self.elapsed += seconds


class HostSensitiveNaive(datetime):
    def astimezone(self, tz=None):
        return datetime(2026, 8, 13, 12, 0, 0).replace(tzinfo=tz)

    def replace(self, **kwargs):
        plain = datetime(
            self.year,
            self.month,
            self.day,
            self.hour,
            self.minute,
            self.second,
            self.microsecond,
        )
        return plain.replace(**kwargs)


class HostSensitiveClock(FakeClock):
    def __init__(self):
        super().__init__()
        self.base = HostSensitiveNaive(2026, 8, 14, 3, 0, 0)


class RecordingEventLog:
    def __init__(self, root):
        self.path = Path(root) / "logs" / "test.jsonl"
        self.events = []

    def write(self, event, **fields):
        self.events.append((event, fields))


class FailingEventLog(RecordingEventLog):
    def __init__(self, root, failures):
        super().__init__(root)
        self.failures = {event: list(errors) for event, errors in failures.items()}
        self.write_attempts = []

    def write(self, event, **fields):
        self.write_attempts.append(event)
        errors = self.failures.get(event, [])
        if errors:
            raise errors.pop(0)
        super().write(event, **fields)


class MutatingStartedEventLog(RecordingEventLog):
    def __init__(self, root, mutation):
        super().__init__(root)
        self.mutation = mutation

    def write(self, event, **fields):
        super().write(event, **fields)
        if event == "WORKFLOW_STARTED":
            self.mutation()


class FixedIdFactory:
    def __init__(self, *values):
        self.values = iter(values)

    def __call__(self, existing_ids):
        value = next(self.values)
        if value in existing_ids:
            raise AssertionError("test ID collision")
        return value


class MutatingIdFactory(FixedIdFactory):
    def __init__(self, mutation, *values):
        super().__init__(*values)
        self.mutation = mutation

    def __call__(self, existing_ids):
        value = super().__call__(existing_ids)
        self.mutation()
        return value


class ScriptedApi:
    def __init__(
        self,
        *,
        sends,
        lists,
        gets,
        time_lists=(),
        on_send=None,
        on_list=None,
        on_time_list=None,
        on_get=None,
    ):
        self.sends = list(sends)
        self.lists = list(lists)
        self.gets = list(gets)
        self.time_lists = list(time_lists)
        self.on_send = on_send
        self.on_list = on_list
        self.on_time_list = on_time_list
        self.on_get = on_get
        self.uploaded = []
        self.sent = []
        self.listed_requests = []
        self.time_listed = []
        self.got = []
        self.sent_content_types = []
        self.time_list_types = []

    def upload_file(self, path):
        self.uploaded.append(Path(path).name)
        return f"file-{len(self.uploaded)}"

    def upload_bytes(self, name, data):
        self.uploaded.append(name)
        return f"file-{len(self.uploaded)}"

    def send_one(self, to, file_ids, *, content_type):
        self.sent.append((to, tuple(file_ids)))
        self.sent_content_types.append(content_type)
        if self.on_send is not None:
            self.on_send()
        value = self.sends.pop(0)
        if isinstance(value, BaseException):
            raise value
        if not isinstance(value, SendResponse):
            raise AssertionError("send fixture must return SendResponse")
        return value

    def list_by_request(self, request_id):
        self.listed_requests.append(request_id)
        if self.on_list is not None:
            self.on_list()
        value = self.lists.pop(0)
        if isinstance(value, BaseException):
            raise value
        if not isinstance(value, MessageListResponse):
            raise AssertionError("list fixture must return MessageListResponse")
        return value

    def list_by_time_and_recipient(self, start, end, to, *, message_type):
        self.time_listed.append((start, end, to))
        self.time_list_types.append(message_type)
        if self.on_time_list is not None:
            self.on_time_list()
        value = self.time_lists.pop(0)
        if isinstance(value, BaseException):
            raise value
        if not isinstance(value, MessageListResponse):
            raise AssertionError("time-list fixture must return MessageListResponse")
        return value

    def get_message(self, message_id):
        self.got.append(message_id)
        if self.on_get is not None:
            self.on_get()
        value = self.gets.pop(0)
        if isinstance(value, BaseException):
            raise value
        if not isinstance(value, MessageResultResponse):
            raise AssertionError("get fixture must return MessageResultResponse")
        return value


class BlockingUploadApi(ScriptedApi):
    def __init__(self, *, entered, release, **kwargs):
        super().__init__(**kwargs)
        self.entered = entered
        self.release = release

    def upload_file(self, path):
        self.entered.set()
        if not self.release.wait(5):
            raise AssertionError("test upload release timed out")
        return super().upload_file(path)

    def upload_bytes(self, name, data):
        self.entered.set()
        if not self.release.wait(5):
            raise AssertionError("test upload release timed out")
        return super().upload_bytes(name, data)


class ByteCapturingApi(ScriptedApi):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.uploaded_bytes = []

    def upload_file(self, path):
        path = Path(path)
        self.uploaded_bytes.append((path.name, path.read_bytes()))
        return super().upload_file(path)

    def upload_bytes(self, name, data):
        self.uploaded_bytes.append((name, data))
        self.uploaded.append(name)
        return f"file-{len(self.uploaded)}"


class RealApiTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body, timeout))
        return self.responses.pop(0)


def raw_response(status, payload):
    return ApiResponse(
        status,
        json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        {},
    )


class SnapshotMutatingStore(ResultStore):
    def snapshot(self, completed_at):
        snapshot = super().snapshot(completed_at)
        self.upsert(
            ResultRow(
                receiving_number="01055556666",
                delivery_id="",
                delivery_status="FAILED",
                is_sent="false",
                attempts=0,
                request_id="",
                message_id="",
                error={"status": "LATE_CHANGE", "message": "after snapshot"},
            )
        )
        self.write_atomic()
        return snapshot


class RecordingResultStore(ResultStore):
    def __init__(self, path):
        super().__init__(path)
        self.write_history = []

    def write_atomic(self):
        super().write_atomic()
        self.write_history.append(self.path.read_bytes())


class ObservingSnapshotStore(ResultStore):
    def __init__(self, path):
        super().__init__(path)
        self.completed_at_seen = None

    def snapshot(self, completed_at):
        self.completed_at_seen = completed_at
        return super().snapshot(completed_at)


class FailingCheckpointStore(ResultStore):
    def __init__(self, path, fail_on_write):
        super().__init__(path)
        self.fail_on_write = fail_on_write
        self.write_count = 0

    def write_atomic(self):
        self.write_count += 1
        if self.write_count == self.fail_on_write:
            raise OSError("checkpoint unavailable")
        super().write_atomic()


class ThreadSafeClock(FakeClock):
    def __init__(self):
        super().__init__()
        self._lock = threading.Lock()

    def monotonic(self):
        with self._lock:
            return self.elapsed

    def now(self):
        with self._lock:
            return self.base + timedelta(seconds=self.elapsed)

    def sleep(self, seconds):
        with self._lock:
            self.sleeps.append(seconds)
            self.elapsed += seconds


class OrchestrationApi:
    """Complete deterministic SENS boundary fake for workflow integration tests."""

    def __init__(
        self,
        *,
        pending=(),
        send_outcomes=None,
        upload_failures=(),
        pending_done=None,
        send_barrier=None,
        pending_barrier=None,
        on_upload=None,
    ):
        self._lock = threading.Lock()
        self._pending_done = pending_done
        self._send_barrier = send_barrier
        self._pending_barrier = pending_barrier
        self._on_upload = on_upload
        self._barrier_entries = 0
        self._pending_barrier_entries = 0
        self._upload_failures = list(upload_failures)
        self._send_outcomes = {
            number: list(outcomes)
            for number, outcomes in (send_outcomes or {}).items()
        }
        self._request_to = {}
        self._message_to = {}
        self._outcomes = {}
        self._new_messages = set()
        for row, outcomes in pending:
            resolved_message_id = row.message_id or f"pending-message-{row.receiving_number[-4:]}"
            self._request_to[row.request_id] = (
                row.receiving_number,
                resolved_message_id,
            )
            self._message_to[resolved_message_id] = (
                row.receiving_number,
                row.request_id,
            )
            self._outcomes[resolved_message_id] = list(outcomes)
        self.trace = []
        self.uploaded = []
        self.sent = []
        self.threads = {}
        self.maximum_active = 0
        self.active = 0
        self.maximum_pending_active = 0
        self.pending_active = 0

    def _record(self, label):
        with self._lock:
            self.trace.append(label)

    def _record_thread(self, number):
        with self._lock:
            self.threads.setdefault(number, set()).add(threading.get_ident())

    def index(self, label):
        return self.trace.index(label)

    def upload_bytes(self, name, data):
        self._record(f"upload:{len(self.uploaded) + 1}")
        if self._pending_done is not None and not self._pending_done.is_set():
            raise AssertionError("upload started before pending reconciliation joined")
        with self._lock:
            self.uploaded.append((name, bytes(data)))
            failure = self._upload_failures.pop(0) if self._upload_failures else None
            upload_number = len(self.uploaded)
        if failure is not None:
            raise failure
        if self._on_upload is not None:
            self._on_upload(upload_number)
        return f"file-{upload_number}"

    def send_one(self, to, file_ids, *, content_type):
        self._record_thread(to)
        wait_at_barrier = False
        with self._lock:
            self.active += 1
            self.maximum_active = max(self.maximum_active, self.active)
            self._barrier_entries += 1
            wait_at_barrier = (
                self._send_barrier is not None
                and self._barrier_entries <= self._send_barrier.parties
            )
            attempt = sum(call[0] == to for call in self.sent) + 1
            self.sent.append((to, tuple(file_ids), content_type))
            self.trace.append(f"{to}:send")
        if wait_at_barrier:
            self._send_barrier.wait()
        request_id = f"request-{to[-4:]}-{attempt}"
        message_id = f"message-{to[-4:]}-{attempt}"
        with self._lock:
            outcomes = self._send_outcomes.setdefault(to, ["success"])
            outcome = outcomes.pop(0) if outcomes else "success"
            if isinstance(outcome, BaseException):
                self.active -= 1
                raise outcome
            self._request_to[request_id] = (to, message_id)
            self._message_to[message_id] = (to, request_id)
            self._outcomes[message_id] = [outcome]
            self._new_messages.add(message_id)
        return send_response(request_id)

    def list_by_request(self, request_id):
        with self._lock:
            number, message_id = self._request_to[request_id]
            is_pending = message_id not in self._new_messages
            if is_pending:
                self.pending_active += 1
                self.maximum_pending_active = max(
                    self.maximum_pending_active,
                    self.pending_active,
                )
                self._pending_barrier_entries += 1
                wait_at_barrier = (
                    self._pending_barrier is not None
                    and self._pending_barrier_entries <= self._pending_barrier.parties
                )
            else:
                wait_at_barrier = False
        self._record_thread(number)
        self._record(f"{number}:list")
        if wait_at_barrier:
            self._pending_barrier.wait()
        return list_response(
            message(
                "READY",
                request_id=request_id,
                message_id=message_id,
                to=number,
            )
        )

    def list_by_time_and_recipient(self, start, end, to, *, message_type):
        self._record_thread(to)
        self._record(f"{to}:time-list")
        return list_response()

    def get_message(self, message_id):
        with self._lock:
            number, request_id = self._message_to[message_id]
            outcomes = self._outcomes[message_id]
            outcome = outcomes.pop(0) if outcomes else "ready"
        self._record_thread(number)
        self._record(f"{number}:get")
        if isinstance(outcome, BaseException):
            raise outcome
        if outcome == "ready":
            record = message(
                "READY",
                request_id=request_id,
                message_id=message_id,
                to=number,
            )
        elif outcome == "fail":
            record = message(
                "COMPLETED",
                "fail",
                "3001",
                "carrier detail",
                request_id=request_id,
                message_id=message_id,
                to=number,
            )
        else:
            record = message(
                "COMPLETED",
                "success",
                "0",
                request_id=request_id,
                message_id=message_id,
                to=number,
            )
        if record.status == "COMPLETED":
            with self._lock:
                if message_id in self._new_messages:
                    self._new_messages.remove(message_id)
                    self.active -= 1
                elif self.pending_active:
                    self.pending_active -= 1
                self.trace.append(f"{number}:get:done")
            if self._pending_done is not None:
                self._pending_done.set()
        return result_response(record)


def pending_result(
    number,
    delivery_id,
    *,
    attempts=1,
    request_id=None,
    message_id=None,
):
    return ResultRow(
        receiving_number=number,
        delivery_id=delivery_id,
        delivery_status="PENDING_CONFIRMATION",
        is_sent="",
        attempts=attempts,
        request_id=(f"pending-request-{number[-4:]}" if request_id is None else request_id),
        message_id=(f"pending-message-{number[-4:]}" if message_id is None else message_id),
        error=None,
    )


def reservation_result(number, delivery_id):
    return pending_result(
        number,
        delivery_id,
        attempts=0,
        request_id="",
        message_id="",
    )


class JoinTrackingWorkflow(Workflow):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.joined_workers = 0
        self._joined_lock = threading.Lock()

    def _run_one(self, row, work, file_ids):
        try:
            return super()._run_one(row, work, file_ids)
        finally:
            with self._joined_lock:
                self.joined_workers += 1


class WorkerGateCoordinator(RunCoordinator):
    barrier = None
    main_thread = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._worker_gate_local = threading.local()

    def before_api_call(self):
        is_worker = threading.get_ident() != self.main_thread
        if is_worker and not getattr(self._worker_gate_local, "entered", False):
            self._worker_gate_local.entered = True
            self.barrier.wait()
        return super().before_api_call()


class SerializationCheckingEventLog(RecordingEventLog):
    def __init__(self, root):
        super().__init__(root)
        self._active_lock = threading.Lock()
        self.active_writes = 0
        self.maximum_active_writes = 0

    def write(self, event, **fields):
        with self._active_lock:
            self.active_writes += 1
            self.maximum_active_writes = max(
                self.maximum_active_writes,
                self.active_writes,
            )
        try:
            super().write(event, **fields)
        finally:
            with self._active_lock:
                self.active_writes -= 1


class SerializationCheckingStore(RecordingResultStore):
    def __init__(self, path):
        super().__init__(path)
        self._active_lock = threading.Lock()
        self.active_writes = 0
        self.maximum_active_writes = 0

    def write_atomic(self):
        with self._active_lock:
            self.active_writes += 1
            self.maximum_active_writes = max(
                self.maximum_active_writes,
                self.active_writes,
            )
        try:
            super().write_atomic()
        finally:
            with self._active_lock:
                self.active_writes -= 1


def config():
    return Config(
        FAKE_ACCESS_KEY,
        FAKE_SECRET_KEY,
        FAKE_SERVICE_ID,
        SENDER,
        "COMM",
    )


def make_workflow(
    root,
    store,
    api,
    clock=None,
    event_log=None,
    delivery_id_factory=None,
    *,
    resend_failed=False,
):
    return Workflow(
        root,
        config(),
        store,
        api,
        clock or FakeClock(),
        event_log or RecordingEventLog(root),
        delivery_id_factory
        or FixedIdFactory(DELIVERY_ID_1, DELIVERY_ID_2, DELIVERY_ID_3),
        resend_failed=resend_failed,
    )


def seed_resend_rows(root, rows):
    store = ResultStore.for_root(root)
    for row in rows:
        store.upsert(row)
    store.write_atomic()
    store.snapshot(datetime(2026, 8, 12, 9, 0, 0))
    return store


def failed_result(number, delivery_id, request_id, message_id):
    return ResultRow(
        receiving_number=number,
        delivery_id=delivery_id,
        delivery_status="FAILED",
        is_sent="false",
        attempts=3,
        request_id=request_id,
        message_id=message_id,
        error={"status": "3001", "message": "redacted"},
    )


class WorkflowTests(unittest.TestCase):
    def assert_event_privacy(self, log, *additional_secrets):
        serialized = json.dumps(log.events, ensure_ascii=False)
        forbidden = (
            RECIPIENT,
            SENDER,
            MESSAGE_BODY,
            "file-1",
            "file-2",
            FAKE_ACCESS_KEY,
            FAKE_SECRET_KEY,
            FAKE_SERVICE_ID,
            RAW_URL,
            *additional_secrets,
        )
        for value in forbidden:
            self.assertNotIn(value, serialized)

    def assert_arbitrary_lookup_message_discarded(self, explicit_message):
        root = make_root()
        log = RecordingEventLog(root)
        failure = TransientLookupError("safe fixed exception args are ignored")
        failure.message = explicit_message
        api = ScriptedApi(
            sends=[send_response()],
            lists=[failure] * 120,
            gets=[],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        lookup_fields = next(
            fields for event, fields in log.events if event == "API_LOOKUP_ERROR"
        )
        self.assertEqual(
            lookup_fields["error"],
            {
                "status": "TRANSIENT_LOOKUP_ERROR",
                "message": "redacted",
            },
        )
        self.assertNotIn(
            explicit_message,
            json.dumps(lookup_fields, ensure_ascii=False),
        )

    def test_all_pending_futures_join_before_upload_and_new_post(self):
        """Catches phase two starting while a pending reconciliation still runs."""
        pending_number = "01000000001"
        new_number = "01000000002"
        pending = pending_result(
            pending_number,
            "AAAAAAAAAAAAAAA2",
        )
        root = make_root((pending_number, new_number))
        store = ResultStore.for_root(root)
        store.upsert(pending)
        store.write_atomic()
        pending_done = threading.Event()
        api = OrchestrationApi(
            pending=((pending, ("success",)),),
            pending_done=pending_done,
        )
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            delivery_id_factory=FixedIdFactory("AAAAAAAAAAAAAAA3"),
        )

        workflow.run_live(workflow.current_token())

        self.assertLess(
            api.index(f"{pending_number}:get:done"),
            api.index("upload:1"),
        )
        self.assertLess(api.index("upload:2"), api.index(f"{new_number}:send"))

    def test_pending_only_terminal_pending_and_ambiguous_work_upload_nothing(self):
        """Catches unconditional image upload when no approved POST remains."""
        cases = (
            ("terminal", "success", "pending-request", "pending-message"),
            ("pending", "ready", "pending-request", "pending-message"),
            ("ambiguous", None, "", ""),
        )
        for label, outcome, request_id, message_id in cases:
            with self.subTest(label=label):
                number = "01000000003"
                root = make_root((number,))
                row = pending_result(
                    number,
                    "AAAAAAAAAAAAAAA4",
                    request_id=request_id,
                    message_id=message_id,
                )
                store = ResultStore.for_root(root)
                store.upsert(row)
                store.write_atomic()
                pending = () if outcome is None else ((row, (outcome,)),)
                api = OrchestrationApi(pending=pending)
                workflow = make_workflow(
                    root,
                    store,
                    api,
                    clock=ThreadSafeClock(),
                    delivery_id_factory=FixedIdFactory(),
                )

                workflow.run_live(workflow.current_token())

                self.assertEqual(api.uploaded, [])
                self.assertEqual(api.sent, [])

    def test_pending_explicit_failure_uploads_only_when_retry_is_approved(self):
        """Catches retrying exhausted failures or skipping an approved retry."""
        for attempts, expected_uploads, expected_sends in ((1, 2, 1), (3, 0, 0)):
            with self.subTest(attempts=attempts):
                number = "01000000004"
                root = make_root((number,))
                row = pending_result(
                    number,
                    "AAAAAAAAAAAAAAA5",
                    attempts=attempts,
                )
                store = ResultStore.for_root(root)
                store.upsert(row)
                store.write_atomic()
                api = OrchestrationApi(
                    pending=((row, ("fail",)),),
                    send_outcomes={number: ("success",)},
                )
                clock = ThreadSafeClock()
                workflow = make_workflow(
                    root,
                    store,
                    api,
                    clock=clock,
                    delivery_id_factory=FixedIdFactory(),
                )

                workflow.run_live(workflow.current_token())

                self.assertEqual(len(api.uploaded), expected_uploads)
                self.assertEqual(len(api.sent), expected_sends)
                self.assertEqual(clock.sleeps.count(RUN_SETTINGS.retry_delay_seconds), expected_sends)

    def test_retry_rejects_any_reconciled_row_change_during_upload(self):
        """Catches a retry silently adopting a changed durable row after approval."""
        mutations = (
            ("attempts", {"attempts": 2}),
            ("request_id", {"request_id": "changed-request"}),
            ("message_id", {"message_id": "changed-message"}),
        )
        for label, changes in mutations:
            with self.subTest(field=label):
                number = "01000000014"
                root = make_root((number,))
                row = pending_result(
                    number,
                    "CCCCCCCCCCCCCCC6",
                    attempts=1,
                )
                store = ResultStore.for_root(root)
                store.upsert(row)
                store.write_atomic()
                changed = replace(row, **changes)

                def mutate_checkpoint(upload_number):
                    if upload_number != 1:
                        return
                    outside_writer = ResultStore.for_root(root)
                    outside_writer.upsert(changed)
                    outside_writer.write_atomic()

                api = OrchestrationApi(
                    pending=((row, ("fail",)),),
                    send_outcomes={number: ("success",)},
                    on_upload=mutate_checkpoint,
                )
                workflow = make_workflow(
                    root,
                    store,
                    api,
                    clock=ThreadSafeClock(),
                    delivery_id_factory=FixedIdFactory(),
                )

                with self.assertRaisesRegex(
                    ResultFormatError,
                    "approved retry row does not match current checkpoint",
                ):
                    workflow.run_live(workflow.current_token())

                self.assertEqual(len(api.uploaded), 2)
                self.assertEqual(api.sent, [])
                self.assertEqual(ResultStore.for_root(root).load()[number], changed)

    def test_upload_failure_and_429_never_retry_upload_or_start_message_post(self):
        """Catches attachment retries and POST continuation after uncertain upload failure."""
        cases = (
            (
                (ExplicitApiFailure("500", "first upload failed", http_status=500),),
                1,
                0,
            ),
            (
                (None, ExplicitApiFailure("500", "second upload failed", http_status=500)),
                2,
                0,
            ),
            (
                (ExplicitApiFailure("429", "rate limited", http_status=429),),
                1,
                1,
            ),
        )
        for upload_failures, expected_uploads, expected_429_count in cases:
            failure = next(item for item in upload_failures if item is not None)
            with self.subTest(
                http_status=failure.http_status,
                expected_uploads=expected_uploads,
            ):
                number = "01000000005"
                root = make_root((number,))
                store = ResultStore.for_root(root)
                api = OrchestrationApi(upload_failures=upload_failures)
                log = RecordingEventLog(root)
                workflow = make_workflow(
                    root,
                    store,
                    api,
                    clock=ThreadSafeClock(),
                    event_log=log,
                    delivery_id_factory=FixedIdFactory("AAAAAAAAAAAAAAA6"),
                )

                with self.assertRaises(ExplicitApiFailure) as raised:
                    workflow.run_live(workflow.current_token())

                self.assertIs(raised.exception, failure)
                self.assertEqual(len(api.uploaded), expected_uploads)
                self.assertEqual(api.sent, [])
                self.assertEqual(store.load(), {})
                self.assertNotIn(
                    "WORKFLOW_COMPLETED",
                    [event for event, _ in log.events],
                )
                self.assertEqual(
                    workflow.coordinator._rate_limit_count,
                    expected_429_count,
                )
                upload_event = next(
                    fields
                    for event, fields in log.events
                    if event == "ATTACHMENT_UPLOAD_FAILED"
                )
                self.assertEqual(
                    upload_event["error"],
                    {"status": "UNKNOWN", "message": "redacted"},
                )
                self.assertNotIn(
                    "mms_",
                    json.dumps(upload_event, ensure_ascii=False),
                )

    def test_hold_ambiguous_work_never_enters_recipient_executor(self):
        """Catches an uncertain blank-correlation row being submitted to a worker."""
        number = "01000000011"
        root = make_root((number,))
        row = pending_result(
            number,
            "CCCCCCCCCCCCCCC3",
            request_id="",
            message_id="",
        )
        store = ResultStore.for_root(root)
        store.upsert(row)
        store.write_atomic()
        api = OrchestrationApi()
        workflow = JoinTrackingWorkflow(
            root,
            config(),
            store,
            api,
            ThreadSafeClock(),
            RecordingEventLog(root),
            FixedIdFactory(),
        )

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual((summary.pending, workflow.joined_workers), (1, 0))
        self.assertEqual((api.uploaded, api.sent), ([], []))

    def test_reservation_event_failure_starts_no_worker_or_message_post(self):
        """Catches workers starting before every reservation event is durable."""
        numbers = ("01000000012", "01000000013")
        root = make_root(numbers)
        store = ResultStore.for_root(root)
        original = EventLogError("reservation event unavailable")
        log = FailingEventLog(root, {"DELIVERY_ID_ASSIGNED": [original]})
        api = OrchestrationApi()
        workflow = JoinTrackingWorkflow(
            root,
            config(),
            store,
            api,
            ThreadSafeClock(),
            log,
            FixedIdFactory(
                "CCCCCCCCCCCCCCC4",
                "CCCCCCCCCCCCCCC5",
            ),
        )

        with self.assertRaises(EventLogError) as raised:
            workflow.run_live(workflow.current_token())

        self.assertIs(raised.exception, original)
        self.assertEqual(workflow.joined_workers, 0)
        self.assertEqual(api.sent, [])
        self.assertEqual(set(store.load()), set(numbers))
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])

    def test_reservations_publish_atomically_before_workers_and_preserve_action_identity(self):
        """Catches partial reservation visibility and changed resume/retry identities."""
        failed_number = "01000000006"
        resume_number = "01000000007"
        retry_number = "01000000008"
        failed = failed_result(
            failed_number,
            "AAAAAAAAAAAAAAA7",
            "old-request",
            "old-message",
        )
        resume = reservation_result(resume_number, "AAAAAAAAAAAAAAA8")
        retry = pending_result(retry_number, "AAAAAAAAAAAAAAA9")
        root = make_root((failed_number, resume_number, retry_number))
        store = RecordingResultStore(ResultStore.for_root(root).path)
        for row in (failed, resume, retry):
            store.upsert(row)
        store.write_atomic()
        store.snapshot(datetime(2026, 8, 12, 9, 0, 0))
        store.write_history.clear()
        log = RecordingEventLog(root)
        api = OrchestrationApi(
            pending=((retry, ("fail",)),),
            send_outcomes={
                failed_number: ("success",),
                resume_number: ("success",),
                retry_number: ("success",),
            },
        )
        new_id = "BBBBBBBBBBBBBBB2"
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            event_log=log,
            delivery_id_factory=FixedIdFactory(new_id),
            resend_failed=True,
        )

        workflow.run_live(workflow.current_token())

        first_write = list(
            csv.DictReader(io.StringIO(store.write_history[0].decode("utf-8")))
        )
        reserved = {fields["receiving_number"]: fields for fields in first_write}
        self.assertEqual(set(reserved), {failed_number, resume_number, retry_number})
        self.assertEqual(
            (reserved[failed_number]["delivery_id"], reserved[failed_number]["attempts"]),
            (new_id, "0"),
        )
        self.assertEqual(
            (reserved[resume_number]["delivery_id"], reserved[resume_number]["attempts"]),
            (resume.delivery_id, "0"),
        )
        self.assertEqual(
            (reserved[retry_number]["delivery_id"], reserved[retry_number]["attempts"]),
            (retry.delivery_id, "1"),
        )
        self.assertEqual(
            {call[1] for call in api.sent},
            {("file-1", "file-2")},
        )
        self.assertTrue(all(call[2] == "COMM" for call in api.sent))
        first_send = min(
            api.index(f"{number}:send")
            for number in (failed_number, resume_number, retry_number)
        )
        self.assertLess(api.index("upload:2"), first_send)
        assigned_before_send = [
            fields["delivery_id"]
            for event, fields in log.events
            if event == "DELIVERY_ID_ASSIGNED"
        ]
        self.assertIn(new_id, assigned_before_send)
        self.assertIn(resume.delivery_id, assigned_before_send)

    def test_new_delivery_ids_are_rejected_when_present_only_in_a_snapshot(self):
        """Catches reusing an ID that disappeared from the current checkpoint."""
        number = "01000000009"
        snapshot_only_id = "CCCCCCCCCCCCCCC2"
        root = make_root((number,))
        snapshot_store = ResultStore.for_root(root)
        snapshot_store.upsert(
            failed_result(
                "01000000010",
                snapshot_only_id,
                "snapshot-request",
                "snapshot-message",
            )
        )
        snapshot_store.snapshot(datetime(2026, 8, 11, 9, 0, 0))
        snapshot_store.rows = {}
        snapshot_store.write_atomic()
        api = OrchestrationApi()
        workflow = make_workflow(
            root,
            snapshot_store,
            api,
            clock=ThreadSafeClock(),
            delivery_id_factory=lambda existing: snapshot_only_id,
        )

        with self.assertRaisesRegex(ResultFormatError, "duplicate"):
            workflow.run_live(workflow.current_token())

        self.assertEqual(len(api.uploaded), 2)
        self.assertEqual(api.sent, [])

    def test_reconcile_and_send_phases_each_use_five_workers_with_thread_ownership(self):
        """Catches wrong pool bounds and a recipient changing threads mid-pipeline."""
        pending_numbers = tuple(f"010000001{index:02d}" for index in range(1, 8))
        new_numbers = tuple(f"010000002{index:02d}" for index in range(1, 8))
        root = make_root(pending_numbers + new_numbers)
        store = SerializationCheckingStore(ResultStore.for_root(root).path)
        pending_rows = []
        for index, number in enumerate(pending_numbers, start=2):
            row = pending_result(
                number,
                f"DDDDDDDDDDDDDDD{index}",
                message_id="",
            )
            store.upsert(row)
            pending_rows.append(row)
        store.write_atomic()
        store.write_history.clear()
        log = SerializationCheckingEventLog(root)
        api = OrchestrationApi(
            pending=tuple((row, ("success",)) for row in pending_rows),
            pending_barrier=threading.Barrier(RUN_SETTINGS.worker_count),
            send_barrier=threading.Barrier(RUN_SETTINGS.worker_count),
        )
        new_ids = tuple(f"EEEEEEEEEEEEEEE{index}" for index in range(2, 9))
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            event_log=log,
            delivery_id_factory=FixedIdFactory(*new_ids),
        )

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual(summary.sent, 14)
        self.assertEqual(api.maximum_pending_active, RUN_SETTINGS.worker_count)
        self.assertEqual(api.maximum_active, RUN_SETTINGS.worker_count)
        for number in pending_numbers + new_numbers:
            self.assertEqual(len(api.threads[number]), 1)
        self.assertEqual(store.maximum_active_writes, 1)
        self.assertEqual(log.maximum_active_writes, 1)

    def test_checkpoint_failure_joins_every_worker_and_stops_all_message_api_calls(self):
        """Catches a durable write crash leaking POSTs or returning before queued jobs join."""
        numbers = tuple(f"010000003{index:02d}" for index in range(1, 8))
        root = make_root(numbers)
        store = FailingCheckpointStore(
            ResultStore.for_root(root).path,
            fail_on_write=2,
        )
        log = RecordingEventLog(root)
        api = OrchestrationApi()
        ids = tuple(f"FFFFFFFFFFFFFFF{index}" for index in range(2, 9))
        WorkerGateCoordinator.barrier = threading.Barrier(RUN_SETTINGS.worker_count)
        WorkerGateCoordinator.main_thread = threading.get_ident()
        with patch("sens_mms.workflow.RunCoordinator", WorkerGateCoordinator):
            workflow = JoinTrackingWorkflow(
                root,
                config(),
                store,
                api,
                ThreadSafeClock(),
                log,
                FixedIdFactory(*ids),
            )

        with self.assertRaisesRegex(OSError, "checkpoint unavailable"):
            workflow.run_live(workflow.current_token())

        self.assertEqual(workflow.joined_workers, 7)
        self.assertEqual(api.sent, [])
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        events = [event for event, _ in log.events]
        self.assertNotIn("RESULT_SNAPSHOT_WRITTEN", events)
        self.assertNotIn("WORKFLOW_COMPLETED", events)

    def test_worker_event_failure_joins_every_worker_and_stops_all_message_api_calls(self):
        """Catches an event checkpoint crash allowing another worker to POST."""
        numbers = tuple(f"010000004{index:02d}" for index in range(1, 8))
        root = make_root(numbers)
        store = ResultStore.for_root(root)
        failure = EventLogError("worker event checkpoint failed")
        log = FailingEventLog(root, {"SEND_ATTEMPT_STARTED": [failure]})
        api = OrchestrationApi()
        ids = tuple(f"GGGGGGGGGGGGGGG{index}" for index in range(2, 9))
        WorkerGateCoordinator.barrier = threading.Barrier(RUN_SETTINGS.worker_count)
        WorkerGateCoordinator.main_thread = threading.get_ident()
        with patch("sens_mms.workflow.RunCoordinator", WorkerGateCoordinator):
            workflow = JoinTrackingWorkflow(
                root,
                config(),
                store,
                api,
                ThreadSafeClock(),
                log,
                FixedIdFactory(*ids),
            )

        with self.assertRaises(EventLogError) as raised:
            workflow.run_live(workflow.current_token())

        self.assertIs(raised.exception, failure)
        self.assertEqual(workflow.joined_workers, 7)
        self.assertEqual(api.sent, [])
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        events = [event for event, _ in log.events]
        self.assertNotIn("RESULT_SNAPSHOT_WRITTEN", events)
        self.assertNotIn("WORKFLOW_COMPLETED", events)

    def test_one_recipient_explicit_delivery_failure_does_not_stop_unrelated_worker(self):
        """Catches recipient-local carrier failure activating the run-global safety stop."""
        failed_number = "01000000501"
        sent_number = "01000000502"
        root = make_root((failed_number, sent_number))
        store = ResultStore.for_root(root)
        log = RecordingEventLog(root)
        api = OrchestrationApi(
            send_outcomes={
                failed_number: ("fail", "fail", "fail"),
                sent_number: ("success",),
            }
        )
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            event_log=log,
            delivery_id_factory=FixedIdFactory(
                "HHHHHHHHHHHHHHH2",
                "HHHHHHHHHHHHHHH3",
            ),
        )

        summary = workflow.run_live(workflow.current_token())

        rows = ResultStore.for_root(root).load()
        self.assertEqual((summary.sent, summary.failed), (1, 1))
        self.assertEqual(rows[failed_number].delivery_status, "FAILED")
        self.assertEqual(rows[sent_number].delivery_status, "SENT")
        self.assertEqual(len([call for call in api.sent if call[0] == failed_number]), 3)
        self.assertEqual(len([call for call in api.sent if call[0] == sent_number]), 1)
        self.assertFalse(workflow.coordinator._stopped.is_set())
        serialized = json.dumps(log.events, ensure_ascii=False)
        self.assertNotIn(failed_number, serialized)
        self.assertNotIn(sent_number, serialized)

    def test_direct_workflow_injection_passes_explicit_seoul_time_to_snapshot_and_log(self):
        """Catches standalone Workflow forwarding a naive clock value across boundaries."""
        root = make_root()
        store = ObservingSnapshotStore(ResultStore.for_root(root).path)
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
        )
        workflow = make_workflow(
            root,
            store,
            api,
            clock=HostSensitiveClock(),
            event_log=log,
        )

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual(str(store.completed_at_seen.tzinfo), "Asia/Seoul")
        self.assertEqual(summary.result_snapshot.name, "result_20260814_030000.csv")
        completed = next(
            fields for event, fields in log.events if event == "WORKFLOW_COMPLETED"
        )
        self.assertEqual(completed["completed_at"], "2026-08-14T03:00:00.000+09:00")

    def test_upload_uses_immutable_bytes_approved_by_final_preflight(self):
        root = make_root()
        image_path = root / "mms_img" / "mms_01_intro.jpg"
        approved = image_path.read_bytes()
        unapproved = b"UNAPPROVED_IMAGE_BYTES_01012345678"
        api = ByteCapturingApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)
        original_preflight = workflow_module.build_preflight
        calls = 0

        def mutate_after_final_preflight(*args, **kwargs):
            nonlocal calls
            report = original_preflight(*args, **kwargs)
            calls += 1
            if calls == 2:
                image_path.write_bytes(unapproved)
            return report

        with patch(
            "sens_mms.workflow.build_preflight",
            side_effect=mutate_after_final_preflight,
        ):
            token = workflow.current_token()
            workflow.run_live(token)

        self.assertEqual(calls, 2)
        self.assertEqual(api.uploaded_bytes[0], ("mms_01_intro.jpg", approved))
        self.assertNotEqual(api.uploaded_bytes[0][1], unapproved)

    def test_real_api_malformed_ids_remain_pending_without_retry_or_durable_marker(self):
        root = make_root()
        send_marker = "SEND_DICT_MARKER_01012345678"
        list_marker = "LIST_ARRAY_MARKER_01012345678"
        transport = RealApiTransport([
            raw_response(200, {"fileId": "attachment-1"}),
            raw_response(200, {"fileId": "attachment-2"}),
            raw_response(202, {
                "statusCode": "202",
                "statusName": "success",
                "requestId": {"secret": send_marker},
                "requestTime": "2026-08-13T10:00:00.000",
            }),
            raw_response(200, {
                "statusCode": "202",
                "statusName": "success",
                "messages": [{
                    "requestId": [list_marker],
                    "messageId": "message-1",
                    "to": RECIPIENT,
                    "requestTime": "2026-08-13 10:00:00",
                    "completeTime": "",
                    "telcoCode": "",
                    "status": "PROCESSING",
                    "statusCode": "",
                    "statusName": "",
                    "statusMessage": "",
                }],
            }),
        ])
        api = SensClient(
            access_key=FAKE_ACCESS_KEY,
            secret_key=FAKE_SECRET_KEY,
            service_id=FAKE_SERVICE_ID,
            from_number=SENDER,
            transport=transport,
            timestamp_ms=lambda: "1700000000000",
        )
        clock = FakeClock()
        log = JsonlEventLog(root / "logs" / "real.jsonl", now=clock.now)
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            clock=clock,
            event_log=log,
        )

        try:
            summary = workflow.run_live(workflow.current_token())
        finally:
            log.close()

        row = ResultStore.for_root(root).load()[RECIPIENT]
        send_posts = [
            call for call in transport.calls
            if call[0] == "POST" and call[1].endswith("/messages")
        ]
        durable = (
            ResultStore.for_root(root).path.read_text(encoding="utf-8")
            + log.path.read_text(encoding="utf-8")
        )
        events = [json.loads(line)["event"] for line in log.path.read_text(
            encoding="utf-8"
        ).splitlines()]
        self.assertEqual((summary.pending, len(send_posts)), (1, 1))
        self.assertEqual(
            (row.delivery_status, row.attempts, row.request_id, row.message_id),
            ("PENDING_CONFIRMATION", 1, "", ""),
        )
        self.assertIn("SEND_AMBIGUOUS_OUTCOME", events)
        self.assertIn("API_LOOKUP_ERROR", events)
        self.assertNotIn("RETRY_SCHEDULED", events)
        self.assertNotIn(send_marker, durable)
        self.assertNotIn(list_marker, durable)

    def test_real_api_malformed_send_status_stays_pending_without_retry_or_marker(self):
        malformed = ("", " ", "abc", "20", "0202", "２０２")
        for status_code in malformed:
            with self.subTest(status_code=repr(status_code)):
                root = make_root()
                malformed_response = raw_response(202, {
                    "statusCode": status_code,
                    "statusName": "fail",
                    "requestId": "request-that-must-not-be-trusted",
                })
                transport = RealApiTransport([
                    raw_response(200, {"fileId": "attachment-1"}),
                    raw_response(200, {"fileId": "attachment-2"}),
                    malformed_response,
                    malformed_response,
                    malformed_response,
                ])
                api = SensClient(
                    access_key=FAKE_ACCESS_KEY,
                    secret_key=FAKE_SECRET_KEY,
                    service_id=FAKE_SERVICE_ID,
                    from_number=SENDER,
                    transport=transport,
                    timestamp_ms=lambda: "1700000000000",
                )
                clock = FakeClock()
                log = JsonlEventLog(
                    root / "logs" / "malformed-status.jsonl", now=clock.now
                )
                workflow = make_workflow(
                    root,
                    ResultStore.for_root(root),
                    api,
                    clock=clock,
                    event_log=log,
                )

                try:
                    summary = workflow.run_live(workflow.current_token())
                finally:
                    log.close()

                row = ResultStore.for_root(root).load()[RECIPIENT]
                send_posts = [
                    call for call in transport.calls
                    if call[0] == "POST" and call[1].endswith("/messages")
                ]
                log_text = log.path.read_text(encoding="utf-8")
                result_text = ResultStore.for_root(root).path.read_text(
                    encoding="utf-8"
                )
                log_rows = [json.loads(line) for line in log_text.splitlines()]
                events = [item["event"] for item in log_rows]
                self.assertEqual((summary.pending, len(send_posts)), (1, 1))
                self.assertEqual(
                    (
                        row.delivery_status,
                        row.attempts,
                        row.request_id,
                        row.message_id,
                    ),
                    ("PENDING_CONFIRMATION", 1, "", ""),
                )
                self.assertIn("SEND_AMBIGUOUS_OUTCOME", events)
                self.assertNotIn("RETRY_SCHEDULED", events)
                self.assertNotIn("request-that-must-not-be-trusted", log_text)
                self.assertNotIn("request-that-must-not-be-trusted", result_text)
                if status_code.strip():
                    durable_api_fields = json.dumps(
                        [
                            {
                                "response": item["response"],
                                "error": item["error"],
                                "state_error": (
                                    item["state"]["error"]
                                    if item["state"] is not None
                                    else None
                                ),
                            }
                            for item in log_rows
                        ],
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    self.assertNotIn(status_code, durable_api_fields)
                    self.assertNotIn(status_code, result_text)

    def test_post_started_approval_failure_writes_workflow_aborted(self):
        root = make_root()
        log = RecordingEventLog(root)
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            ScriptedApi(sends=[], lists=[], gets=[]),
            event_log=log,
        )

        with self.assertRaises(workflow_module.ApprovalTokenMismatch):
            workflow.run_live("wrong-token")

        self.assertEqual(
            [event for event, _ in log.events],
            ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
        )

    def test_abort_log_failure_does_not_replace_post_started_primary_exception(self):
        root = make_root()
        abort_failure = EventLogError("secondary abort failure")
        log = FailingEventLog(root, {"WORKFLOW_ABORTED": [abort_failure]})
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            ScriptedApi(sends=[], lists=[], gets=[]),
            event_log=log,
        )

        with self.assertRaises(workflow_module.ApprovalTokenMismatch) as raised:
            workflow.run_live("wrong-token")

        self.assertEqual(str(raised.exception), "approval token mismatch")
        self.assertEqual(
            log.write_attempts,
            ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
        )

    def test_lock_release_failure_after_started_writes_workflow_aborted(self):
        root = make_root()
        release_failure = ResultFormatError("live execution lock release failed")

        class ReleaseFailingLock:
            def __init__(self, path):
                self.path = path

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc_value, traceback):
                if exc_type is None:
                    raise release_failure
                return False

        log = RecordingEventLog(root)
        store = ResultStore.for_root(root)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="SENT",
            is_sent="true",
            attempts=1,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        workflow = make_workflow(
            root,
            store,
            ScriptedApi(sends=[], lists=[], gets=[]),
            event_log=log,
        )

        with patch("sens_mms.workflow.LiveExecutionLock", ReleaseFailingLock):
            with self.assertRaises(ResultFormatError) as raised:
                workflow.run_live(workflow.current_token())

        self.assertIs(raised.exception, release_failure)
        self.assertEqual(
            [event for event, _ in log.events][-2:],
            ["WORKFLOW_COMPLETED", "WORKFLOW_ABORTED"],
        )

    def test_delivery_id_is_reserved_before_first_post_and_kept_through_success(self):
        root = make_root()
        store = ResultStore.for_root(root)
        observed = []
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
            on_send=lambda: observed.append(
                next(iter(ResultStore.for_root(root).load().values()))
            ),
        )
        log = RecordingEventLog(root)
        workflow = make_workflow(
            root,
            store,
            api,
            event_log=log,
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_1),
        )

        workflow.run_live(workflow.current_token())

        before_post = observed[0]
        final = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual(before_post.delivery_id, DELIVERY_ID_1)
        self.assertEqual(before_post.delivery_status, "PENDING_CONFIRMATION")
        self.assertEqual(before_post.attempts, 1)
        self.assertEqual(final.delivery_id, DELIVERY_ID_1)
        self.assertEqual(final.delivery_status, "SENT")
        states = [
            fields["state"]
            for event, fields in log.events
            if event == "RESULT_STATE_CHANGED"
        ]
        self.assertEqual([states[0]["attempts"], states[1]["attempts"]], [0, 1])

    def test_reservation_bytes_are_written_before_attempt_one_and_post(self):
        root = make_root()
        store = RecordingResultStore(ResultStore.for_root(root).path)
        writes_seen_at_post = []
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
            on_send=lambda: writes_seen_at_post.append(len(store.write_history)),
        )
        workflow = make_workflow(root, store, api)

        workflow.run_live(workflow.current_token())

        def only_attempts(payload):
            rows = list(
                csv.DictReader(io.StringIO(payload.decode("utf-8"), newline=""))
            )
            self.assertEqual(len(rows), 1)
            return int(rows[0]["attempts"])

        self.assertEqual(
            [only_attempts(payload) for payload in store.write_history[:2]],
            [0, 1],
        )
        self.assertEqual(writes_seen_at_post, [2])

    def test_resend_failed_reaches_current_token_and_run_live_preflight(self):
        root = make_root()
        observed_modes = []

        def recording_preflight(root_arg, config_arg, store_arg, *, resend_failed):
            self.assertEqual(root_arg, root)
            self.assertIs(store_arg, store)
            observed_modes.append(resend_failed)
            return SimpleNamespace(
                approval_token="resend-token",
                eligible_numbers=(),
                eligible_rows=(),
                work_items=(),
                content_type="COMM",
                approved_images=(),
            )

        store = ResultStore.for_root(root)
        workflow = make_workflow(
            root,
            store,
            ScriptedApi(sends=[], lists=[], gets=[]),
            resend_failed=True,
        )

        with patch("sens_mms.workflow.build_preflight", side_effect=recording_preflight):
            token = workflow.current_token()
            workflow.run_live(token)

        self.assertEqual(token, "resend-token")
        self.assertEqual(observed_modes, [True, True])

    def test_resend_second_preflight_blocks_started_log_mutation_of_exact_candidate(self):
        mutations = (
            {"delivery_id": DELIVERY_ID_2},
            {"attempts": 2},
            {"error": {"status": "3002", "message": "redacted"}},
        )
        for changed_fields in mutations:
            with self.subTest(changed_fields=changed_fields):
                root = make_root()
                store = seed_resend_rows(
                    root,
                    (failed_result(RECIPIENT, DELIVERY_ID_1, "request-1", "message-1"),),
                )

                def mutate_checkpoint():
                    fresh = ResultStore.for_root(root)
                    row = fresh.load()[RECIPIENT]
                    fresh.upsert(replace(row, **changed_fields))
                    fresh.write_atomic()

                log = MutatingStartedEventLog(root, mutate_checkpoint)
                api = ScriptedApi(
                    sends=[send_response()],
                    lists=[list_response(message("PROCESSING"))],
                    gets=[result_response(message("COMPLETED", "success", "0"))],
                )
                workflow = make_workflow(
                    root,
                    store,
                    api,
                    event_log=log,
                    delivery_id_factory=FixedIdFactory(DELIVERY_ID_3),
                    resend_failed=True,
                )
                token = workflow.current_token()

                with self.assertRaisesRegex(
                    ResultFormatError,
                    "^resend snapshot does not match current checkpoint$",
                ):
                    workflow.run_live(token)

                self.assertEqual(
                    (api.uploaded, api.sent, api.listed_requests, api.got),
                    ([], [], [], []),
                )
                self.assertEqual(
                    [event for event, _ in log.events],
                    ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
                )
                self.assert_event_privacy(log)

    def test_resend_fresh_check_blocks_mutation_after_second_preflight_before_reservation(self):
        root = make_root()
        store = seed_resend_rows(
            root,
            (failed_result(RECIPIENT, DELIVERY_ID_1, "request-1", "message-1"),),
        )
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
        )
        log = RecordingEventLog(root)
        workflow = make_workflow(
            root,
            store,
            api,
            event_log=log,
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_3),
            resend_failed=True,
        )
        original_preflight = workflow_module.build_preflight
        calls = 0

        def mutate_after_validated(*args, **kwargs):
            nonlocal calls
            report = original_preflight(*args, **kwargs)
            calls += 1
            if calls == 2:
                fresh = ResultStore.for_root(root)
                row = fresh.load()[RECIPIENT]
                fresh.upsert(replace(row, attempts=2))
                fresh.write_atomic()
            return report

        with patch(
            "sens_mms.workflow.build_preflight",
            side_effect=mutate_after_validated,
        ):
            token = workflow.current_token()
            with self.assertRaisesRegex(
                ResultFormatError,
                "^approved resend row does not match current checkpoint$",
            ):
                workflow.run_live(token)

        self.assertEqual(calls, 2)
        self.assertEqual(
            (api.uploaded, api.sent, api.listed_requests, api.got),
            (["mms_01_intro.jpg", "mms_02_details.jpg"], [], [], []),
        )
        self.assertEqual(
            [event for event, _ in log.events],
            ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
        )

    def test_resend_factory_mutation_of_each_candidate_identity_field_blocks_without_publish_or_api(self):
        """Catches a candidate changing after ID generation but before reservation publication."""
        mutations = (
            {"delivery_id": DELIVERY_ID_2},
            {"attempts": 2},
            {"error": {"status": "3002", "message": "redacted"}},
        )
        for changed_fields in mutations:
            with self.subTest(changed_fields=changed_fields):
                root = make_root()
                store = seed_resend_rows(
                    root,
                    (failed_result(RECIPIENT, DELIVERY_ID_1, "request-1", "message-1"),),
                )
                mutation_payload = []

                def mutate_checkpoint():
                    fresh = ResultStore.for_root(root)
                    row = fresh.load()[RECIPIENT]
                    fresh.upsert(replace(row, **changed_fields))
                    fresh.write_atomic()
                    mutation_payload.append(fresh.path.read_bytes())

                api = ScriptedApi(
                    sends=[send_response()],
                    lists=[list_response(message("PROCESSING"))],
                    gets=[result_response(message("COMPLETED", "success", "0"))],
                )
                log = RecordingEventLog(root)
                workflow = make_workflow(
                    root,
                    store,
                    api,
                    event_log=log,
                    delivery_id_factory=MutatingIdFactory(
                        mutate_checkpoint,
                        DELIVERY_ID_3,
                    ),
                    resend_failed=True,
                )

                with self.assertRaises(Exception) as raised:
                    workflow.run_live(workflow.current_token())
                self.assertIsInstance(raised.exception, ResultFormatError)
                self.assertEqual(
                    str(raised.exception),
                    "result row does not match expected checkpoint",
                )

                self.assertEqual(store.path.read_bytes(), mutation_payload[0])
                self.assertEqual(
                    (api.uploaded, api.sent, api.listed_requests, api.time_listed, api.got),
                    (["mms_01_intro.jpg", "mms_02_details.jpg"], [], [], [], []),
                )
                self.assertEqual(
                    [event for event, _ in log.events],
                    ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
                )

    def test_resend_factory_noncandidate_mutation_is_preserved_by_atomic_reservation_publish(self):
        """Catches reservation publication overwriting a fresh non-candidate checkpoint row."""
        noncandidate = "01099998888"
        root = make_root((RECIPIENT, noncandidate))
        candidate = failed_result(RECIPIENT, DELIVERY_ID_1, "request-1", "message-1")
        untouched = ResultRow(
            receiving_number=noncandidate,
            delivery_id=DELIVERY_ID_2,
            delivery_status="SENT",
            is_sent="true",
            attempts=1,
            request_id="request-2",
            message_id="message-2",
            error=None,
        )
        store = seed_resend_rows(root, (candidate, untouched))

        def mutate_non_candidate():
            fresh = ResultStore.for_root(root)
            rows = fresh.load()
            fresh.upsert(replace(rows[noncandidate], attempts=2))
            fresh.write_atomic()

        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
        )
        workflow = make_workflow(
            root,
            store,
            api,
            delivery_id_factory=MutatingIdFactory(mutate_non_candidate, DELIVERY_ID_3),
            resend_failed=True,
        )

        workflow.run_live(workflow.current_token())

        final = ResultStore.for_root(root).load()
        self.assertEqual(final[noncandidate], replace(untouched, attempts=2))
        self.assertEqual(final[RECIPIENT].delivery_status, "SENT")

    def test_resend_publishes_all_candidate_reservations_in_one_atomic_write(self):
        """Catches per-candidate writes exposing a partially reserved resend batch."""
        second = "01099998888"
        root = make_root((RECIPIENT, second))
        seeded = seed_resend_rows(
            root,
            (
                failed_result(RECIPIENT, DELIVERY_ID_1, "request-1", "message-1"),
                failed_result(second, DELIVERY_ID_2, "request-2", "message-2"),
            ),
        )
        store = RecordingResultStore(seeded.path)
        store.load()
        api = OrchestrationApi(
            send_outcomes={
                RECIPIENT: ("success",),
                second: ("success",),
            },
        )
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_3, DELIVERY_ID_4),
            resend_failed=True,
        )

        workflow.run_live(workflow.current_token())

        payload = store.write_history[0].decode("utf-8")
        rows = list(csv.DictReader(io.StringIO(payload, newline="")))
        self.assertEqual(len(rows), 2)
        self.assertEqual(
            {(row["delivery_id"], row["delivery_status"], row["attempts"]) for row in rows},
            {
                (DELIVERY_ID_3, "PENDING_CONFIRMATION", "0"),
                (DELIVERY_ID_4, "PENDING_CONFIRMATION", "0"),
            },
        )

    def test_live_execution_lock_rejects_a_second_workflow_before_started_log_or_api(self):
        """Catches two cooperating workflows passing live preflight concurrently."""
        root = make_root()
        first_store = ResultStore.for_root(root)
        second_store = ResultStore.for_root(root)
        entered = threading.Event()
        release = threading.Event()
        first_api = BlockingUploadApi(
            entered=entered,
            release=release,
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
        )
        second_api = ScriptedApi(
            sends=[send_response("request-second")],
            lists=[list_response(message(
                "PROCESSING",
                request_id="request-second",
                message_id="message-second",
            ))],
            gets=[result_response(message(
                "COMPLETED",
                "success",
                "0",
                request_id="request-second",
                message_id="message-second",
            ))],
        )
        first_log = RecordingEventLog(root)
        second_log = RecordingEventLog(root)
        first = make_workflow(root, first_store, first_api, event_log=first_log)
        second = make_workflow(
            root,
            second_store,
            second_api,
            event_log=second_log,
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_2),
        )
        first_token = first.current_token()
        second_token = second.current_token()
        failures = []

        thread = threading.Thread(
            target=lambda: self._capture_workflow_failure(first, first_token, failures),
            daemon=True,
        )
        thread.start()
        try:
            self.assertTrue(entered.wait(5), "first workflow did not reach upload")
            with self.assertRaises(Exception) as raised:
                second.run_live(second_token)
            self.assertIsInstance(raised.exception, ResultFormatError)
            self.assertEqual(str(raised.exception), "live execution already in progress")
            self.assertEqual(second_log.events, [])
            self.assertEqual(
                (second_api.uploaded, second_api.sent, second_api.listed_requests, second_api.got),
                ([], [], [], []),
            )
        finally:
            release.set()
            thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(failures, [])

    @staticmethod
    def _capture_workflow_failure(workflow, token, failures):
        try:
            workflow.run_live(token)
        except BaseException as exc:
            failures.append(exc)

    def test_resend_upload_precedes_atomic_candidate_reservations(self):
        second = "01099998888"
        root = make_root((RECIPIENT, second))
        store = seed_resend_rows(
            root,
            (
                failed_result(RECIPIENT, DELIVERY_ID_1, "request-1", "message-1"),
                failed_result(second, DELIVERY_ID_2, "request-2", "message-2"),
            ),
        )
        rows_seen_at_upload = []

        def observe_checkpoint(_upload_number):
            rows_seen_at_upload.append(ResultStore.for_root(root).load())

        api = OrchestrationApi(
            send_outcomes={
                RECIPIENT: ("success",),
                second: ("success",),
            },
            on_upload=observe_checkpoint,
        )
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_3, DELIVERY_ID_4),
            resend_failed=True,
        )

        workflow.run_live(workflow.current_token())

        rows_at_first_upload = rows_seen_at_upload[0]
        self.assertEqual(set(rows_at_first_upload), {RECIPIENT, second})
        self.assertEqual(
            {
                (
                    row.delivery_status,
                    row.attempts,
                    row.request_id,
                    row.message_id,
                )
                for row in rows_at_first_upload.values()
            },
            {
                ("FAILED", 3, "request-1", "message-1"),
                ("FAILED", 3, "request-2", "message-2"),
            },
        )
        self.assertEqual(
            {row.delivery_id for row in rows_at_first_upload.values()},
            {DELIVERY_ID_1, DELIVERY_ID_2},
        )
        final_rows = ResultStore.for_root(root).load()
        self.assertEqual(
            {row.delivery_id for row in final_rows.values()},
            {DELIVERY_ID_3, DELIVERY_ID_4},
        )

    def test_success_path_logs_send_list_every_poll_state_and_completion(self):
        root = make_root()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[
                result_response(message("PROCESSING")),
                result_response(message("COMPLETED", "success", "0")),
            ],
        )
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            event_log=log,
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_1),
        )

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual(
            [event for event, _ in log.events],
            [
                "WORKFLOW_STARTED",
                "DELIVERY_ID_ASSIGNED",
                "RESULT_STATE_CHANGED",
                "RESULT_STATE_CHANGED",
                "SEND_ATTEMPT_STARTED",
                "RESULT_STATE_CHANGED",
                "SEND_RESPONSE",
                "RESULT_STATE_CHANGED",
                "MESSAGE_LIST_RESPONSE",
                "DELIVERY_POLL_RESPONSE",
                "RESULT_STATE_CHANGED",
                "DELIVERY_POLL_RESPONSE",
                "RESULT_SNAPSHOT_WRITTEN",
                "WORKFLOW_COMPLETED",
            ],
        )
        responses = [
            (event, fields)
            for event, fields in log.events
            if event
            in {"SEND_RESPONSE", "MESSAGE_LIST_RESPONSE", "DELIVERY_POLL_RESPONSE"}
        ]
        self.assertEqual(
            [event for event, _ in responses],
            [
                "SEND_RESPONSE",
                "MESSAGE_LIST_RESPONSE",
                "DELIVERY_POLL_RESPONSE",
                "DELIVERY_POLL_RESPONSE",
            ],
        )
        serialized = json.dumps(responses, ensure_ascii=False)
        self.assertNotIn(RECIPIENT, serialized)
        self.assertNotIn("0212345678", serialized)
        self.assertNotIn("file-1", serialized)
        send_fields = responses[0][1]
        self.assertEqual(
            (send_fields["api"], send_fields["http_status"], send_fields["request_id"]),
            ("send", 202, "request-1"),
        )
        completed = log.events[-1][1]
        self.assertEqual(
            completed["summary"],
            {"total": 1, "sent": 1, "failed": 0, "pending_confirmation": 0},
        )
        self.assertEqual(completed["result_snapshot"], str(summary.result_snapshot.resolve()))
        self.assertIn("+09:00", completed["completed_at"])

    def test_snapshot_counts_and_summary_paths_come_from_immutable_snapshot(self):
        root = make_root()
        store = SnapshotMutatingStore(ResultStore.for_root(root).path)
        log = RecordingEventLog(root)
        workflow = make_workflow(
            root,
            store,
            ScriptedApi(
                sends=[send_response()],
                lists=[list_response(message("PROCESSING"))],
                gets=[result_response(message("COMPLETED", "success", "0"))],
            ),
            event_log=log,
        )

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual((summary.total, summary.sent, summary.failed, summary.pending), (1, 1, 0, 0))
        self.assertEqual(summary.event_log, log.path)
        self.assertTrue(summary.result_snapshot.is_file())
        self.assertEqual(len(ResultStore(summary.result_snapshot).load()), 1)
        self.assertEqual(len(ResultStore.for_root(root).load()), 2)
        snapshot_event = log.events[-2]
        self.assertEqual(snapshot_event[0], "RESULT_SNAPSHOT_WRITTEN")
        self.assertEqual(snapshot_event[1]["result_snapshot"], str(summary.result_snapshot.resolve()))

    def test_snapshot_failure_does_not_log_workflow_completed(self):
        class FailingSnapshotStore(ResultStore):
            def snapshot(self, completed_at):
                raise OSError("snapshot failed")

        root = make_root()
        store = FailingSnapshotStore(ResultStore.for_root(root).path)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="SENT",
            is_sent="true",
            attempts=1,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        log = RecordingEventLog(root)
        workflow = make_workflow(
            root, store, ScriptedApi(sends=[], lists=[], gets=[]), event_log=log
        )

        with self.assertRaises(OSError):
            workflow.run_live(workflow.current_token())

        self.assertEqual(
            [event for event, _ in log.events],
            ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
        )

    def test_accepted_processing_completed_success_becomes_sent(self):
        root = make_root()
        store = ResultStore.for_root(root)
        clock = FakeClock()
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[
                result_response(message("PROCESSING")),
                result_response(message("COMPLETED", "success", "0")),
            ],
        )
        workflow = make_workflow(root, store, api, clock)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.sent, summary.failed, summary.pending), (1, 0, 0))
        self.assertEqual((row.delivery_status, row.is_sent, row.attempts), ("SENT", "true", 1))
        self.assertEqual((row.request_id, row.message_id, row.error), ("request-1", "message-1", None))
        self.assertEqual(api.uploaded, ["mms_01_intro.jpg", "mms_02_details.jpg"])
        self.assertEqual(api.sent, [(RECIPIENT, ("file-1", "file-2"))])
        self.assertEqual(clock.sleeps, [1])

    def test_explicit_post_failure_retries_after_ten_seconds_then_succeeds(self):
        root = make_root()
        store = ResultStore.for_root(root)
        clock = FakeClock()
        api = ScriptedApi(
            sends=[ExplicitApiFailure("400", "rejected"), send_response("request-2")],
            lists=[list_response(message(
                "COMPLETED", "success", "0",
                request_id="request-2", message_id="message-2",
            ))],
            gets=[result_response(message(
                "COMPLETED", "success", "0",
                request_id="request-2", message_id="message-2",
            ))],
        )
        workflow = make_workflow(root, store, api, clock)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.sent, row.attempts), (1, 2))
        self.assertEqual(len(api.sent), 2)
        self.assertEqual(clock.sleeps, [10.0])

    def test_three_explicit_post_failures_become_failed(self):
        root = make_root()
        clock = FakeClock()
        api = ScriptedApi(
            sends=[
                ExplicitApiFailure("400", "first"),
                ExplicitApiFailure("429", "second"),
                ExplicitApiFailure("500", "third"),
            ],
            lists=[],
            gets=[],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api, clock)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.failed, row.delivery_status, row.is_sent), (1, "FAILED", "false"))
        self.assertEqual(row.attempts, 3)
        self.assertEqual(row.error, {"status": "500", "message": "redacted"})
        self.assertEqual(clock.sleeps, [10.0, 10.0])

    def test_final_external_error_masks_phone_numbers_before_persistence(self):
        root = make_root()
        api = ScriptedApi(
            sends=[
                ExplicitApiFailure("400", "first"),
                ExplicitApiFailure("400", "second"),
                ExplicitApiFailure("400", f"recipient {RECIPIENT} rejected"),
            ],
            lists=[],
            gets=[],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        workflow.run_live(workflow.current_token())

        error = ResultStore.for_root(root).load()[RECIPIENT].error
        self.assertNotIn(RECIPIENT, error["message"])
        self.assertEqual(error["message"], "redacted")

    def test_completed_failure_retries_and_then_succeeds(self):
        root = make_root()
        clock = FakeClock()
        api = ScriptedApi(
            sends=[send_response(), send_response("request-2")],
            lists=[
                list_response(message("PROCESSING")),
                list_response(message(
                    "PROCESSING", request_id="request-2", message_id="message-2"
                )),
            ],
            gets=[
                result_response(message("COMPLETED", "fail", "3001", "subscriber missing")),
                result_response(message(
                    "COMPLETED", "success", "0",
                    request_id="request-2", message_id="message-2",
                )),
            ],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api, clock)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.sent, row.attempts), (1, 2))
        self.assertEqual(clock.sleeps, [10.0])

    def test_processing_for_two_minutes_stays_pending_then_restart_reconciles_success(self):
        root = make_root()
        first_clock = FakeClock()
        first_api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("PROCESSING")) for _ in range(120)],
        )
        first = make_workflow(root, ResultStore.for_root(root), first_api, first_clock)

        first_summary = first.run_live(first.current_token())

        pending = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((first_summary.pending, pending.delivery_status, pending.is_sent), (1, "PENDING_CONFIRMATION", ""))
        self.assertEqual(first_clock.elapsed, 120)
        self.assertEqual(len(first_api.sent), 1)

        second_api = ScriptedApi(
            sends=[], lists=[], gets=[result_response(message("COMPLETED", "success", "0"))]
        )
        second = make_workflow(root, ResultStore.for_root(root), second_api)

        second_summary = second.run_live(second.current_token())

        final = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((second_summary.sent, final.delivery_status), (1, "SENT"))
        self.assertEqual(second_api.sent, [])
        self.assertEqual(second_api.uploaded, [])

    def test_lost_post_response_without_unique_lookup_is_pending_and_never_retried(self):
        root = make_root()
        api = ScriptedApi(
            sends=[AmbiguousPostOutcome("response lost")],
            lists=[],
            gets=[],
            time_lists=[list_response()],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.pending, row.delivery_status, row.request_id, row.message_id), (1, "PENDING_CONFIRMATION", "", ""))
        self.assertEqual(row.attempts, 1)
        self.assertEqual(len(api.sent), 1)

    def test_transient_result_lookup_error_is_pending_without_retry(self):
        root = make_root()
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[TransientLookupError("temporary")] * 120,
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.pending, row.delivery_status), (1, "PENDING_CONFIRMATION"))
        self.assertEqual(len(api.sent), 1)

    def test_two_recipients_receive_distinct_ids_and_share_one_ordered_file_pair(self):
        second = "01099998888"
        root = make_root((RECIPIENT, second))
        api = OrchestrationApi(
            send_outcomes={
                RECIPIENT: ("success",),
                second: ("success",),
            },
        )
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            clock=ThreadSafeClock(),
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_1, DELIVERY_ID_2),
        )

        summary = workflow.run_live(workflow.current_token())

        rows = ResultStore.for_root(root).load()
        self.assertEqual(summary.sent, 2)
        self.assertEqual(
            {rows[RECIPIENT].delivery_id, rows[second].delivery_id},
            {DELIVERY_ID_1, DELIVERY_ID_2},
        )
        self.assertEqual(
            [name for name, _data in api.uploaded],
            ["mms_01_intro.jpg", "mms_02_details.jpg"],
        )
        self.assertEqual(
            {(number, files) for number, files, _content_type in api.sent},
            {
                (RECIPIENT, ("file-1", "file-2")),
                (second, ("file-1", "file-2")),
            },
        )

    def test_three_completed_failures_become_failed_with_last_delivery_error(self):
        root = make_root()
        clock = FakeClock()
        api = ScriptedApi(
            sends=[send_response(), send_response("request-2"), send_response("request-3")],
            lists=[
                list_response(message("PROCESSING")),
                list_response(message("PROCESSING", request_id="request-2", message_id="message-2")),
                list_response(message("PROCESSING", request_id="request-3", message_id="message-3")),
            ],
            gets=[
                result_response(message("COMPLETED", "fail", "3001", "first")),
                result_response(message(
                    "COMPLETED", "fail", "3002", "second",
                    request_id="request-2", message_id="message-2",
                )),
                result_response(message(
                    "COMPLETED", "fail", "3003", "third",
                    request_id="request-3", message_id="message-3",
                )),
            ],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api, clock)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.failed, row.attempts), (1, 3))
        self.assertEqual(row.error, {"status": "3003", "message": "redacted"})
        self.assertEqual(clock.sleeps, [10.0, 10.0])

    def test_third_completed_failure_is_checkpointed_before_final_poll_event(self):
        root = make_root()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[send_response(), send_response("request-2"), send_response("request-3")],
            lists=[
                list_response(message("PROCESSING")),
                list_response(message(
                    "PROCESSING", request_id="request-2", message_id="message-2"
                )),
                list_response(message(
                    "PROCESSING", request_id="request-3", message_id="message-3"
                )),
            ],
            gets=[
                result_response(message("COMPLETED", "fail", "3001", "first")),
                result_response(message(
                    "COMPLETED", "fail", "3002", "second",
                    request_id="request-2", message_id="message-2",
                )),
                result_response(message(
                    "COMPLETED", "fail", "3003", f"recipient {RECIPIENT} failed",
                    request_id="request-3", message_id="message-3",
                )),
            ],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        failed_states = [
            (index, fields)
            for index, (event, fields) in enumerate(log.events)
            if event == "RESULT_STATE_CHANGED"
            and fields["state"]["delivery_status"] == "FAILED"
        ]
        poll_indexes = [
            index
            for index, (event, _) in enumerate(log.events)
            if event == "DELIVERY_POLL_RESPONSE"
        ]
        final_poll_index = poll_indexes[-1]
        self.assertEqual(len(failed_states), 1)
        self.assertEqual(failed_states[0][0], final_poll_index - 1)
        self.assertEqual(log.events[final_poll_index - 1][0], "RESULT_STATE_CHANGED")
        failed_fields = failed_states[0][1]
        self.assertEqual(failed_fields["delivery_id"], DELIVERY_ID_1)
        self.assertEqual(failed_fields["state"]["error"]["status"], "3003")
        self.assertNotIn(RECIPIENT, failed_fields["state"]["error"]["message"])

    def test_existing_attempt_three_failure_is_checkpointed_before_poll_event_once(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=3,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[],
            lists=[],
            gets=[result_response(message(
                "COMPLETED", "fail", "3999", f"recipient {RECIPIENT} failed"
            ))],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        failed_states = [
            (index, fields)
            for index, (event, fields) in enumerate(log.events)
            if event == "RESULT_STATE_CHANGED"
            and fields["state"]["delivery_status"] == "FAILED"
        ]
        poll_indexes = [
            index
            for index, (event, _) in enumerate(log.events)
            if event == "DELIVERY_POLL_RESPONSE"
        ]
        self.assertEqual(len(failed_states), 1)
        self.assertEqual(len(poll_indexes), 1)
        self.assertEqual(failed_states[0][0], poll_indexes[0] - 1)
        self.assertEqual(failed_states[0][1]["delivery_id"], DELIVERY_ID_1)
        self.assertEqual(
            failed_states[0][1]["state"]["error"]["status"], "3999"
        )
        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((row.delivery_status, row.attempts), ("FAILED", 3))
        self.assertEqual((api.uploaded, api.sent), ([], []))

    def test_ambiguous_post_uniquely_found_by_time_and_recipient_is_reconciled(self):
        root = make_root()
        api = ScriptedApi(
            sends=[AmbiguousPostOutcome("response lost")],
            lists=[],
            gets=[result_response(message("COMPLETED", "success", "0"))],
            time_lists=[list_response(message("PROCESSING"))],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.sent, row.request_id, row.message_id), (1, "request-1", "message-1"))
        self.assertEqual(len(api.sent), 1)

    def test_existing_sent_recipient_is_never_uploaded_or_sent_again(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="SENT",
            is_sent="true",
            attempts=1,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        api = ScriptedApi(sends=[], lists=[], gets=[])
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual(summary.sent, 1)
        self.assertEqual((api.uploaded, api.sent), ([], []))

    def test_existing_pending_lookup_error_is_not_uploaded_or_resent(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=1,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        api = ScriptedApi(
            sends=[],
            lists=[],
            gets=[TransientLookupError("temporary")] * 120,
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual(summary.pending, 1)
        self.assertEqual((api.uploaded, api.sent), ([], []))

    def test_existing_pending_completed_failure_uses_only_remaining_attempts(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=1,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        clock = FakeClock()
        api = ScriptedApi(
            sends=[send_response("request-2")],
            lists=[list_response(message(
                "PROCESSING", request_id="request-2", message_id="message-2"
            ))],
            gets=[
                result_response(message("COMPLETED", "fail", "3001", "first failed")),
                result_response(message(
                    "COMPLETED", "success", "0",
                    request_id="request-2", message_id="message-2",
                )),
            ],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api, clock)

        summary = workflow.run_live(workflow.current_token())

        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.sent, row.attempts), (1, 2))
        self.assertEqual(row.delivery_id, DELIVERY_ID_1)
        self.assertEqual(len(api.sent), 1)
        self.assertEqual(clock.sleeps, [10.0])

    def test_explicit_failure_then_success_logs_safe_response_and_retry_before_sleep(self):
        root = make_root()
        log = RecordingEventLog(root)
        sleep_observations = []
        clock = FakeClock(
            on_sleep=lambda seconds: sleep_observations.append(
                (seconds, log.events[-1])
            )
        )
        api = ScriptedApi(
            sends=[
                ExplicitApiFailure(
                    "429",
                    f"recipient {RECIPIENT} rejected",
                    http_status=429,
                    response={"unsafe": RAW_URL},
                ),
                send_response("request-2"),
            ],
            lists=[list_response(message(
                "PROCESSING", request_id="request-2", message_id="message-2"
            ))],
            gets=[result_response(message(
                "COMPLETED", "success", "0",
                request_id="request-2", message_id="message-2",
            ))],
        )
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            clock,
            log,
            FixedIdFactory(DELIVERY_ID_1),
        )

        workflow.run_live(workflow.current_token())

        attempts = [
            fields for event, fields in log.events
            if event == "SEND_ATTEMPT_STARTED"
        ]
        retries = [
            fields for event, fields in log.events if event == "RETRY_SCHEDULED"
        ]
        failures = [
            fields for event, fields in log.events
            if event == "SEND_RESPONSE" and fields.get("error") is not None
        ]
        self.assertEqual([item["attempt"] for item in attempts], [1, 2])
        self.assertEqual({item["delivery_id"] for item in attempts}, {DELIVERY_ID_1})
        self.assertEqual(retries, [{"delivery_id": DELIVERY_ID_1, "attempt": 2}])
        self.assertEqual(
            sleep_observations,
            [(10.0, ("RETRY_SCHEDULED", retries[0]))],
        )
        self.assertEqual(len(failures), 1)
        self.assertEqual(
            failures[0],
            {
                "delivery_id": DELIVERY_ID_1,
                "attempt": 1,
                "api": "send",
                "http_status": 429,
                "response": None,
                "error": {
                    "status": "429",
                    "message": "redacted",
                },
            },
        )
        self.assert_event_privacy(log)

    def test_three_explicit_failures_checkpoint_final_failed_before_third_response(self):
        root = make_root()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[
                ExplicitApiFailure("400", "first", http_status=400),
                ExplicitApiFailure("429", "second", http_status=429),
                ExplicitApiFailure("500", "third", http_status=500),
            ],
            lists=[],
            gets=[],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        failure_responses = [
            index for index, (event, fields) in enumerate(log.events)
            if event == "SEND_RESPONSE" and fields.get("error") is not None
        ]
        failed_states = [
            index for index, (event, fields) in enumerate(log.events)
            if event == "RESULT_STATE_CHANGED"
            and fields["state"]["delivery_status"] == "FAILED"
        ]
        retries = [
            fields for event, fields in log.events if event == "RETRY_SCHEDULED"
        ]
        self.assertEqual(len(failure_responses), 3)
        self.assertEqual(len(failed_states), 1)
        self.assertEqual(failed_states[0], failure_responses[2] - 1)
        self.assertEqual([item["attempt"] for item in retries], [2, 3])
        self.assertEqual({item["delivery_id"] for item in retries}, {DELIVERY_ID_1})
        self.assert_event_privacy(log)

    def test_three_completed_failures_schedule_two_retries_with_stable_id(self):
        root = make_root()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[send_response(), send_response("request-2"), send_response("request-3")],
            lists=[
                list_response(message("PROCESSING")),
                list_response(message(
                    "PROCESSING", request_id="request-2", message_id="message-2"
                )),
                list_response(message(
                    "PROCESSING", request_id="request-3", message_id="message-3"
                )),
            ],
            gets=[
                result_response(message("COMPLETED", "fail", "3001", "first")),
                result_response(message(
                    "COMPLETED", "fail", "3002", "second",
                    request_id="request-2", message_id="message-2",
                )),
                result_response(message(
                    "COMPLETED", "fail", "3003", "third",
                    request_id="request-3", message_id="message-3",
                )),
            ],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        retries = [
            fields for event, fields in log.events if event == "RETRY_SCHEDULED"
        ]
        attempts = [
            fields for event, fields in log.events
            if event == "SEND_ATTEMPT_STARTED"
        ]
        polls = [
            index for index, (event, _) in enumerate(log.events)
            if event == "DELIVERY_POLL_RESPONSE"
        ]
        failed_states = [
            index for index, (event, fields) in enumerate(log.events)
            if event == "RESULT_STATE_CHANGED"
            and fields["state"]["delivery_status"] == "FAILED"
        ]
        self.assertEqual([item["attempt"] for item in retries], [2, 3])
        self.assertEqual({item["delivery_id"] for item in retries}, {DELIVERY_ID_1})
        self.assertEqual({item["delivery_id"] for item in attempts}, {DELIVERY_ID_1})
        self.assertEqual(failed_states, [polls[-1] - 1])
        self.assert_event_privacy(log)

    def test_one_hundred_twenty_processing_polls_are_logged_over_two_minutes(self):
        root = make_root()
        log = RecordingEventLog(root)
        clock = FakeClock()
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("PROCESSING")) for _ in range(120)],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, clock, log
        )

        summary = workflow.run_live(workflow.current_token())

        polls = [
            fields for event, fields in log.events
            if event == "DELIVERY_POLL_RESPONSE"
        ]
        self.assertEqual(len(polls), 120)
        self.assertTrue(all(
            item["response"]["message"]["status"] == "PROCESSING"
            for item in polls
        ))
        self.assertEqual(len(api.got), 120)
        self.assertEqual(api.gets, [])
        self.assertEqual((clock.elapsed, len(api.sent), summary.pending), (120, 1, 1))
        self.assert_event_privacy(log)

    def test_request_without_message_id_logs_all_two_minute_list_responses(self):
        root = make_root()
        log = RecordingEventLog(root)
        clock = FakeClock()
        unresolved = message("PROCESSING", message_id="")
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(unresolved) for _ in range(120)],
            gets=[],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, clock, log
        )

        summary = workflow.run_live(workflow.current_token())

        lists = [
            fields for event, fields in log.events
            if event == "MESSAGE_LIST_RESPONSE"
        ]
        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual(len(lists), 120)
        self.assertEqual(len(api.listed_requests), 120)
        self.assertEqual((clock.elapsed, summary.pending), (120, 1))
        self.assertEqual((row.request_id, row.message_id, len(api.sent)), ("request-1", "", 1))
        self.assert_event_privacy(log)

    def test_ambiguous_post_unique_recovery_logs_fixed_error_and_list_once(self):
        root = make_root()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[AmbiguousPostOutcome(f"lost {RAW_URL}")],
            lists=[],
            gets=[result_response(message("COMPLETED", "success", "0"))],
            time_lists=[list_response(message("PROCESSING"))],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        ambiguous_indexes = [
            index for index, (event, _) in enumerate(log.events)
            if event == "SEND_AMBIGUOUS_OUTCOME"
        ]
        self.assertEqual(len(ambiguous_indexes), 1)
        ambiguous_index = ambiguous_indexes[0]
        list_index = next(
            index for index, (event, _) in enumerate(log.events)
            if event == "MESSAGE_LIST_RESPONSE"
        )
        ambiguous_fields = log.events[ambiguous_index][1]
        self.assertEqual(
            ambiguous_fields["error"],
            {
                "status": "AMBIGUOUS_POST_OUTCOME",
                "message": "redacted",
            },
        )
        self.assertLess(ambiguous_index, list_index)
        self.assertEqual(
            log.events[list_index - 1][1]["state"]["message_id"],
            "message-1",
        )
        self.assertEqual((len(api.sent), len(api.time_listed)), (1, 1))
        self.assert_event_privacy(log)

    def test_ambiguous_post_multiple_matches_logs_list_once_and_never_resends(self):
        root = make_root()
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[AmbiguousPostOutcome("connection closed")],
            lists=[],
            gets=[],
            time_lists=[list_response(
                message("PROCESSING"),
                message(
                    "PROCESSING",
                    request_id="request-2",
                    message_id="message-2",
                ),
            )],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        summary = workflow.run_live(workflow.current_token())

        response_events = [
            event for event, _ in log.events
            if event in {"SEND_AMBIGUOUS_OUTCOME", "MESSAGE_LIST_RESPONSE"}
        ]
        row = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual(
            response_events,
            ["SEND_AMBIGUOUS_OUTCOME", "MESSAGE_LIST_RESPONSE"],
        )
        self.assertEqual((summary.pending, len(api.sent)), (1, 1))
        self.assertEqual((row.request_id, row.message_id), ("", ""))
        self.assert_event_privacy(log)

    def test_transient_request_list_errors_remain_safe_through_timeout(self):
        root = make_root()
        log = RecordingEventLog(root)
        failure = TransientLookupError(RAW_URL)
        failure.message = f"temporary lookup for {RECIPIENT}"
        failure.__cause__ = RuntimeError("REQUEST_LIST_CAUSE_SECRET")
        api = ScriptedApi(
            sends=[send_response()],
            lists=[failure] * 120,
            gets=[],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        summary = workflow.run_live(workflow.current_token())

        errors = [fields for event, fields in log.events if event == "API_LOOKUP_ERROR"]
        self.assertEqual(len(errors), 120)
        self.assertEqual(errors[0]["api"], "list")
        self.assertEqual(
            errors[0]["error"],
            {
                "status": "TRANSIENT_LOOKUP_ERROR",
                "message": "redacted",
            },
        )
        self.assertEqual((summary.pending, len(api.sent)), (1, 1))
        self.assert_event_privacy(log, "REQUEST_LIST_CAUSE_SECRET")

    def test_transient_time_list_error_logs_one_safe_lookup_error(self):
        root = make_root()
        log = RecordingEventLog(root)
        failure = TransientLookupError(RAW_URL)
        failure.message = f"temporary recovery for {RECIPIENT}"
        failure.__cause__ = RuntimeError("TIME_LIST_CAUSE_SECRET")
        api = ScriptedApi(
            sends=[AmbiguousPostOutcome("response lost")],
            lists=[],
            gets=[],
            time_lists=[failure],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        summary = workflow.run_live(workflow.current_token())

        events = [event for event, _ in log.events]
        errors = [fields for event, fields in log.events if event == "API_LOOKUP_ERROR"]
        self.assertIn("SEND_AMBIGUOUS_OUTCOME", events)
        self.assertIn("API_LOOKUP_ERROR", events)
        self.assertLess(
            events.index("SEND_AMBIGUOUS_OUTCOME"),
            events.index("API_LOOKUP_ERROR"),
        )
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0]["api"], "list")
        self.assertEqual(
            errors[0]["error"]["message"],
            "redacted",
        )
        self.assertEqual((summary.pending, len(api.sent)), (1, 1))
        self.assert_event_privacy(log, "TIME_LIST_CAUSE_SECRET")

    def test_transient_get_error_without_message_attr_uses_fixed_generic_error(self):
        root = make_root()
        log = RecordingEventLog(root)
        failure = TransientLookupError(RAW_URL)
        failure.__cause__ = RuntimeError("GET_CAUSE_SECRET")
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[failure] * 120,
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        summary = workflow.run_live(workflow.current_token())

        errors = [fields for event, fields in log.events if event == "API_LOOKUP_ERROR"]
        self.assertEqual(len(errors), 120)
        self.assertEqual(errors[0]["api"], "get")
        self.assertEqual(
            errors[0]["error"],
            {
                "status": "TRANSIENT_LOOKUP_ERROR",
                "message": "redacted",
            },
        )
        self.assertEqual((summary.pending, len(api.sent)), (1, 1))
        self.assert_event_privacy(log, "GET_CAUSE_SECRET")

    def test_query_like_lookup_message_is_replaced_with_fixed_generic_error(self):
        root = make_root()
        log = RecordingEventLog(root)
        failure = TransientLookupError("temporary")
        unsafe_query = f"recipient={RECIPIENT}&authorization=FAKE_HEADER_SECRET"
        failure.message = unsafe_query
        api = ScriptedApi(
            sends=[send_response()],
            lists=[failure] * 120,
            gets=[],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        workflow.run_live(workflow.current_token())

        lookup = next(
            fields for event, fields in log.events if event == "API_LOOKUP_ERROR"
        )
        self.assertEqual(
            lookup["error"],
            {
                "status": "TRANSIENT_LOOKUP_ERROR",
                "message": "redacted",
            },
        )
        self.assert_event_privacy(log, unsafe_query, "FAKE_HEADER_SECRET")

    def test_formatted_phone_lookup_message_is_never_inspected_or_logged(self):
        self.assert_arbitrary_lookup_message_discarded(
            "temporary lookup for 010-1234-5678"
        )

    def test_name_lookup_message_is_never_inspected_or_logged(self):
        self.assert_arbitrary_lookup_message_discarded(
            "temporary lookup for Alice"
        )

    def test_opaque_bearer_lookup_message_is_never_inspected_or_logged(self):
        self.assert_arbitrary_lookup_message_discarded(
            "temporary lookup bearer ZYXWVUTSRQP"
        )

    def test_legacy_pending_blank_id_is_persisted_before_get_observation(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(ResultRow(
            receiving_number=RECIPIENT,
            delivery_id="",
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=1,
            request_id="request-1",
            message_id="message-1",
            error=None,
        ))
        store.write_atomic()
        observed = []
        log = RecordingEventLog(root)
        api = ScriptedApi(
            sends=[],
            lists=[],
            gets=[result_response(message("COMPLETED", "success", "0"))],
            on_get=lambda: observed.append(
                ResultStore.for_root(root).load()[RECIPIENT]
            ),
        )
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            event_log=log,
            delivery_id_factory=FixedIdFactory(DELIVERY_ID_1),
        )

        workflow.run_live(workflow.current_token())

        events = [event for event, _ in log.events]
        self.assertEqual(observed[0].delivery_id, DELIVERY_ID_1)
        self.assertLess(events.index("DELIVERY_ID_ASSIGNED"), events.index("RESULT_STATE_CHANGED"))
        self.assertLess(events.index("RESULT_STATE_CHANGED"), events.index("DELIVERY_POLL_RESPONSE"))
        self.assertEqual((api.sent, api.uploaded), ([], []))
        self.assert_event_privacy(log)

    def test_attempt_zero_pending_reservation_resumes_with_same_delivery_id(self):
        """Catches a durable pre-POST reservation being held instead of resumed."""
        root = make_root()
        store = ResultStore.for_root(root)
        reservation = ResultRow(
            receiving_number=RECIPIENT,
            delivery_id=DELIVERY_ID_1,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=0,
            request_id="",
            message_id="",
            error=None,
        )
        store.upsert(reservation)
        store.write_atomic()
        api = OrchestrationApi()
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            clock=ThreadSafeClock(),
            delivery_id_factory=FixedIdFactory(),
        )

        summary = workflow.run_live(workflow.current_token())

        final = ResultStore.for_root(root).load()[RECIPIENT]
        self.assertEqual((summary.sent, final.delivery_status), (1, "SENT"))
        self.assertEqual(final.delivery_id, DELIVERY_ID_1)
        self.assertEqual(len(api.uploaded), 2)
        self.assertEqual(len(api.sent), 1)

    def test_workflow_started_log_failure_makes_zero_api_calls_and_no_snapshot(self):
        root = make_root()
        original = EventLogError("event log write failed")
        log = FailingEventLog(root, {"WORKFLOW_STARTED": [original]})
        api = ScriptedApi(sends=[], lists=[], gets=[])
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )
        token = workflow.current_token()

        with self.assertRaises(EventLogError) as raised:
            workflow.run_live(token)

        self.assertIs(raised.exception, original)
        self.assertEqual(
            (api.uploaded, api.sent, api.listed_requests, api.time_listed, api.got),
            ([], [], [], [], []),
        )
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        self.assertEqual(log.write_attempts, ["WORKFLOW_STARTED"])

    def test_send_response_log_failure_preserves_requests_and_aborts_without_snapshot(self):
        second = "01099998888"
        root = make_root((RECIPIENT, second))
        original = EventLogError("event log write failed")
        log = FailingEventLog(root, {"SEND_RESPONSE": [original]})
        api = OrchestrationApi()
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            clock=ThreadSafeClock(),
            event_log=log,
        )

        with self.assertRaises(EventLogError) as raised:
            workflow.run_live(workflow.current_token())

        rows = ResultStore.for_root(root).load()
        self.assertIs(raised.exception, original)
        self.assertLessEqual(1, len(api.sent))
        self.assertLessEqual(len(api.sent), 2)
        self.assertEqual(set(rows), {RECIPIENT, second})
        self.assertTrue(all(
            row.delivery_status == "PENDING_CONFIRMATION"
            for row in rows.values()
        ))
        self.assertTrue(any(row.request_id for row in rows.values()))
        self.assertEqual(log.write_attempts.count("WORKFLOW_ABORTED"), 1)
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        self.assert_event_privacy(log, second)

    def test_ambiguous_event_log_failure_keeps_atomic_reservations_pending(self):
        second = "01099998888"
        root = make_root((RECIPIENT, second))
        original = EventLogError("event log write failed")
        log = FailingEventLog(root, {"SEND_AMBIGUOUS_OUTCOME": [original]})
        api = OrchestrationApi(
            send_outcomes={
                RECIPIENT: (AmbiguousPostOutcome("response lost"),),
                second: ("success",),
            },
        )
        workflow = make_workflow(
            root,
            ResultStore.for_root(root),
            api,
            clock=ThreadSafeClock(),
            event_log=log,
        )

        with self.assertRaises(EventLogError) as raised:
            workflow.run_live(workflow.current_token())

        rows = ResultStore.for_root(root).load()
        self.assertIs(raised.exception, original)
        self.assertLessEqual(1, len(api.sent))
        self.assertLessEqual(len(api.sent), 2)
        self.assertFalse(any(label.endswith(":time-list") for label in api.trace))
        self.assertEqual(set(rows), {RECIPIENT, second})
        self.assertTrue(any(
            row.attempts == 1 and row.request_id == ""
            for row in rows.values()
        ))
        self.assertEqual(log.write_attempts.count("WORKFLOW_ABORTED"), 1)
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        self.assert_event_privacy(log, second)

    def test_checkpoint_failure_stops_second_recipient_and_snapshot(self):
        second = "01099998888"
        root = make_root((RECIPIENT, second))
        store = FailingCheckpointStore(ResultStore.for_root(root).path, fail_on_write=3)
        log = RecordingEventLog(root)
        api = OrchestrationApi()
        workflow = make_workflow(
            root,
            store,
            api,
            clock=ThreadSafeClock(),
            event_log=log,
        )

        with self.assertRaisesRegex(OSError, "checkpoint unavailable"):
            workflow.run_live(workflow.current_token())

        persisted = ResultStore.for_root(root).load()
        attempted = [
            row
            for row in persisted.values()
            if row.attempts == 1 and row.request_id == ""
        ]
        self.assertLessEqual(len(api.sent), 1)
        self.assertEqual(len(attempted), 1)
        self.assertNotIn("WORKFLOW_COMPLETED", [event for event, _ in log.events])
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        self.assert_event_privacy(log, second)

    def test_failed_aborted_write_never_replaces_original_event_log_error(self):
        root = make_root()
        original = EventLogError("original safe failure")
        abort_failure = EventLogError("secondary safe failure")
        log = FailingEventLog(
            root,
            {
                "SEND_RESPONSE": [original],
                "WORKFLOW_ABORTED": [abort_failure],
            },
        )
        api = ScriptedApi(
            sends=[send_response()],
            lists=[],
            gets=[],
        )
        workflow = make_workflow(
            root, ResultStore.for_root(root), api, event_log=log
        )

        with self.assertRaises(EventLogError) as raised:
            workflow.run_live(workflow.current_token())

        self.assertIs(raised.exception, original)
        self.assertEqual(log.write_attempts.count("WORKFLOW_ABORTED"), 1)
        self.assertEqual(len(api.sent), 1)
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])

    def test_duplicate_recipient_is_sent_once(self):
        root = make_root((RECIPIENT, RECIPIENT))
        api = ScriptedApi(
            sends=[send_response()],
            lists=[list_response(message("PROCESSING"))],
            gets=[result_response(message("COMPLETED", "success", "0"))],
        )
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        self.assertEqual(summary.sent, 1)
        self.assertEqual(len(api.sent), 1)

    def test_invalid_recipient_is_recorded_failed_without_any_api_call(self):
        root = make_root(("010-ABCD-0000",))
        api = ScriptedApi(sends=[], lists=[], gets=[])
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        summary = workflow.run_live(workflow.current_token())

        rows = ResultStore.for_root(root).load()
        row = next(iter(rows.values()))
        self.assertEqual((summary.failed, row.delivery_status, row.attempts), (1, "FAILED", 0))
        self.assertEqual(row.delivery_id, "")
        self.assertEqual(row.error["status"], "VALIDATION_ERROR")
        self.assertEqual((api.uploaded, api.sent), ([], []))

    def test_invalid_image_blocks_before_any_api_call(self):
        root = make_root()
        (root / "mms_img" / "mms_02_details.jpg").write_bytes(b"not-a-jpeg")
        api = ScriptedApi(sends=[], lists=[], gets=[])
        workflow = make_workflow(root, ResultStore.for_root(root), api)

        with self.assertRaises(ValueError):
            workflow.current_token()

        self.assertEqual((api.uploaded, api.sent), ([], []))


if __name__ == "__main__":
    unittest.main()
