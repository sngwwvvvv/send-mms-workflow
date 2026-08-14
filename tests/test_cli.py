import csv
from contextlib import redirect_stderr
from datetime import datetime
from io import StringIO
import inspect
import json
from pathlib import Path
import unittest
from zoneinfo import ZoneInfo

from sens_mms.api import ApiResponse, make_signature
from sens_mms.event_log import EventLogError, create_event_log
from sens_mms.inputs import MESSAGE_BODY
from sens_mms.preflight import build_preflight
from sens_mms.results import ResultRow, ResultStore
from sens_mms_cli import SystemClock, main
from tests.test_workflow import FakeClock, RECIPIENT, make_root


ENV = {
    "NCP_ACCESS_KEY_ID": "FAKE_ACCESS_KEY_SHOULD_NOT_PRINT",
    "NCP_SECRET_KEY": "FAKE_SECRET_KEY_SHOULD_NOT_PRINT",
    "NCP_SENS_SERVICE_ID": "FAKE_SERVICE_ID_SHOULD_NOT_PRINT",
    "NCP_SENS_FROM_NUMBER": "0212345678",
    "SENS_CONTENT_TYPE": "COMM",
}
SECOND = "01099998888"
FORMATTED_RECIPIENT = "010-1234-5678"
NAME = "Sensitive Recipient Name"
OLD_ID_1 = "AAAAAAAAAAAAAAAA"
OLD_ID_2 = "BBBBBBBBBBBBBBBB"
SENT_ID = "CCCCCCCCCCCCCCCC"
PENDING_ID = "DDDDDDDDDDDDDDDD"
UNRELATED_ID = "EEEEEEEEEEEEEEEE"
NEW_ID_1 = "FFFFFFFFFFFFFFFF"
NEW_ID_2 = "GGGGGGGGGGGGGGGG"
REQUEST_1 = "request-sensitive-1"
REQUEST_2 = "request-sensitive-2"
MESSAGE_1 = "message-sensitive-1"
MESSAGE_2 = "message-sensitive-2"
FILE_1 = "file-sensitive-1"
FILE_2 = "file-sensitive-2"
BLOCKED = {
    "status": "BLOCKED",
    "message": "operation could not be completed safely",
}
LEGACY_BYTES = (
    b"receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error\r\n"
    b"01012345678,SENT,true,1,legacy-request,legacy-message,null\r\n"
)


class ScriptedHttpTransport:
    def __init__(self, responses=()):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body, timeout))
        if not self.responses:
            raise AssertionError("unexpected network call")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


class FixedIdFactory:
    def __init__(self, *values):
        self.values = iter(values)

    def __call__(self, existing_ids):
        value = next(self.values)
        if value in existing_ids:
            raise AssertionError("test delivery ID collided")
        return value


class FactoryTracker:
    def __init__(self, factory=create_event_log):
        self.factory = factory
        self.calls = []

    def __call__(self, logs_dir, started_at, *, now):
        self.calls.append((Path(logs_dir), started_at, now))
        return self.factory(logs_dir, started_at, now=now)


class FailOnSendResponseLog:
    def __init__(self, delegate, *, close_error=None):
        self.delegate = delegate
        self.path = delegate.path
        self.close_error = close_error

    def write(self, event, **fields):
        if event == "SEND_RESPONSE":
            raise EventLogError(
                f"sensitive log error {RECIPIENT} {REQUEST_1} {ENV['NCP_SECRET_KEY']}"
            )
        self.delegate.write(event, **fields)

    def close(self):
        self.delegate.close()
        if self.close_error is not None:
            raise self.close_error


class CloseFailingLog:
    def __init__(self, delegate):
        self.delegate = delegate
        self.path = delegate.path

    def write(self, event, **fields):
        self.delegate.write(event, **fields)

    def close(self):
        self.delegate.close()
        raise EventLogError(f"sensitive close failure {RECIPIENT} {REQUEST_1}")


class MutateCurrentOnCloseLog:
    def __init__(self, delegate, root):
        self.delegate = delegate
        self.path = delegate.path
        self.root = root

    def write(self, event, **fields):
        self.delegate.write(event, **fields)

    def close(self):
        self.delegate.close()
        store = ResultStore.for_root(self.root)
        rows = store.load()
        row = next(iter(rows.values()))
        row.error = {
            "status": "SENSITIVE_CURRENT_STATUS",
            "message": "SENSITIVE_CURRENT_ONLY_MESSAGE",
        }
        store.upsert(row)
        store.write_atomic()


class MutateCurrentOnStartedLog:
    def __init__(self, delegate, root, changed_fields):
        self.delegate = delegate
        self.path = delegate.path
        self.root = root
        self.changed_fields = changed_fields

    def write(self, event, **fields):
        self.delegate.write(event, **fields)
        if event == "WORKFLOW_STARTED":
            store = ResultStore.for_root(self.root)
            row = store.load()[RECIPIENT]
            for key, value in self.changed_fields.items():
                setattr(row, key, value)
            store.upsert(row)
            store.write_atomic()

    def close(self):
        self.delegate.close()


def api_response(status, payload):
    return ApiResponse(status, json.dumps(payload).encode("utf-8"), {})


def official_message(
    recipient,
    request_id,
    message_id,
    status,
    *,
    status_name="",
    status_code="",
    status_message="",
):
    return {
        "requestId": request_id,
        "messageId": message_id,
        "requestTime": "2026-08-13 10:00:00",
        "contentType": "COMM",
        "type": "MMS",
        "subject": "must be filtered",
        "content": MESSAGE_BODY,
        "countryCode": "82",
        "from": ENV["NCP_SENS_FROM_NUMBER"],
        "to": recipient,
        "completeTime": "" if status != "COMPLETED" else "2026-08-13 10:00:30",
        "telcoCode": "SKT",
        "files": [{"fileId": FILE_1, "name": "mms_01_intro.jpg"}],
        "status": status,
        "statusCode": status_code,
        "statusName": status_name,
        "statusMessage": status_message,
    }


def success_responses(recipient=RECIPIENT, request_id=REQUEST_1, message_id=MESSAGE_1):
    processing = official_message(
        recipient, request_id, message_id, "PROCESSING"
    )
    completed = official_message(
        recipient,
        request_id,
        message_id,
        "COMPLETED",
        status_name="success",
        status_code="0",
        status_message="success",
    )
    return [
        api_response(200, {"fileId": FILE_1}),
        api_response(200, {"fileId": FILE_2}),
        api_response(
            202,
            {
                "requestId": request_id,
                "requestTime": "2026-08-13T10:00:00.000",
                "statusCode": "202",
                "statusName": "success",
            },
        ),
        api_response(
            200,
            {
                "statusCode": "202",
                "statusName": "success",
                "pageSize": 100,
                "pageIndex": 0,
                "itemCount": 1,
                "hasMore": False,
                "messages": [processing],
            },
        ),
        api_response(
            200,
            {
                "statusCode": "200",
                "statusName": "success",
                "messages": [completed],
            },
        ),
    ]


def recipient_success_responses(recipient, request_id, message_id):
    processing = official_message(
        recipient, request_id, message_id, "PROCESSING"
    )
    completed = official_message(
        recipient,
        request_id,
        message_id,
        "COMPLETED",
        status_name="success",
        status_code="0",
        status_message="success",
    )
    return [
        api_response(
            202,
            {
                "requestId": request_id,
                "requestTime": "2026-08-13T10:00:00.000",
                "statusCode": "202",
                "statusName": "success",
            },
        ),
        api_response(
            200,
            {
                "statusCode": "202",
                "statusName": "success",
                "pageSize": 100,
                "pageIndex": 0,
                "itemCount": 1,
                "hasMore": False,
                "messages": [processing],
            },
        ),
        api_response(
            200,
            {
                "statusCode": "200",
                "statusName": "success",
                "messages": [completed],
            },
        ),
    ]


def run_cli(argv, root, *, transport=None, event_log_factory=create_event_log,
            delivery_id_factory=None, clock=None):
    output = StringIO()
    kwargs = {
        "root": root,
        "environ": ENV,
        "transport": transport or ScriptedHttpTransport(),
        "clock": clock or FakeClock(),
        "timestamp_ms": lambda: "1700000000000",
        "stdout": output,
    }
    parameters = inspect.signature(main).parameters
    if "event_log_factory" in parameters:
        kwargs["event_log_factory"] = event_log_factory
    if "delivery_id_factory" in parameters:
        kwargs["delivery_id_factory"] = (
            delivery_id_factory or FixedIdFactory(NEW_ID_1, NEW_ID_2)
        )
    code = main(argv, **kwargs)
    return code, json.loads(output.getvalue()), output.getvalue()


def approval_token(root, *, resend_failed=False):
    args = ["preflight"]
    if resend_failed:
        args.append("--resend-failed")
    code, public, _ = run_cli(args, root)
    if code != 0:
        raise AssertionError(public)
    return public["approval_token"]


def failed_row(number, delivery_id, request_id, message_id, *, message="api detail"):
    return ResultRow(
        receiving_number=number,
        delivery_id=delivery_id,
        delivery_status="FAILED",
        is_sent="false",
        attempts=3,
        request_id=request_id,
        message_id=message_id,
        error={"status": "3001", "message": message},
    )


def sent_row(number):
    return ResultRow(
        receiving_number=number,
        delivery_id=SENT_ID,
        delivery_status="SENT",
        is_sent="true",
        attempts=1,
        request_id="sent-request",
        message_id="sent-message",
        error=None,
    )


def pending_reservation(number):
    return ResultRow(
        receiving_number=number,
        delivery_id=PENDING_ID,
        delivery_status="PENDING_CONFIRMATION",
        is_sent="",
        attempts=0,
        request_id="",
        message_id="",
        error=None,
    )


def validation_failure(number):
    return ResultRow(
        receiving_number=number,
        delivery_id="",
        delivery_status="FAILED",
        is_sent="false",
        attempts=0,
        request_id="",
        message_id="",
        error={"status": "VALIDATION_ERROR", "message": "수신번호 검증 실패"},
    )


def write_snapshot(root, name, rows):
    path = root / "results" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=tuple(rows[0].to_dict()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_dict())
    return path


def seed_resend_root():
    sent = "01011112222"
    pending = "01033334444"
    invalid = "not-a-number"
    unrelated = "01055556666"
    root = make_root((RECIPIENT, SECOND, sent, pending, invalid))
    rows = (
        failed_row(RECIPIENT, OLD_ID_1, REQUEST_1, MESSAGE_1),
        failed_row(SECOND, OLD_ID_2, REQUEST_2, MESSAGE_2),
        sent_row(sent),
        pending_reservation(pending),
        validation_failure(invalid),
        failed_row(
            unrelated,
            UNRELATED_ID,
            "unrelated-request",
            "unrelated-message",
        ),
    )
    store = ResultStore.for_root(root)
    for row in rows:
        store.upsert(row)
    store.write_atomic()
    older = write_snapshot(root, "result_20260814_100000.csv", (rows[0],))
    latest = write_snapshot(root, "result_20260814_110000.csv", rows)
    return root, older, latest, rows


class CliTests(unittest.TestCase):
    def assert_public_safe(self, public, *additional):
        rendered = json.dumps(public, ensure_ascii=False)
        signature = make_signature(
            "POST",
            f"/sms/v2/services/{ENV['NCP_SENS_SERVICE_ID']}/messages",
            "1700000000000",
            ENV["NCP_ACCESS_KEY_ID"],
            ENV["NCP_SECRET_KEY"],
        )
        forbidden = (
            RECIPIENT,
            SECOND,
            FORMATTED_RECIPIENT,
            NAME,
            OLD_ID_1,
            OLD_ID_2,
            NEW_ID_1,
            NEW_ID_2,
            REQUEST_1,
            REQUEST_2,
            MESSAGE_1,
            MESSAGE_2,
            ENV["NCP_ACCESS_KEY_ID"],
            ENV["NCP_SECRET_KEY"],
            ENV["NCP_SENS_SERVICE_ID"],
            signature,
            MESSAGE_BODY,
            FILE_1,
            FILE_2,
            *additional,
        )
        for value in forbidden:
            self.assertNotIn(value, rendered)

    def test_system_clock_now_uses_explicit_seoul_timezone(self):
        now = SystemClock.now()

        self.assertEqual(now.tzinfo, ZoneInfo("Asia/Seoul"))

    def test_normal_live_writes_checkpoint_snapshot_and_jsonl_under_dedicated_folders(self):
        root = make_root()
        transport = ScriptedHttpTransport(success_responses())
        token = approval_token(root)

        code, public, rendered = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            transport=transport,
            delivery_id_factory=FixedIdFactory(NEW_ID_1),
        )

        current = root / "results" / "result.csv"
        snapshots = list((root / "results").glob("result_*.csv"))
        logs = list((root / "logs").glob("delivery_*.jsonl"))
        self.assertEqual(code, 0)
        self.assertTrue(current.exists())
        self.assertEqual(len(snapshots), 1)
        self.assertEqual(len(logs), 1)
        self.assertEqual(Path(public["result_csv"]), current.resolve())
        self.assertEqual(Path(public["result_snapshot"]), snapshots[0].resolve())
        self.assertEqual(Path(public["event_log"]), logs[0].resolve())
        self.assertTrue(Path(public["result_snapshot"]).is_absolute())
        self.assertTrue(Path(public["event_log"]).is_absolute())
        events = [json.loads(line) for line in logs[0].read_text(encoding="utf-8").splitlines()]
        self.assertTrue(events)
        self.assertEqual(public["approved_at"], "2026-08-13T10:00:00.000+09:00")
        self.assertEqual(snapshots[0].name, "result_20260813_100000.csv")
        self.assertTrue(all(event["logged_at"].endswith("+09:00") for event in events))
        serialized_events = json.dumps(events, ensure_ascii=False)
        for forbidden in (
            RECIPIENT,
            ENV["NCP_SENS_FROM_NUMBER"],
            MESSAGE_BODY,
            FILE_1,
            FILE_2,
            ENV["NCP_ACCESS_KEY_ID"],
            ENV["NCP_SECRET_KEY"],
            ENV["NCP_SENS_SERVICE_ID"],
        ):
            self.assertNotIn(forbidden, serialized_events)
        self.assertEqual((public["sent"], public["failed"], public["pending_confirmation"]), (1, 0, 0))
        self.assert_public_safe(public)
        self.assertNotIn(RECIPIENT, rendered)

    def test_preflight_creates_no_event_log_and_never_calls_network(self):
        root = make_root()
        transport = ScriptedHttpTransport()
        tracker = FactoryTracker()

        code, public, _ = run_cli(
            ["preflight"], root, transport=transport, event_log_factory=tracker
        )

        self.assertEqual(code, 0)
        self.assertEqual(public["mode"], "normal")
        self.assertEqual(transport.calls, [])
        self.assertEqual(tracker.calls, [])
        self.assertFalse((root / "logs").exists())

    def test_wrong_token_and_missing_sender_confirmation_create_no_log_or_calls(self):
        for argv in (
            ["live", "--approval-token", "wrong", "--confirm-sender-registered"],
            ["live", "--approval-token", "irrelevant"],
        ):
            with self.subTest(argv=argv):
                root = make_root()
                transport = ScriptedHttpTransport()
                tracker = FactoryTracker()

                code, public, _ = run_cli(
                    argv, root, transport=transport, event_log_factory=tracker
                )

                self.assertNotEqual(code, 0)
                self.assertEqual(public, BLOCKED)
                self.assertEqual(transport.calls, [])
                self.assertEqual(tracker.calls, [])
                self.assertFalse((root / "logs").exists())
                self.assert_public_safe(public, "wrong", "irrelevant")

    def test_event_log_creation_failure_makes_zero_calls_and_prints_fixed_blocked_output(self):
        root = make_root()
        transport = ScriptedHttpTransport()
        token = approval_token(root)

        def fail_creation(logs_dir, started_at, *, now):
            raise EventLogError(
                f"sensitive create failure {RECIPIENT} {ENV['NCP_SECRET_KEY']}"
            )

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            transport=transport,
            event_log_factory=fail_creation,
        )

        self.assertNotEqual(code, 0)
        self.assertEqual(public, BLOCKED)
        self.assertEqual(transport.calls, [])
        self.assert_public_safe(public)

    def test_event_log_mid_run_failure_stops_later_recipient_and_keeps_accepted_pending(self):
        root = make_root((RECIPIENT, SECOND))
        token = approval_token(root)
        transport = ScriptedHttpTransport(
            success_responses()
            + recipient_success_responses(SECOND, REQUEST_2, MESSAGE_2)
        )

        def failing_factory(logs_dir, started_at, *, now):
            return FailOnSendResponseLog(
                create_event_log(logs_dir, started_at, now=now)
            )

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            transport=transport,
            event_log_factory=failing_factory,
            delivery_id_factory=FixedIdFactory(NEW_ID_1, NEW_ID_2),
        )

        rows = ResultStore.for_root(root).load()
        self.assertNotEqual(code, 0)
        self.assertEqual(public, BLOCKED)
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual(set(rows), {RECIPIENT})
        self.assertEqual(rows[RECIPIENT].delivery_status, "PENDING_CONFIRMATION")
        self.assertEqual(rows[RECIPIENT].request_id, REQUEST_1)
        self.assertEqual(list((root / "results").glob("result_*.csv")), [])
        self.assert_public_safe(public)

    def test_close_failure_after_success_blocks_completion(self):
        root = make_root()
        token = approval_token(root)
        transport = ScriptedHttpTransport(success_responses())

        def close_failing_factory(logs_dir, started_at, *, now):
            return CloseFailingLog(create_event_log(logs_dir, started_at, now=now))

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            transport=transport,
            event_log_factory=close_failing_factory,
            delivery_id_factory=FixedIdFactory(NEW_ID_1),
        )

        self.assertNotEqual(code, 0)
        self.assertEqual(public, BLOCKED)
        self.assertNotIn("sent", public)
        self.assert_public_safe(public)

    def test_close_failure_during_run_failure_does_not_expose_either_exception(self):
        root = make_root((RECIPIENT, SECOND))
        token = approval_token(root)
        transport = ScriptedHttpTransport(success_responses())

        def doubly_failing_factory(logs_dir, started_at, *, now):
            return FailOnSendResponseLog(
                create_event_log(logs_dir, started_at, now=now),
                close_error=EventLogError(f"secondary close secret {REQUEST_2}"),
            )

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            transport=transport,
            event_log_factory=doubly_failing_factory,
            delivery_id_factory=FixedIdFactory(NEW_ID_1, NEW_ID_2),
        )

        self.assertNotEqual(code, 0)
        self.assertEqual(public, BLOCKED)
        self.assertEqual(len(transport.calls), 3)
        self.assert_public_safe(public, "secondary close secret")

    def test_resend_preflight_selects_filename_latest_snapshot_with_exact_count_and_zero_calls(self):
        root, older, latest, _ = seed_resend_root()
        transport = ScriptedHttpTransport()
        tracker = FactoryTracker()

        code, public, _ = run_cli(
            ["preflight", "--resend-failed"],
            root,
            transport=transport,
            event_log_factory=tracker,
        )

        self.assertEqual(code, 0)
        self.assertEqual(public["mode"], "resend_failed")
        self.assertEqual(public["eligible_count"], 2)
        self.assertEqual(public["resend_source"], latest.name)
        self.assertNotEqual(public["resend_source"], older.name)
        self.assertEqual(transport.calls, [])
        self.assertEqual(tracker.calls, [])

    def test_resend_live_assigns_new_ids_only_to_candidates_after_fresh_matching_token(self):
        root, _, _, original_rows = seed_resend_root()
        before = {row.receiving_number: row for row in original_rows}
        token = approval_token(root, resend_failed=True)
        transport = ScriptedHttpTransport(
            [api_response(200, {"fileId": FILE_1}), api_response(200, {"fileId": FILE_2})]
            + recipient_success_responses(RECIPIENT, "new-request-1", "new-message-1")
            + recipient_success_responses(SECOND, "new-request-2", "new-message-2")
        )

        code, public, _ = run_cli(
            [
                "live",
                "--resend-failed",
                "--approval-token",
                token,
                "--confirm-sender-registered",
            ],
            root,
            transport=transport,
            delivery_id_factory=FixedIdFactory(NEW_ID_1, NEW_ID_2),
        )

        after = ResultStore.for_root(root).load()
        self.assertEqual(code, 0)
        self.assertEqual(after[RECIPIENT].delivery_id, NEW_ID_1)
        self.assertEqual(after[SECOND].delivery_id, NEW_ID_2)
        self.assertTrue({OLD_ID_1, OLD_ID_2}.isdisjoint({NEW_ID_1, NEW_ID_2}))
        for number in ("01011112222", "01033334444", "not-a-number", "01055556666"):
            self.assertEqual(after[number], before[number])
        self.assertEqual(len(transport.calls), 8)
        self.assert_public_safe(public, "new-request-1", "new-message-1", "new-request-2", "new-message-2")

    def test_resend_live_rejects_stale_normal_token_before_log_or_network(self):
        root, _, _, _ = seed_resend_root()
        stale_normal_token = approval_token(root)
        tracker = FactoryTracker()
        transport = ScriptedHttpTransport()

        code, public, _ = run_cli(
            [
                "live",
                "--resend-failed",
                "--approval-token",
                stale_normal_token,
                "--confirm-sender-registered",
            ],
            root,
            transport=transport,
            event_log_factory=tracker,
        )

        self.assertNotEqual(code, 0)
        self.assertEqual(public, BLOCKED)
        self.assertEqual(tracker.calls, [])
        self.assertEqual(transport.calls, [])

    def test_corrupt_latest_snapshot_and_current_mismatch_block_before_log_or_calls(self):
        for mode in ("corrupt", "mismatch"):
            with self.subTest(mode=mode):
                root, _, latest, _ = seed_resend_root()
                if mode == "corrupt":
                    latest.write_bytes(b"corrupt latest snapshot")
                else:
                    store = ResultStore.for_root(root)
                    rows = store.load()
                    rows[RECIPIENT].request_id = "changed-current-request"
                    store.write_atomic()
                tracker = FactoryTracker()
                transport = ScriptedHttpTransport()

                code, public, _ = run_cli(
                    [
                        "live",
                        "--resend-failed",
                        "--approval-token",
                        "untrusted-token",
                        "--confirm-sender-registered",
                    ],
                    root,
                    transport=transport,
                    event_log_factory=tracker,
                )

                self.assertNotEqual(code, 0)
                self.assertEqual(public, BLOCKED)
                self.assertEqual(tracker.calls, [])
                self.assertEqual(transport.calls, [])
                self.assert_public_safe(public, "changed-current-request", "untrusted-token")

    def test_resend_second_preflight_blocks_started_log_exact_row_mutation_before_transport(self):
        changes = (
            {"delivery_id": NEW_ID_1},
            {"attempts": 2},
            {"error": {"status": "3002", "message": "redacted"}},
        )
        for changed_fields in changes:
            with self.subTest(changed_fields=changed_fields):
                root, _, _, _ = seed_resend_root()
                token = approval_token(root, resend_failed=True)
                transport = ScriptedHttpTransport()

                def mutating_factory(logs_dir, started_at, *, now):
                    return MutateCurrentOnStartedLog(
                        create_event_log(logs_dir, started_at, now=now),
                        root,
                        changed_fields,
                    )

                code, public, _ = run_cli(
                    [
                        "live",
                        "--resend-failed",
                        "--approval-token",
                        token,
                        "--confirm-sender-registered",
                    ],
                    root,
                    transport=transport,
                    event_log_factory=mutating_factory,
                )

                self.assertNotEqual(code, 0)
                self.assertEqual(public, BLOCKED)
                self.assertEqual(transport.calls, [])
                logs = list((root / "logs").glob("delivery_*.jsonl"))
                self.assertEqual(len(logs), 1)
                rows = [
                    json.loads(line)
                    for line in logs[0].read_text(encoding="utf-8").splitlines()
                ]
                self.assertEqual(
                    [row["event"] for row in rows],
                    ["WORKFLOW_STARTED", "WORKFLOW_ABORTED"],
                )
                serialized = json.dumps(rows, ensure_ascii=False)
                for forbidden in (
                    RECIPIENT,
                    ENV["NCP_SENS_FROM_NUMBER"],
                    MESSAGE_BODY,
                    ENV["NCP_ACCESS_KEY_ID"],
                    ENV["NCP_SECRET_KEY"],
                    ENV["NCP_SENS_SERVICE_ID"],
                ):
                    self.assertNotIn(forbidden, serialized)

    def test_legacy_root_result_migrates_once_without_changing_source_and_repeat_is_idempotent(self):
        root = make_root()
        legacy = root / "result.csv"
        legacy.write_bytes(LEGACY_BYTES)
        tracker = FactoryTracker()
        transport = ScriptedHttpTransport()

        first_code, first_public, _ = run_cli(
            ["preflight"], root, transport=transport, event_log_factory=tracker
        )
        migrated_bytes = (root / "results" / "result.csv").read_bytes()
        marker_bytes = (root / "results" / ".legacy_result_migrated.json").read_bytes()
        second_code, second_public, _ = run_cli(
            ["preflight"], root, transport=transport, event_log_factory=tracker
        )

        self.assertEqual((first_code, second_code), (0, 0))
        self.assertEqual(first_public["approval_token"], second_public["approval_token"])
        self.assertEqual(legacy.read_bytes(), LEGACY_BYTES)
        self.assertEqual((root / "results" / "result.csv").read_bytes(), migrated_bytes)
        self.assertEqual((root / "results" / ".legacy_result_migrated.json").read_bytes(), marker_bytes)
        self.assertEqual(transport.calls, [])
        self.assertEqual(tracker.calls, [])

    def test_legacy_plus_new_current_without_marker_blocks_before_log_or_calls(self):
        root = make_root()
        (root / "result.csv").write_bytes(LEGACY_BYTES)
        store = ResultStore.for_root(root)
        store.upsert(sent_row(RECIPIENT))
        store.write_atomic()
        tracker = FactoryTracker()
        transport = ScriptedHttpTransport()

        code, public, _ = run_cli(
            ["preflight"], root, transport=transport, event_log_factory=tracker
        )

        self.assertNotEqual(code, 0)
        self.assertEqual(public, BLOCKED)
        self.assertEqual(transport.calls, [])
        self.assertEqual(tracker.calls, [])
        self.assert_public_safe(public, "legacy-request", "legacy-message")

    def test_completion_error_summary_comes_from_snapshot_and_uses_fixed_message(self):
        root = make_root(("not-a-number",))
        token = approval_token(root)

        def mutating_factory(logs_dir, started_at, *, now):
            return MutateCurrentOnCloseLog(
                create_event_log(logs_dir, started_at, now=now), root
            )

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            event_log_factory=mutating_factory,
        )

        self.assertEqual(code, 0)
        self.assertEqual(
            public["failed_error_summary"],
            [{"status": "VALIDATION_ERROR", "message": "recipient validation failed", "count": 1}],
        )
        self.assertNotIn("SENSITIVE_CURRENT", json.dumps(public))
        self.assert_public_safe(public, "not-a-number", "SENSITIVE_CURRENT_ONLY_MESSAGE")

    def test_completion_replaces_arbitrary_status_that_looks_like_a_delivery_id(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(
            failed_row(
                RECIPIENT,
                OLD_ID_2,
                REQUEST_1,
                MESSAGE_1,
                message=f"sensitive failure {RECIPIENT} {ENV['NCP_SECRET_KEY']}",
            )
        )
        store.rows[RECIPIENT].error["status"] = OLD_ID_1
        store.write_atomic()
        token = approval_token(root)

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
        )

        self.assertEqual(code, 0)
        self.assertEqual(
            public["failed_error_summary"],
            [{"status": "UNKNOWN", "message": "delivery failed", "count": 1}],
        )
        self.assert_public_safe(public)

    def test_completion_replaces_ten_digit_sender_shaped_numeric_status(self):
        root = make_root()
        store = ResultStore.for_root(root)
        store.upsert(
            failed_row(
                RECIPIENT,
                OLD_ID_2,
                REQUEST_1,
                MESSAGE_1,
                message="sensitive human failure",
            )
        )
        store.rows[RECIPIENT].error["status"] = "0212345678"
        store.write_atomic()
        token = approval_token(root)

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
        )

        self.assertEqual(code, 0)
        self.assertEqual(
            public["failed_error_summary"],
            [{"status": "UNKNOWN", "message": "delivery failed", "count": 1}],
        )
        self.assertNotIn("0212345678", json.dumps(public))

    def test_invalid_sensitive_arguments_return_fixed_blocked_json_without_stderr_or_system_exit(self):
        root = make_root()
        output = StringIO()
        error_output = StringIO()
        sensitive_argument = "--secret-value=010-1234-5678-Alice-opaque-token"

        with redirect_stderr(error_output):
            try:
                code = main(
                    ["live", sensitive_argument],
                    root=root,
                    environ=ENV,
                    transport=ScriptedHttpTransport(),
                    clock=FakeClock(),
                    stdout=output,
                )
            except SystemExit as exc:
                code = exc.code

        expected = json.dumps(BLOCKED, ensure_ascii=False) + "\n"
        self.assertEqual(code, 2)
        self.assertEqual(output.getvalue(), expected)
        self.assertEqual(error_output.getvalue(), "")
        self.assertNotIn("010-1234-5678", output.getvalue())
        self.assertNotIn("Alice", output.getvalue())
        self.assertNotIn("opaque-token", output.getvalue())

    def test_naive_injected_clock_is_wrapped_as_explicit_seoul_for_log_and_snapshot(self):
        root = make_root()
        token = approval_token(root)
        observed_now = []

        def inspecting_factory(logs_dir, started_at, *, now):
            observed_now.append(now())
            return create_event_log(logs_dir, started_at, now=now)

        code, public, _ = run_cli(
            ["live", "--approval-token", token, "--confirm-sender-registered"],
            root,
            transport=ScriptedHttpTransport(success_responses()),
            event_log_factory=inspecting_factory,
            delivery_id_factory=FixedIdFactory(NEW_ID_1),
            clock=FakeClock(),
        )

        self.assertEqual(code, 0)
        self.assertEqual(observed_now[0].tzinfo, ZoneInfo("Asia/Seoul"))
        self.assertEqual(observed_now[0].isoformat(), "2026-08-13T10:00:00+09:00")
        self.assertEqual(
            Path(public["result_snapshot"]).name,
            "result_20260813_100000.csv",
        )
        log_rows = [
            json.loads(line)
            for line in Path(public["event_log"])
            .read_text(encoding="utf-8")
            .splitlines()
        ]
        self.assertTrue(all(row["logged_at"].endswith("+09:00") for row in log_rows))


if __name__ == "__main__":
    unittest.main()
