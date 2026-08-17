import json
import inspect
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from sens_mms import event_log
from sens_mms.api import (
    MessageListResponse,
    MessageRecord,
    MessageResultResponse,
    SendResponse,
)
from sens_mms.event_log import (
    EventLogError,
    JsonlEventLog,
    create_event_log,
    mask_digit_runs,
    safe_error_dict,
)


def message_record():
    return MessageRecord(
        request_id="request-1",
        message_id="message-1",
        message_type="MMS",
        to="01012345678",
        request_time="2026-08-13 10:00:00",
        complete_time="2026-08-13 10:00:30",
        telco_code="SKT",
        status="COMPLETED",
        status_code="0",
        status_name="fail",
        status_message="failed for 01012345678",
    )


def serialized_message():
    return {
        "requestId": "request-1",
        "messageId": "message-1",
        "requestTime": "2026-08-13 10:00:00",
        "completeTime": "2026-08-13 10:00:30",
        "telcoCode": "SKT",
        "status": "COMPLETED",
        "statusCode": "0",
        "statusName": "fail",
        "statusMessage": "redacted",
    }


class EventLogTests(unittest.TestCase):
    def test_private_serializer_builds_the_exact_jsonl_row_before_write(self):
        serializer = getattr(event_log, "_serialize_event_line", None)
        self.assertTrue(callable(serializer), "_serialize_event_line must exist")

        encoded = serializer(
            now=datetime(2026, 8, 14, 3, 4, 5, 678901, tzinfo=timezone.utc),
            sequence=3,
            event="RESULT_SNAPSHOT_WRITTEN",
            result_snapshot="C:/results/result_20260814_120405.csv",
            completed_at="2026-08-14T12:04:05.678+09:00",
        )

        self.assertEqual(
            json.loads(encoded),
            {
                "schema_version": 1,
                "sequence": 3,
                "logged_at": "2026-08-14T12:04:05.678+09:00",
                "event": "RESULT_SNAPSHOT_WRITTEN",
                "delivery_id": None,
                "attempt": None,
                "api": None,
                "http_status": None,
                "request_id": None,
                "message_id": None,
                "response": None,
                "error": None,
                "state": None,
                "summary": None,
                "completed_at": "2026-08-14T12:04:05.678+09:00",
                "result_snapshot": "C:/results/result_20260814_120405.csv",
            },
        )

    def test_resend_archive_event_is_path_free_and_deidentified(self):
        """Catches the archive boundary leaking a path or correlation identifier."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        now = lambda: datetime(2026, 8, 14, 3, 4, 5, 678901, tzinfo=timezone.utc)

        with JsonlEventLog(path, now=now) as log:
            log.write("RESEND_ARCHIVE_WRITTEN")

        row = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(row["event"], "RESEND_ARCHIVE_WRITTEN")
        self.assertEqual(
            {
                key: value
                for key, value in row.items()
                if key not in {"schema_version", "sequence", "logged_at", "event"}
            },
            {
                "delivery_id": None,
                "attempt": None,
                "api": None,
                "http_status": None,
                "request_id": None,
                "message_id": None,
                "response": None,
                "error": None,
                "state": None,
                "summary": None,
                "completed_at": None,
                "result_snapshot": None,
            },
        )

    def test_total_sanitizers_never_invoke_hostile_values_across_jsonl_boundaries(self):
        """Catches response/error/state sanitizers invoking or rejecting hostile values."""
        calls = []

        class HostileText(str):
            def __str__(self):
                calls.append("str")
                raise RuntimeError("HOSTILE_TEXT_MARKER")

            def __eq__(self, other):
                calls.append("eq")
                raise RuntimeError("HOSTILE_TEXT_MARKER")

            def __bool__(self):
                calls.append("bool")
                raise RuntimeError("HOSTILE_TEXT_MARKER")

            def __len__(self):
                calls.append("len")
                raise RuntimeError("HOSTILE_TEXT_MARKER")

            def isascii(self):
                calls.append("isascii")
                raise RuntimeError("HOSTILE_TEXT_MARKER")

            def isdigit(self):
                calls.append("isdigit")
                raise RuntimeError("HOSTILE_TEXT_MARKER")

        class HostileObject:
            def __str__(self):
                calls.append("str")
                raise RuntimeError("HOSTILE_OBJECT_MARKER")

            def __eq__(self, other):
                calls.append("eq")
                raise RuntimeError("HOSTILE_OBJECT_MARKER")

            def __bool__(self):
                calls.append("bool")
                raise RuntimeError("HOSTILE_OBJECT_MARKER")

            def __len__(self):
                calls.append("len")
                raise RuntimeError("HOSTILE_OBJECT_MARKER")

        cases = (
            (["RAW_LIST_MARKER"], "redacted"),
            ({"raw": "RAW_DICT_MARKER"}, "redacted"),
            (HostileText("HOSTILE_TEXT_MARKER"), "redacted"),
            (HostileObject(), "redacted"),
        )
        boundaries = (
            lambda value: {"response": {"statusCode": value, "statusMessage": value}},
            lambda value: {"error": {"status": value, "message": value}},
            lambda value: {
                "state": {"error": {"status": value, "message": value}}
            },
        )
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        raised_count = 0
        exception_leaked_marker = False
        log = JsonlEventLog(path, now=lambda: datetime.now(timezone.utc))
        try:
            for boundary in boundaries:
                for value, _ in cases:
                    try:
                        log.write("hostile_api_value", **boundary(value))
                    except Exception as exc:
                        raised_count += 1
                        exception_text = str(exc)
                        exception_leaked_marker = exception_leaked_marker or any(
                            marker in exception_text
                            for marker in ("HOSTILE_TEXT_MARKER", "HOSTILE_OBJECT_MARKER")
                        )
        finally:
            log.close()

        self.assertFalse(
            exception_leaked_marker,
            "a hostile marker escaped through an exception",
        )
        self.assertEqual(raised_count, 0, "total sanitization must not raise")
        self.assertEqual(calls, [], "sanitization invoked hostile user code")
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(rows), 12)
        for index, row in enumerate(rows):
            boundary_index = index // len(cases)
            expected_message = cases[index % len(cases)][1]
            if boundary_index == 0:
                actual = row["response"]
            elif boundary_index == 1:
                actual = row["error"]
            else:
                actual = row["state"]["error"]
            self.assertEqual(
                actual,
                {"statusCode": "UNKNOWN", "statusMessage": expected_message}
                if boundary_index == 0
                else {"status": "UNKNOWN", "message": expected_message},
            )
        rendered = path.read_text(encoding="utf-8")
        for marker in (
            "RAW_LIST_MARKER",
            "RAW_DICT_MARKER",
            "HOSTILE_TEXT_MARKER",
            "HOSTILE_OBJECT_MARKER",
        ):
            self.assertNotIn(marker, rendered)

    def test_exact_empty_builtin_messages_are_empty_at_every_jsonl_boundary(self):
        """Catches total sanitization rejecting exact empty built-in containers."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        raised_count = 0
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            for value in (None, "", [], {}):
                try:
                    log.write(
                        "empty_api_value",
                        response={"statusCode": value, "statusMessage": value},
                        error={"status": value, "message": value},
                        state={"error": {"status": value, "message": value}},
                    )
                except Exception:
                    raised_count += 1

        self.assertEqual(raised_count, 0, "exact empty built-ins must not raise")
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(rows), 4)
        for row in rows:
            self.assertEqual(
                row["response"],
                {"statusCode": "UNKNOWN", "statusMessage": ""},
            )
            self.assertEqual(row["error"], {"status": "UNKNOWN", "message": ""})
            self.assertEqual(
                row["state"]["error"],
                {"status": "UNKNOWN", "message": ""},
            )

    def test_other_response_fields_accept_only_exact_builtin_types(self):
        """Catches choice/time/page/ID/container fields retaining hostile subclasses."""
        calls = []

        class HostileText(str):
            def _called(self, name):
                calls.append(name)
                raise RuntimeError("HOSTILE_FIELD_MARKER")

            def __str__(self):
                return self._called("str")

            def __eq__(self, other):
                return self._called("eq")

            def __bool__(self):
                return self._called("bool")

            def __len__(self):
                return self._called("len")

            def lower(self):
                return self._called("lower")

        class HostileInt(int):
            def __str__(self):
                calls.append("str")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

            def __int__(self):
                calls.append("int")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

            def __index__(self):
                calls.append("index")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

        class HostileList(list):
            def __iter__(self):
                calls.append("iter")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

            def __len__(self):
                calls.append("len")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

        class HostileDict(dict):
            def __iter__(self):
                calls.append("iter")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

            def items(self):
                calls.append("items")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

            def __len__(self):
                calls.append("len")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

        class HostileObject:
            def __bool__(self):
                calls.append("bool")
                raise RuntimeError("HOSTILE_FIELD_MARKER")

        fields = (
            ("statusName", HostileText("success"), ""),
            ("status", HostileText("READY"), ""),
            ("telcoCode", HostileText("SKT"), ""),
            ("requestTime", HostileText("2026-08-13T10:00:00.000"), ""),
            ("completeTime", HostileText("2026-08-13 10:00:00"), ""),
            ("requestId", HostileText("HOSTILE_FIELD_MARKER"), ""),
            ("messageId", HostileText("HOSTILE_FIELD_MARKER"), ""),
            ("pageSize", HostileInt(10), None),
            ("pageIndex", HostileInt(0), None),
            ("itemCount", HostileInt(1), None),
            ("hasMore", HostileObject(), None),
            ("messages", HostileList([{"status": "READY"}]), []),
            ("message", HostileDict({"status": "READY"}), "redacted"),
        )
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        raised_count = 0
        leaked_marker = False
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            for key, value, _ in fields:
                try:
                    log.write("hostile_response_field", response={key: value})
                except Exception as exc:
                    raised_count += 1
                    leaked_marker = leaked_marker or "HOSTILE_FIELD_MARKER" in str(exc)

        self.assertFalse(leaked_marker, "a hostile field marker escaped an exception")
        self.assertEqual(raised_count, 0, "field sanitization must be total")
        self.assertEqual(calls, [], "field sanitization invoked hostile user code")
        rows = [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(len(rows), len(fields))
        for row, (key, _, expected) in zip(rows, fields):
            self.assertEqual(row["response"], {key: expected})
        self.assertNotIn("HOSTILE_FIELD_MARKER", path.read_text(encoding="utf-8"))

    def test_top_level_and_state_correlation_ids_reject_hostile_str_subclasses(self):
        """Catches correlation ID copies carrying hostile subclasses to the JSON encoder."""
        calls = []

        class HostileId(str):
            def __str__(self):
                calls.append("str")
                raise RuntimeError("HOSTILE_ID_MARKER")

            def __eq__(self, other):
                calls.append("eq")
                raise RuntimeError("HOSTILE_ID_MARKER")

            def __bool__(self):
                calls.append("bool")
                raise RuntimeError("HOSTILE_ID_MARKER")

        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        hostile = HostileId("HOSTILE_ID_MARKER")
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            log.write(
                "hostile_correlation",
                request_id=hostile,
                message_id=hostile,
                state={"request_id": hostile, "message_id": hostile},
            )

        row = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(row["request_id"], "")
        self.assertEqual(row["message_id"], "")
        self.assertEqual(row["state"], {"request_id": "", "message_id": ""})
        self.assertEqual(calls, [])
        self.assertNotIn("HOSTILE_ID_MARKER", path.read_text(encoding="utf-8"))

    def test_send_response_serializer_names_only_safe_documented_fields(self):
        """Catches send serialization that retains an HTTP body or omits correlation fields."""
        serializer = getattr(event_log, "send_to_log_dict", None)
        self.assertIsNotNone(serializer)
        actual = serializer(SendResponse(
            http_status=202,
            request_id="request-1",
            request_time="2026-08-13T10:00:00.000",
            status_code="202",
            status_name="success",
        ))

        self.assertEqual(actual, {
            "requestId": "request-1",
            "requestTime": "2026-08-13T10:00:00.000",
            "statusCode": "202",
            "statusName": "success",
        })

    def test_message_serializer_excludes_internal_recipient_and_redacts_status_message(self):
        """Catches typed message logs retaining arbitrary human-controlled status text."""
        serializer = getattr(event_log, "message_to_log_dict", None)
        self.assertIsNotNone(serializer)

        actual = serializer(message_record())

        self.assertEqual(actual, serialized_message())
        self.assertNotIn("to", actual)
        self.assertNotIn("type", actual)
        self.assertNotIn("01012345678", json.dumps(actual))

    def test_list_response_serializer_includes_safe_envelope_and_serialized_messages(self):
        """Catches list serialization that drops page metadata or bypasses message filtering."""
        serializer = getattr(event_log, "list_to_log_dict", None)
        self.assertIsNotNone(serializer)
        response = MessageListResponse(
            http_status=200,
            status_code="202",
            status_name="success",
            messages=(message_record(),),
            page_size=10,
            page_index=0,
            item_count=1,
            has_more=False,
        )

        actual = serializer(response)

        self.assertEqual(actual, {
            "statusCode": "202",
            "statusName": "success",
            "pageSize": 10,
            "pageIndex": 0,
            "itemCount": 1,
            "hasMore": False,
            "messages": [serialized_message()],
        })

    def test_message_result_serializer_includes_one_filtered_message(self):
        """Catches result serialization that flattens or leaks fields from the internal record."""
        serializer = getattr(event_log, "message_result_to_log_dict", None)
        self.assertIsNotNone(serializer)
        response = MessageResultResponse(
            http_status=200,
            status_code="200",
            status_name="success",
            message=message_record(),
        )

        actual = serializer(response)

        self.assertEqual(actual, {
            "statusCode": "200",
            "statusName": "success",
            "message": serialized_message(),
        })

    def test_all_typed_serializers_pass_the_event_writer_allowlist(self):
        """Catches a serializer adding a field that the durable writer must reject."""
        serializers = (
            getattr(event_log, "send_to_log_dict", None),
            getattr(event_log, "list_to_log_dict", None),
            getattr(event_log, "message_result_to_log_dict", None),
        )
        self.assertNotIn(None, serializers)
        values = (
            SendResponse(202, "request-1", "time", "202", "success"),
            MessageListResponse(
                200, "202", "success", (message_record(),), 10, 0, 1, False
            ),
            MessageResultResponse(200, "200", "success", message_record()),
        )
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            for serializer, value in zip(serializers, values):
                log.write("api_response", response=serializer(value))

        self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 3)

    def test_write_records_schema_sequence_and_seoul_millisecond_timestamp(self):
        """Catches a row that omits the durability schema or uses UTC/seconds timestamps."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        now = lambda: datetime(2026, 8, 14, 3, 4, 5, 678901, tzinfo=timezone.utc)

        with JsonlEventLog(path, now=now) as log:
            log.write("send_response", delivery_id="delivery-1", attempt=1, http_status=202)
            log.write(
                "get_response",
                delivery_id="delivery-1",
                attempt=1,
                state={"delivery_status": "PROCESSING"},
            )

        rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(rows, [
            {
                "schema_version": 1,
                "sequence": 1,
                "logged_at": "2026-08-14T12:04:05.678+09:00",
                "event": "send_response",
                "delivery_id": "delivery-1",
                "attempt": 1,
                "api": None,
                "http_status": 202,
                "request_id": None,
                "message_id": None,
                "response": None,
                "error": None,
                "state": None,
                "summary": None,
                "completed_at": None,
                "result_snapshot": None,
            },
            {
                "schema_version": 1,
                "sequence": 2,
                "logged_at": "2026-08-14T12:04:05.678+09:00",
                "event": "get_response",
                "delivery_id": "delivery-1",
                "attempt": 1,
                "api": None,
                "http_status": None,
                "request_id": None,
                "message_id": None,
                "response": None,
                "error": None,
                "state": {"delivery_status": "PROCESSING"},
                "summary": None,
                "completed_at": None,
                "result_snapshot": None,
            },
        ])

    def test_event_log_error_is_a_runtime_error(self):
        """Catches callers being unable to handle logging failures as operational runtime errors."""
        self.assertTrue(issubclass(EventLogError, RuntimeError))

    def test_mask_digit_runs_keeps_only_the_last_four_digits_of_each_long_run(self):
        """Catches privacy regressions that retain an entire long number in a log value."""
        self.assertEqual(
            mask_digit_runs("recipient 01012345678; code 1234; ref 987654"),
            "recipient *******5678; code 1234; ref **7654",
        )

    def test_safe_error_dict_keeps_short_numeric_status_and_redacts_message(self):
        """Catches helper-generated errors retaining arbitrary human-controlled text."""
        self.assertEqual(
            safe_error_dict(503, "gateway failed for 01012345678"),
            {"status": "503", "message": "redacted"},
        )

    def test_write_rejects_unapproved_keyword_fields(self):
        """Catches an API change that allows recipient or raw-request data into event rows."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            with self.assertRaises(TypeError):
                log.write("send_response", recipient="01012345678")

        parameters = inspect.signature(JsonlEventLog.write).parameters
        self.assertEqual(
            tuple(parameters),
            (
                "self", "event", "delivery_id", "attempt", "api", "http_status",
                "request_id", "message_id", "response", "error", "state", "summary",
                "completed_at", "result_snapshot",
            ),
        )

    def test_create_event_log_uses_seoul_name_and_same_second_collision_suffix(self):
        """Catches a factory that overwrites the same-second log instead of reserving a new file."""
        logs_dir = Path(tempfile.mkdtemp()) / "logs"
        started_at = datetime(2026, 8, 14, 3, 4, 5, tzinfo=timezone.utc)

        first = create_event_log(logs_dir, started_at, now=lambda: started_at)
        second = create_event_log(logs_dir, started_at, now=lambda: started_at)
        first.close()
        second.close()

        self.assertEqual(first.path.name, "delivery_20260814_120405.jsonl")
        self.assertEqual(second.path.name, "delivery_20260814_120405_001.jsonl")

    def test_existing_log_is_never_overwritten(self):
        """Catches opening an existing durable log in append or truncate mode."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        path.write_text('{"existing":true}\n', encoding="utf-8")

        with self.assertRaises(EventLogError):
            JsonlEventLog(path, now=lambda: datetime.now(timezone.utc))

        self.assertEqual(path.read_text(encoding="utf-8"), '{"existing":true}\n')

    def test_fsync_failure_is_a_generic_safe_event_log_error(self):
        """Catches fsync failures that leak an event payload or escape as an OS error."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        log = JsonlEventLog(path, now=lambda: datetime.now(timezone.utc))
        try:
            with patch("sens_mms.event_log.os.fsync", side_effect=OSError("disk full")):
                with self.assertRaises(EventLogError) as raised:
                    log.write("send_response", summary="recipient 01012345678")
            self.assertNotIn("recipient", str(raised.exception))
            self.assertNotIn("01012345678", str(raised.exception))
        finally:
            log.close()

    def test_unsafe_nested_values_are_rejected_before_any_bytes_are_written(self):
        """Catches nested recipient, body, or credential fields bypassing the writer boundary."""
        unsafe_values = {
            "response": {"messages": [{"to": "01012345678"}]},
            "error": {"status": "X", "message": "bad", "headers": {"Authorization": "secret"}},
            "state": {"delivery_status": "FAILED", "error": {"body": "01012345678"}},
            "summary": {"total": 1, "recipient_name": "Kim"},
        }
        for field, value in unsafe_values.items():
            with self.subTest(field=field):
                path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
                log = JsonlEventLog(path, now=lambda: datetime.now(timezone.utc))
                try:
                    with self.assertRaises(EventLogError) as raised:
                        log.write("send_response", **{field: value})
                    self.assertNotIn("01012345678", str(raised.exception))
                    self.assertNotIn("secret", str(raised.exception))
                    self.assertEqual(path.read_text(encoding="utf-8"), "")
                finally:
                    log.close()

    def test_human_messages_are_redacted_but_request_and_message_ids_are_preserved(self):
        """Catches writer-boundary redaction corrupting IDs or retaining human text."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            log.write(
                "get_response",
                response={
                    "requestId": "request-01012345678",
                    "messages": [{"messageId": "message-01012345678", "statusMessage": "failed 01012345678"}],
                },
                error={"status": "503", "message": "recipient 01012345678"},
            )

        row = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(row["response"]["requestId"], "request-01012345678")
        self.assertEqual(row["response"]["messages"][0]["messageId"], "message-01012345678")
        self.assertEqual(row["response"]["messages"][0]["statusMessage"], "redacted")
        self.assertEqual(row["error"]["message"], "redacted")

    def test_real_jsonl_redacts_arbitrary_human_text_and_sender_shaped_status_codes(self):
        """Catches direct writer callers bypassing serializers with sensitive human text."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        sensitive = (
            "010-1234-5678 Alice [개업소연 안내] "
            "opaque-token secret-value"
        )
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            log.write(
                "get_response",
                request_id="request-correlation-unchanged",
                message_id="message-correlation-unchanged",
                response={
                    "statusCode": "0212345678",
                    "messages": [{
                        "requestId": "request-correlation-unchanged",
                        "messageId": "message-correlation-unchanged",
                        "statusCode": "0212345678",
                        "statusMessage": sensitive,
                    }],
                },
                error={"status": "0212345678", "message": sensitive},
                state={
                    "delivery_status": "FAILED",
                    "error": {"status": "0212345678", "message": sensitive},
                },
            )

        row = json.loads(path.read_text(encoding="utf-8"))
        rendered = json.dumps(row, ensure_ascii=False)
        for forbidden in (
            "010-1234-5678",
            "Alice",
            "[개업소연 안내]",
            "opaque-token",
            "secret-value",
            "0212345678",
        ):
            self.assertNotIn(forbidden, rendered)
        self.assertEqual(row["request_id"], "request-correlation-unchanged")
        self.assertEqual(row["message_id"], "message-correlation-unchanged")
        self.assertEqual(row["response"]["statusCode"], "UNKNOWN")
        self.assertEqual(row["response"]["messages"][0]["statusMessage"], "redacted")
        self.assertEqual(row["error"], {"status": "UNKNOWN", "message": "redacted"})
        self.assertEqual(
            row["state"]["error"],
            {"status": "UNKNOWN", "message": "redacted"},
        )

    def test_raw_typed_serializer_redacts_every_nonempty_status_message_and_bad_code(self):
        """Catches typed serializers trusting raw API status text or long status codes."""
        sensitive = (
            "010-1234-5678 Alice [개업소연 안내] "
            "opaque-token secret-value"
        )
        record = MessageRecord(
            request_id="request-correlation-unchanged",
            message_id="message-correlation-unchanged",
            message_type="MMS",
            to="01012345678",
            request_time="2026-08-13 10:00:00",
            complete_time="2026-08-13 10:00:30",
            telco_code="SKT",
            status="COMPLETED",
            status_code="0212345678",
            status_name="fail",
            status_message=sensitive,
        )

        actual = event_log.message_to_log_dict(record)
        rendered = json.dumps(actual, ensure_ascii=False)

        for forbidden in (
            "010-1234-5678",
            "Alice",
            "[개업소연 안내]",
            "opaque-token",
            "secret-value",
            "0212345678",
        ):
            self.assertNotIn(forbidden, rendered)
        self.assertEqual(actual["requestId"], "request-correlation-unchanged")
        self.assertEqual(actual["messageId"], "message-correlation-unchanged")
        self.assertEqual(actual["statusCode"], "UNKNOWN")
        self.assertEqual(actual["statusMessage"], "redacted")

    def test_writer_sanitizes_every_api_field_by_meaning_not_only_by_key(self):
        """Catches allowlisted response keys retaining adversarial API-controlled values."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        secret = "010-1234-5678 Alice body-fragment opaque-secret"
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            log.write(
                "api_response",
                response={
                    "statusCode": "0212345678",
                    "statusName": secret,
                    "requestId": "request-correlation-unchanged",
                    "requestTime": secret,
                    "pageSize": secret,
                    "pageIndex": [secret],
                    "itemCount": True,
                    "hasMore": 1,
                    "messages": [
                        {
                            "messageId": "message-correlation-unchanged",
                            "completeTime": secret,
                            "telcoCode": secret,
                            "status": secret,
                            "statusMessage": 1012345678,
                        },
                        {"statusMessage": [secret]},
                        {"statusMessage": {"statusName": secret}},
                    ],
                    "message": {
                        "requestId": "nested-request-unchanged",
                        "messageId": "nested-message-unchanged",
                        "statusName": secret,
                    },
                },
            )

        response = json.loads(path.read_text(encoding="utf-8"))["response"]
        self.assertNotIn(secret, json.dumps(response, ensure_ascii=False))
        self.assertNotIn("1012345678", json.dumps(response, ensure_ascii=False))
        self.assertEqual(response["statusCode"], "UNKNOWN")
        self.assertEqual(response["statusName"], "")
        self.assertEqual(response["requestId"], "request-correlation-unchanged")
        self.assertEqual(response["requestTime"], "")
        self.assertIsNone(response["pageSize"])
        self.assertIsNone(response["pageIndex"])
        self.assertIsNone(response["itemCount"])
        self.assertIsNone(response["hasMore"])
        self.assertEqual(response["messages"][0]["messageId"], "message-correlation-unchanged")
        self.assertEqual(response["messages"][0]["completeTime"], "")
        self.assertEqual(response["messages"][0]["telcoCode"], "")
        self.assertEqual(response["messages"][0]["status"], "")
        self.assertEqual(
            [item["statusMessage"] for item in response["messages"]],
            ["redacted", "redacted", "redacted"],
        )
        self.assertEqual(response["message"]["requestId"], "nested-request-unchanged")
        self.assertEqual(response["message"]["messageId"], "nested-message-unchanged")
        self.assertEqual(response["message"]["statusName"], "")

    def test_typed_serializers_apply_the_same_adversarial_field_matrix(self):
        """Catches typed serializers bypassing durable field-specific sanitizers."""
        secret = "010-1234-5678 Alice body-fragment opaque-secret"
        record = MessageRecord(
            request_id="request-correlation-unchanged",
            message_id="message-correlation-unchanged",
            message_type="MMS",
            to="01012345678",
            request_time=secret,
            complete_time=secret,
            telco_code=secret,
            status=secret,
            status_code="0212345678",
            status_name=secret,
            status_message={"secret": secret},
        )
        values = (
            event_log.send_to_log_dict(SendResponse(
                202,
                "send-request-unchanged",
                secret,
                "0212345678",
                secret,
            )),
            event_log.message_to_log_dict(record),
            event_log.list_to_log_dict(MessageListResponse(
                200,
                "0212345678",
                secret,
                (record,),
                secret,
                [secret],
                True,
                1,
            )),
            event_log.message_result_to_log_dict(MessageResultResponse(
                200,
                "0212345678",
                secret,
                record,
            )),
        )

        rendered = json.dumps(values, ensure_ascii=False)
        self.assertNotIn(secret, rendered)
        self.assertNotIn("0212345678", rendered)
        self.assertEqual(values[0], {
            "requestId": "send-request-unchanged",
            "requestTime": "",
            "statusCode": "UNKNOWN",
            "statusName": "",
        })
        self.assertEqual(values[1]["requestId"], "request-correlation-unchanged")
        self.assertEqual(values[1]["messageId"], "message-correlation-unchanged")
        self.assertEqual(values[1]["statusMessage"], "redacted")
        self.assertEqual(values[2]["pageSize"], None)
        self.assertEqual(values[2]["pageIndex"], None)
        self.assertEqual(values[2]["itemCount"], None)
        self.assertEqual(values[2]["hasMore"], None)
        self.assertEqual(values[3]["message"]["statusName"], "")

    def test_documented_reserved_envelope_and_etc_telco_values_are_preserved(self):
        """Catches field sanitizers blanking explicitly approved documented values."""
        record = message_record()
        record = MessageRecord(
            request_id=record.request_id,
            message_id=record.message_id,
            message_type=record.message_type,
            to=record.to,
            request_time=record.request_time,
            complete_time=record.complete_time,
            telco_code="ETC",
            status=record.status,
            status_code=record.status_code,
            status_name=record.status_name,
            status_message=record.status_message,
        )
        response = MessageListResponse(
            http_status=200,
            status_code="202",
            status_name="reserved",
            messages=(record,),
            page_size=None,
            page_index=None,
            item_count=None,
            has_more=None,
        )

        actual = event_log.list_to_log_dict(response)

        self.assertEqual(actual["statusName"], "reserved")
        self.assertEqual(actual["messages"][0]["telcoCode"], "ETC")
        self.assertEqual(
            (actual["pageSize"], actual["pageIndex"], actual["itemCount"], actual["hasMore"]),
            (None, None, None, None),
        )

    def test_naive_event_times_are_attached_to_seoul_without_host_conversion(self):
        """Catches naive event timestamps and filenames being interpreted as host-local time."""
        class HostSensitiveNaive(datetime):
            def astimezone(self, tz=None):
                return datetime(2026, 8, 13, 12, 4, 5, 678901, tzinfo=timezone.utc).astimezone(tz)

        base = Path(tempfile.mkdtemp())
        naive = HostSensitiveNaive(2026, 8, 14, 3, 4, 5, 678901)
        log = create_event_log(base / "logs", naive, now=lambda: naive)
        log.write("standalone")
        log.close()

        row = json.loads(log.path.read_text(encoding="utf-8"))
        self.assertEqual(log.path.name, "delivery_20260814_030405.jsonl")
        self.assertEqual(row["logged_at"], "2026-08-14T03:04:05.678+09:00")

    def test_empty_human_messages_remain_empty(self):
        """Catches redaction inventing an error message when the upstream field is empty."""
        record = message_record()
        record = MessageRecord(
            request_id=record.request_id,
            message_id=record.message_id,
            message_type=record.message_type,
            to=record.to,
            request_time=record.request_time,
            complete_time=record.complete_time,
            telco_code=record.telco_code,
            status=record.status,
            status_code=record.status_code,
            status_name=record.status_name,
            status_message="",
        )

        self.assertEqual(event_log.message_to_log_dict(record)["statusMessage"], "")
        self.assertEqual(safe_error_dict("503", "")["message"], "")

    def test_persistence_failure_poisoning_blocks_later_writes_without_appending(self):
        """Catches retrying a possibly persisted sequence after an fsync failure."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        log = JsonlEventLog(path, now=lambda: datetime.now(timezone.utc))
        try:
            with patch("sens_mms.event_log.os.fsync", side_effect=[OSError("disk full"), None]):
                with self.assertRaises(EventLogError):
                    log.write("send_response")
                checkpoint = path.read_text(encoding="utf-8")
                with self.assertRaises(EventLogError):
                    log.write("get_response")
            self.assertEqual(path.read_text(encoding="utf-8"), checkpoint)
        finally:
            log.close()

    def test_state_allows_a_null_error(self):
        """Catches rejecting a valid state snapshot before any delivery error is known."""
        path = Path(tempfile.mkdtemp()) / "delivery.jsonl"
        with JsonlEventLog(path, now=lambda: datetime.now(timezone.utc)) as log:
            log.write("state_checkpoint", state={"delivery_status": "PROCESSING", "error": None})

        self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["state"]["error"], None)
