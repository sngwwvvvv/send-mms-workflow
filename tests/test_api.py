import base64
import json
import socket
import tempfile
import unittest
from pathlib import Path

from sens_mms.api import (
    AmbiguousPostOutcome,
    ApiResponse,
    ExplicitApiFailure,
    SensClient,
    TransientLookupError,
    UrlLibTransport,
    make_signature,
)


class RecordingTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, headers, body, timeout):
        self.calls.append((method, url, dict(headers), body, timeout))
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def response(status, payload):
    return ApiResponse(status, json.dumps(payload).encode("utf-8"), {})


def client(transport):
    return SensClient(
        access_key="access",
        secret_key="secret",
        service_id="svc",
        from_number="0212345678",
        transport=transport,
        timestamp_ms=lambda: "1700000000000",
    )


def official_message(**overrides):
    message = {
        "requestId": "request-1",
        "messageId": "message-1",
        "requestTime": "2026-08-13 10:00:00",
        "contentType": "COMM",
        "type": "MMS",
        "subject": "unsafe subject",
        "content": "unsafe body",
        "countryCode": "82",
        "from": "0212345678",
        "to": "01012345678",
        "completeTime": "2026-08-13 10:00:30",
        "telcoCode": "SKT",
        "files": [{"fileId": "file-1", "name": "image.jpg"}],
        "status": "COMPLETED",
        "statusCode": "0",
        "statusName": "success",
        "statusMessage": "delivered",
        "unexpected": {"secret": "must not survive"},
    }
    message.update(overrides)
    return message


class ApiTests(unittest.TestCase):
    def test_signature_matches_official_string_to_sign(self):
        signature = make_signature(
            "POST",
            "/sms/v2/services/svc/messages",
            "1700000000000",
            "access",
            "secret",
        )
        self.assertEqual(signature, "7lLIUF/vg70uc1AkCmekGjUxqT5ekiPPnwKqwZrI6qI=")

    def test_send_one_builds_one_recipient_without_subject_and_keeps_file_order(self):
        transport = RecordingTransport([response(202, {
            "requestId": "request-1",
            "requestTime": "2026-08-13T10:00:00.000",
            "statusCode": "202",
            "statusName": "success",
        })])

        actual = client(transport).send_one("01012345678", ["file-1", "file-2"])

        self.assertEqual(actual.http_status, 202)
        self.assertEqual(actual.request_id, "request-1")
        self.assertEqual(actual.request_time, "2026-08-13T10:00:00.000")
        self.assertEqual(actual.status_code, "202")
        self.assertEqual(actual.status_name, "success")
        method, url, headers, body, _ = transport.calls[0]
        payload = json.loads(body)
        self.assertEqual(method, "POST")
        self.assertTrue(url.endswith("/sms/v2/services/svc/messages"))
        self.assertEqual(payload["messages"], [{"to": "01012345678"}])
        self.assertNotIn("subject", payload)
        self.assertEqual(payload["files"], [{"fileId": "file-1"}, {"fileId": "file-2"}])
        self.assertEqual(headers["x-ncp-apigw-timestamp"], "1700000000000")

    def test_list_query_is_part_of_signed_uri(self):
        transport = RecordingTransport([response(200, {
            "statusCode": "202", "statusName": "success", "messages": []
        })])

        actual = client(transport).list_by_request("request with space")

        self.assertEqual(actual.http_status, 200)
        self.assertEqual(actual.status_code, "202")
        self.assertEqual(actual.status_name, "success")
        self.assertEqual(actual.messages, ())

        _, url, headers, _, _ = transport.calls[0]
        self.assertTrue(url.endswith("?requestId=request+with+space"))
        self.assertEqual(
            headers["x-ncp-apigw-signature-v2"],
            make_signature(
                "GET",
                "/sms/v2/services/svc/messages?requestId=request+with+space",
                "1700000000000",
                "access",
                "secret",
            ),
        )

    def test_upload_encodes_raw_jpeg_without_data_url_prefix(self):
        path = Path(tempfile.mkdtemp()) / "image.jpg"
        path.write_bytes(b"\xff\xd8\xff\xd9")
        transport = RecordingTransport([response(200, {
            "fileId": "file-1",
            "createTime": "2026-08-13T10:00:00.000",
            "expireTime": "2026-08-15T10:00:00.000",
        })])

        self.assertEqual(client(transport).upload_file(path), "file-1")

        payload = json.loads(transport.calls[0][3])
        self.assertEqual(payload, {"fileName": "image.jpg", "fileBody": "/9j/2Q=="})

    def test_upload_bytes_encodes_the_exact_approved_bytes(self):
        transport = RecordingTransport([response(200, {"fileId": "file-1"})])

        actual = client(transport).upload_bytes(
            "mms_01_intro.jpg", b"approved-image-bytes"
        )

        self.assertEqual(actual, "file-1")
        payload = json.loads(transport.calls[0][3])
        self.assertEqual(payload["fileName"], "mms_01_intro.jpg")
        self.assertEqual(
            payload["fileBody"],
            base64.b64encode(b"approved-image-bytes").decode("ascii"),
        )

    def test_upload_rejects_non_mapping_json_and_invalid_file_id_safely(self):
        marker = "UPLOAD_MARKER_01012345678"
        malformed = ([marker], 7, marker, None, {"fileId": [marker]}, {"fileId": ""})
        for payload in malformed:
            with self.subTest(payload_type=type(payload).__name__, payload=payload):
                transport = RecordingTransport([response(200, payload)])

                with self.assertRaises(ExplicitApiFailure) as raised:
                    client(transport).upload_bytes("image.jpg", b"approved")

                self.assertEqual(raised.exception.status, "INVALID_RESPONSE")
                self.assertNotIn(marker, repr(raised.exception))

    def test_network_loss_during_send_is_ambiguous(self):
        transport = RecordingTransport([socket.timeout("timed out")])

        with self.assertRaises(AmbiguousPostOutcome):
            client(transport).send_one("01012345678", ["file-1", "file-2"])

    def test_two_xx_send_non_mapping_json_is_ambiguous_without_raw_marker(self):
        marker = "SEND_TOP_LEVEL_MARKER_01012345678"
        for payload in ([marker], 7, marker, None):
            with self.subTest(payload_type=type(payload).__name__):
                transport = RecordingTransport([response(200, payload)])

                with self.assertRaises(AmbiguousPostOutcome) as raised:
                    client(transport).send_one(
                        "01012345678", ["file-1", "file-2"]
                    )

                self.assertNotIn(marker, repr(raised.exception))

    def test_two_xx_send_missing_or_non_exact_status_code_is_ambiguous(self):
        marker = "SEND_STATUS_MARKER_01012345678"
        malformed = (
            {},
            {"statusCode": None},
            {"statusCode": 202},
            {"statusCode": [marker]},
            {"statusCode": {"secret": marker}},
        )
        for payload in malformed:
            with self.subTest(payload=payload):
                transport = RecordingTransport([response(200, payload)])

                with self.assertRaises(AmbiguousPostOutcome) as raised:
                    client(transport).send_one(
                        "01012345678", ["file-1", "file-2"]
                    )

                self.assertNotIn(marker, repr(raised.exception))

    def test_two_xx_send_malformed_string_status_code_is_ambiguous(self):
        malformed = ("", " ", "abc", "20", "0202", "２０２")
        for status_code in malformed:
            with self.subTest(status_code=repr(status_code)):
                transport = RecordingTransport([response(200, {
                    "statusCode": status_code,
                    "statusName": "fail",
                    "requestId": "request-that-must-not-be-trusted",
                })])

                with self.assertRaises(AmbiguousPostOutcome) as raised:
                    client(transport).send_one(
                        "01012345678", ["file-1", "file-2"]
                    )

                self.assertEqual(
                    str(raised.exception),
                    "message POST returned an invalid response",
                )

    def test_accepted_send_missing_or_non_exact_request_id_is_ambiguous(self):
        marker = "SEND_ID_MARKER_01012345678"
        for request_id in (None, "", [marker], {"secret": marker}):
            with self.subTest(request_id=request_id):
                payload = {
                    "statusCode": "202",
                    "statusName": "success",
                    "requestTime": "2026-08-13T10:00:00.000",
                }
                if request_id is not None:
                    payload["requestId"] = request_id
                transport = RecordingTransport([response(202, payload)])

                with self.assertRaises(AmbiguousPostOutcome) as raised:
                    client(transport).send_one(
                        "01012345678", ["file-1", "file-2"]
                    )

                self.assertNotIn(marker, repr(raised.exception))

    def test_http_failure_with_unreadable_body_is_explicit_not_ambiguous(self):
        transport = RecordingTransport([ApiResponse(400, b"not-json", {})])

        with self.assertRaises(ExplicitApiFailure) as raised:
            client(transport).send_one("01012345678", ["file-1", "file-2"])

        self.assertEqual(raised.exception.status, "400")
        self.assertEqual(raised.exception.http_status, 400)
        self.assertEqual(raised.exception.response, {})

    def test_http_failure_retains_no_api_controlled_response_fields(self):
        transport = RecordingTransport([response(400, {
            "statusCode": "400",
            "statusName": "invalid request",
            "requestId": "request-1",
            "requestTime": "2026-08-13T10:00:00.000",
            "content": "unsafe body",
            "from": "0212345678",
            "files": [{"fileId": "file-1"}],
            "extra": "unsafe",
        })])

        with self.assertRaises(ExplicitApiFailure) as raised:
            client(transport).send_one("01012345678", ["file-1", "file-2"])

        self.assertEqual(raised.exception.status, "400")
        self.assertEqual(raised.exception.message, "HTTP request failed")
        self.assertEqual(raised.exception.http_status, 400)
        self.assertEqual(raised.exception.response, {})
        self.assertNotIn("unsafe body", str(raised.exception))
        self.assertNotIn("0212345678", str(raised.exception))

    def test_http_failure_never_retains_api_controlled_sensitive_error_text(self):
        sensitive = "recipient 01012345678 rejected: unsafe content"
        for field in ("message", "errorMessage", "statusName"):
            with self.subTest(field=field):
                payload = {
                    "statusCode": "400",
                    "statusName": "fail",
                    "requestId": "request-1",
                    "requestTime": "2026-08-13T10:00:00.000",
                    field: sensitive,
                }
                transport = RecordingTransport([response(400, payload)])

                with self.assertRaises(ExplicitApiFailure) as raised:
                    client(transport).send_one(
                        "01012345678", ["file-1", "file-2"]
                    )

                failure = raised.exception
                self.assertEqual(failure.status, "400")
                self.assertEqual(failure.message, "HTTP request failed")
                self.assertEqual(failure.http_status, 400)
                self.assertEqual(failure.response, {})
                retained = " ".join((
                    str(failure), failure.status, failure.message,
                    json.dumps(failure.response),
                ))
                self.assertNotIn(sensitive, retained)
                self.assertNotIn("01012345678", retained)
                self.assertNotIn("unsafe content", retained)

    def test_exact_three_digit_non_202_send_response_is_explicit_and_safe(self):
        sensitive = "recipient 01012345678 rejected: unsafe content"
        transport = RecordingTransport([response(200, {
            "statusCode": "400",
            "statusName": sensitive,
            "requestId": "request-1",
            "requestTime": "2026-08-13T10:00:00.000",
        })])

        with self.assertRaises(ExplicitApiFailure) as raised:
            client(transport).send_one(
                "01012345678", ["file-1", "file-2"]
            )

        failure = raised.exception
        self.assertEqual(failure.status, "INVALID_RESPONSE")
        self.assertEqual(failure.message, "message request was not accepted")
        retained = " ".join((
            str(failure), failure.status, failure.message,
            json.dumps(failure.response),
        ))
        self.assertNotIn(sensitive, retained)
        self.assertNotIn("01012345678", retained)

    def test_safe_failure_response_rejects_arbitrary_compact_status_name(self):
        compact_sensitive = "recipient01012345678"
        transport = RecordingTransport([response(400, {
            "statusCode": "400",
            "statusName": compact_sensitive,
            "requestId": "request-1",
            "requestTime": "2026-08-13T10:00:00.000",
        })])

        with self.assertRaises(ExplicitApiFailure) as raised:
            client(transport).send_one("01012345678", ["file-1", "file-2"])

        self.assertNotIn("statusName", raised.exception.response)
        self.assertNotIn(compact_sensitive, str(raised.exception))
        self.assertNotIn(
            compact_sensitive, json.dumps(raised.exception.response)
        )

    def test_failure_never_retains_api_controlled_metadata_values(self):
        unsafe_values = (
            "RECIPIENT_01012345678_REJECTED",
            "01012345678",
            "01012345678",
        )
        payload = {
            "statusCode": unsafe_values[0],
            "statusName": "fail",
            "requestId": unsafe_values[1],
            "requestTime": unsafe_values[2],
        }
        for http_status, expected_type in (
            (400, ExplicitApiFailure),
            (200, AmbiguousPostOutcome),
        ):
            with self.subTest(http_status=http_status):
                transport = RecordingTransport([response(http_status, payload)])

                with self.assertRaises(expected_type) as raised:
                    client(transport).send_one(
                        "01012345678", ["file-1", "file-2"]
                    )

                failure = raised.exception
                if isinstance(failure, ExplicitApiFailure):
                    self.assertEqual(failure.status, "400")
                    self.assertEqual(failure.message, "HTTP request failed")
                    self.assertEqual(failure.response, {})
                else:
                    self.assertEqual(
                        str(failure),
                        "message POST returned an invalid response",
                    )
                retained = " ".join((
                    str(failure), repr(vars(failure)),
                    json.dumps(getattr(failure, "response", {})),
                ))
                for unsafe in unsafe_values:
                    self.assertNotIn(unsafe, retained)

    def test_get_message_returns_the_single_documented_message(self):
        message = official_message(
            completeTime="", telcoCode="", status="PROCESSING",
            statusCode="", statusName="", statusMessage="",
        )
        transport = RecordingTransport([response(200, {
            "statusCode": "200", "statusName": "success", "messages": [message]
        })])

        actual = client(transport).get_message("message-1")

        self.assertEqual(actual.http_status, 200)
        self.assertEqual(actual.status_code, "200")
        self.assertEqual(actual.status_name, "success")
        self.assertEqual(actual.message.request_id, "request-1")
        self.assertEqual(actual.message.message_id, "message-1")
        self.assertEqual(actual.message.to, "01012345678")
        self.assertEqual(actual.message.status, "PROCESSING")
        for unsafe_attribute in (
            "content", "subject", "from", "files", "file_id", "unexpected"
        ):
            self.assertFalse(hasattr(actual.message, unsafe_attribute))
        self.assertTrue(transport.calls[0][1].endswith(
            "/sms/v2/services/svc/messages/message-1"
        ))

    def test_time_recipient_lookup_encodes_all_identifying_filters(self):
        transport = RecordingTransport([response(200, {
            "statusCode": "202", "statusName": "success", "messages": []
        })])

        client(transport).list_by_time_and_recipient(
            "2026-08-13 09:59:59", "2026-08-13 10:00:30", "01012345678"
        )

        url = transport.calls[0][1]
        self.assertIn("requestStartTime=2026-08-13+09%3A59%3A59", url)
        self.assertIn("requestEndTime=2026-08-13+10%3A00%3A30", url)
        self.assertIn("to=01012345678", url)

    def test_list_returns_typed_allowlisted_messages_and_page_envelope(self):
        transport = RecordingTransport([response(200, {
            "statusCode": "202",
            "statusName": "success",
            "messages": [official_message()],
            "pageSize": 10,
            "pageIndex": 0,
            "itemCount": 1,
            "hasMore": False,
            "content": "unsafe envelope body",
            "extra": {"secret": "must not survive"},
        })])

        actual = client(transport).list_by_request("request-1")

        self.assertEqual(actual.http_status, 200)
        self.assertEqual(actual.messages[0].message_id, "message-1")
        self.assertEqual(actual.page_size, 10)
        self.assertEqual(actual.page_index, 0)
        self.assertEqual(actual.item_count, 1)
        self.assertIs(actual.has_more, False)
        self.assertFalse(hasattr(actual, "content"))
        self.assertFalse(hasattr(actual, "extra"))
        self.assertFalse(hasattr(actual.messages[0], "content"))
        self.assertFalse(hasattr(actual.messages[0], "files"))

    def test_list_preserves_none_for_absent_optional_page_fields(self):
        transport = RecordingTransport([response(200, {
            "statusCode": "202", "statusName": "success", "messages": []
        })])

        actual = client(transport).list_by_request("request-1")

        self.assertIsNone(actual.page_size)
        self.assertIsNone(actual.page_index)
        self.assertIsNone(actual.item_count)
        self.assertIsNone(actual.has_more)

    def test_list_rejects_a_non_list_messages_field(self):
        transport = RecordingTransport([response(200, {
            "statusCode": "202", "statusName": "success", "messages": {}
        })])

        with self.assertRaisesRegex(TransientLookupError, "omitted messages"):
            client(transport).list_by_request("request-1")

    def test_get_operations_reject_non_mapping_json_with_fixed_transient_error(self):
        marker = "LOOKUP_TOP_LEVEL_MARKER_01012345678"
        operations = (
            lambda sens: sens.list_by_request("request-1"),
            lambda sens: sens.get_message("message-1"),
        )
        for operation in operations:
            for payload in ([marker], 7, marker, None):
                with self.subTest(operation=operation, payload=payload):
                    transport = RecordingTransport([response(200, payload)])

                    with self.assertRaises(TransientLookupError) as raised:
                        operation(client(transport))

                    self.assertNotIn(marker, repr(raised.exception))

    def test_list_and_get_reject_non_string_message_correlation_ids(self):
        marker = "MESSAGE_ID_MARKER_01012345678"
        operations = (
            (
                "list",
                "202",
                lambda sens: sens.list_by_request("request-1"),
            ),
            (
                "get",
                "200",
                lambda sens: sens.get_message("message-1"),
            ),
        )
        for operation_name, envelope_code, operation in operations:
            for field in ("requestId", "messageId"):
                for invalid in ([marker], {"secret": marker}, ""):
                    with self.subTest(
                        operation=operation_name, field=field, invalid=invalid
                    ):
                        record = official_message(**{field: invalid})
                        transport = RecordingTransport([response(200, {
                            "statusCode": envelope_code,
                            "statusName": "success",
                            "messages": [record],
                        })])

                        with self.assertRaises(TransientLookupError) as raised:
                            operation(client(transport))

                        self.assertNotIn(marker, repr(raised.exception))

    def test_get_rejects_zero_or_multiple_messages(self):
        malformed = ([], [official_message(), official_message(messageId="message-2")])
        for messages in malformed:
            with self.subTest(count=len(messages)):
                transport = RecordingTransport([response(200, {
                    "statusCode": "200", "statusName": "success", "messages": messages
                })])
                with self.assertRaisesRegex(TransientLookupError, "one message"):
                    client(transport).get_message("message-1")

    def test_urllib_transport_converts_real_http_boundary_to_api_response(self):
        class HttpResponse:
            status = 202
            headers = {"Date": "Thu, 13 Aug 2026 01:00:00 GMT"}

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def read(self):
                return b'{"statusCode":"202"}'

        seen = {}

        def opener(request, timeout):
            seen["request"] = request
            seen["timeout"] = timeout
            return HttpResponse()

        transport = UrlLibTransport(opener=opener)

        actual = transport.request(
            "POST",
            "https://example.test/messages",
            {"Content-Type": "application/json"},
            b"{}",
            12.5,
        )

        self.assertEqual(actual, ApiResponse(
            202,
            b'{"statusCode":"202"}',
            {"Date": "Thu, 13 Aug 2026 01:00:00 GMT"},
        ))
        self.assertEqual(seen["request"].method, "POST")
        self.assertEqual(seen["request"].full_url, "https://example.test/messages")
        self.assertEqual(seen["request"].data, b"{}")
        self.assertEqual(seen["timeout"], 12.5)


if __name__ == "__main__":
    unittest.main()
