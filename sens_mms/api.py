from __future__ import annotations

import base64
import hashlib
import hmac
import json
import socket
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from .inputs import MESSAGE_BODY


@dataclass(frozen=True)
class ApiResponse:
    status: int
    body: bytes
    headers: Mapping[str, str]


@dataclass(frozen=True)
class SendResponse:
    http_status: int
    request_id: str
    request_time: str
    status_code: str
    status_name: str


@dataclass(frozen=True)
class MessageRecord:
    request_id: str
    message_id: str
    to: str
    request_time: str
    complete_time: str
    telco_code: str
    status: str
    status_code: str
    status_name: str
    status_message: str
    message_type: str = ""


@dataclass(frozen=True)
class MessageListResponse:
    http_status: int
    status_code: str
    status_name: str
    messages: tuple[MessageRecord, ...]
    page_size: int | None
    page_index: int | None
    item_count: int | None
    has_more: bool | None


@dataclass(frozen=True)
class MessageResultResponse:
    http_status: int
    status_code: str
    status_name: str
    message: MessageRecord


class Transport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        headers: Mapping[str, str],
        body: bytes | None,
        timeout: float,
    ) -> ApiResponse: ...


class UrlLibTransport:
    def __init__(self, opener=urlopen):
        self._opener = opener

    def request(self, method, url, headers, body, timeout):
        request = Request(url, data=body, headers=dict(headers), method=method)
        try:
            with self._opener(request, timeout=timeout) as response:
                return ApiResponse(
                    int(response.status),
                    response.read(),
                    dict(response.headers.items()),
                )
        except HTTPError as response:
            return ApiResponse(
                int(response.code),
                response.read(),
                dict(response.headers.items()) if response.headers else {},
            )


class SensApiError(RuntimeError):
    pass


class ExplicitApiFailure(SensApiError):
    def __init__(
        self,
        status,
        message,
        *,
        http_status=None,
        response=None,
    ):
        self.status = str(status)
        self.message = str(message)
        self.http_status = http_status if type(http_status) is int else None
        self.response = {}
        super().__init__(f"SENS request failed ({self.status}): {self.message}")


class AmbiguousPostOutcome(SensApiError):
    pass


class TransientLookupError(SensApiError):
    def __init__(self, message: str, *, http_status: int | None = None):
        self.http_status = http_status if type(http_status) is int else None
        super().__init__(message)


def make_signature(
    method: str,
    uri: str,
    timestamp: str,
    access_key: str,
    secret_key: str,
) -> str:
    message = f"{method} {uri}\n{timestamp}\n{access_key}".encode("utf-8")
    digest = hmac.new(secret_key.encode("utf-8"), message, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


class SensClient:
    def __init__(
        self,
        *,
        access_key: str,
        secret_key: str,
        service_id: str,
        from_number: str,
        transport: Transport,
        timestamp_ms: Callable[[], str],
        timeout: float = 15.0,
        base_url: str = "https://sens.apigw.ntruss.com",
    ):
        self._access_key = access_key
        self._secret_key = secret_key
        self._service_id = service_id
        self._from_number = from_number
        self._transport = transport
        self._timestamp_ms = timestamp_ms
        self._timeout = timeout
        self._base_url = base_url.rstrip("/")

    @property
    def _messages_uri(self) -> str:
        service_id = quote(self._service_id, safe=":")
        return f"/sms/v2/services/{service_id}/messages"

    @property
    def _files_uri(self) -> str:
        service_id = quote(self._service_id, safe=":")
        return f"/sms/v2/services/{service_id}/files"

    def _request_json(
        self,
        method: str,
        uri: str,
        payload: dict | None = None,
        *,
        ambiguous_message_post: bool = False,
    ) -> tuple[int, dict]:
        timestamp = str(self._timestamp_ms())
        headers = {
            "x-ncp-apigw-timestamp": timestamp,
            "x-ncp-iam-access-key": self._access_key,
            "x-ncp-apigw-signature-v2": make_signature(
                method, uri, timestamp, self._access_key, self._secret_key
            ),
            "Content-Type": "application/json",
        }
        body = None
        if payload is not None:
            body = json.dumps(
                payload, ensure_ascii=False, separators=(",", ":")
            ).encode("utf-8")
        try:
            response = self._transport.request(
                method, self._base_url + uri, headers, body, self._timeout
            )
        except (TimeoutError, socket.timeout, ConnectionError, OSError) as exc:
            if ambiguous_message_post:
                raise AmbiguousPostOutcome(
                    "message POST response was not received"
                ) from exc
            if method == "GET":
                raise TransientLookupError(
                    "SENS lookup response was not received"
                ) from exc
            raise ExplicitApiFailure(
                "NETWORK_ERROR", "attachment upload was not confirmed"
            ) from exc
        try:
            data = json.loads(response.body.decode("utf-8")) if response.body else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            if response.status < 200 or response.status >= 300:
                if method == "GET":
                    raise TransientLookupError(
                        "SENS lookup was not successful",
                        http_status=response.status,
                    ) from exc
                raise ExplicitApiFailure(
                    str(response.status),
                    "HTTP request failed with an unreadable response",
                    http_status=response.status,
                ) from exc
            if ambiguous_message_post:
                raise AmbiguousPostOutcome(
                    "message POST returned an unreadable response"
                ) from exc
            if method == "GET":
                raise TransientLookupError(
                    "SENS lookup returned an unreadable response"
                ) from exc
            raise ExplicitApiFailure(
                "INVALID_RESPONSE", "attachment response was unreadable"
            ) from exc
        if response.status < 200 or response.status >= 300:
            if method == "GET":
                raise TransientLookupError(
                    "SENS lookup was not successful",
                    http_status=response.status,
                )
            raise ExplicitApiFailure(
                response.status,
                "HTTP request failed",
                http_status=response.status,
                response=data,
            )
        if type(data) is not dict:
            if ambiguous_message_post:
                raise AmbiguousPostOutcome(
                    "message POST returned an invalid response"
                )
            if method == "GET":
                raise TransientLookupError(
                    "SENS lookup returned an invalid response"
                )
            raise ExplicitApiFailure(
                "INVALID_RESPONSE", "attachment response was invalid"
            )
        return response.status, data

    def upload_file(self, path: Path) -> str:
        path = Path(path)
        return self.upload_bytes(path.name, path.read_bytes())

    def upload_bytes(self, file_name: str, raw: bytes) -> str:
        _, data = self._request_json(
            "POST",
            self._files_uri,
            {
                "fileName": file_name,
                "fileBody": base64.b64encode(raw).decode("ascii"),
            },
        )
        file_id = data.get("fileId")
        if type(file_id) is not str or not file_id:
            raise ExplicitApiFailure("INVALID_RESPONSE", "fileId was not returned")
        return file_id

    def _send_message(self, payload: dict) -> SendResponse:
        http_status, data = self._request_json(
            "POST",
            self._messages_uri,
            payload,
            ambiguous_message_post=True,
        )
        status_code = data.get("statusCode")
        if (
            type(status_code) is not str
            or len(status_code) != 3
            or not status_code.isascii()
            or not status_code.isdigit()
        ):
            raise AmbiguousPostOutcome(
                "message POST returned an invalid response"
            )
        if status_code != "202":
            raise ExplicitApiFailure(
                "INVALID_RESPONSE",
                "message request was not accepted",
                http_status=http_status,
            )
        request_id = data.get("requestId")
        if type(request_id) is not str or not request_id:
            raise AmbiguousPostOutcome(
                "message POST acceptance could not be confirmed"
            )
        return SendResponse(
            http_status=http_status,
            request_id=request_id,
            request_time=_exact_string(data.get("requestTime")),
            status_code=status_code,
            status_name=_exact_string(data.get("statusName")),
        )

    def send_mms(
        self,
        to: str,
        file_ids: Sequence[str],
        *,
        content_type: str,
    ) -> SendResponse:
        if type(content_type) is not str or content_type not in {"COMM", "AD"}:
            raise ExplicitApiFailure("INVALID_REQUEST", "content type is invalid")
        return self._send_message(
            {
                "type": "MMS",
                "contentType": content_type,
                "countryCode": "82",
                "from": self._from_number,
                "content": "",
                "messages": [{"to": to}],
                "files": [{"fileId": file_id} for file_id in file_ids],
            }
        )

    def send_lms(
        self,
        to: str,
        *,
        content_type: str,
    ) -> SendResponse:
        if type(content_type) is not str or content_type not in {"COMM", "AD"}:
            raise ExplicitApiFailure("INVALID_REQUEST", "content type is invalid")
        return self._send_message(
            {
                "type": "LMS",
                "contentType": content_type,
                "countryCode": "82",
                "from": self._from_number,
                "content": MESSAGE_BODY,
                "messages": [{"to": to}],
            }
        )

    def send_one(
        self,
        to: str,
        file_ids: Sequence[str],
        *,
        content_type: str,
    ) -> SendResponse:
        if type(content_type) is not str or content_type not in {"COMM", "AD"}:
            raise ExplicitApiFailure("INVALID_REQUEST", "content type is invalid")
        return self._send_message(
            {
                "type": "MMS",
                "contentType": content_type,
                "countryCode": "82",
                "from": self._from_number,
                "content": MESSAGE_BODY,
                "messages": [{"to": to}],
                "files": [{"fileId": file_id} for file_id in file_ids],
            }
        )

    def list_by_request(self, request_id: str) -> MessageListResponse:
        query = urlencode({"requestId": request_id})
        return self._list_messages(query)

    def list_by_time_and_recipient(
        self,
        request_start_time: str,
        request_end_time: str,
        to: str,
        *,
        message_type: str = "MMS",
    ) -> MessageListResponse:
        query = urlencode(
            {
                "requestStartTime": request_start_time,
                "requestEndTime": request_end_time,
                "to": to,
                "type": message_type,
            }
        )
        return self._list_messages(query)

    def _list_messages(self, query: str) -> MessageListResponse:
        http_status, data = self._request_json(
            "GET", f"{self._messages_uri}?{query}"
        )
        if type(data.get("statusCode")) is not str or data["statusCode"] != "202":
            raise TransientLookupError("SENS list lookup was not successful")
        messages = data.get("messages")
        if type(messages) is not list:
            raise TransientLookupError("SENS list lookup omitted messages")
        if any(type(message) is not dict for message in messages):
            raise TransientLookupError("SENS list lookup omitted messages")
        return MessageListResponse(
            http_status=http_status,
            status_code=data["statusCode"],
            status_name=_exact_string(data.get("statusName")),
            messages=tuple(_message_record(message) for message in messages),
            page_size=data.get("pageSize"),
            page_index=data.get("pageIndex"),
            item_count=data.get("itemCount"),
            has_more=data.get("hasMore"),
        )

    def get_message(self, message_id: str) -> MessageResultResponse:
        uri = f"{self._messages_uri}/{quote(message_id, safe='')}"
        http_status, data = self._request_json("GET", uri)
        if type(data.get("statusCode")) is not str or data["statusCode"] != "200":
            raise TransientLookupError("SENS result lookup was not successful")
        messages = data.get("messages")
        if (
            type(messages) is not list
            or len(messages) != 1
            or type(messages[0]) is not dict
        ):
            raise TransientLookupError("SENS result lookup did not return one message")
        return MessageResultResponse(
            http_status=http_status,
            status_code=data["statusCode"],
            status_name=_exact_string(data.get("statusName")),
            message=_message_record(messages[0]),
        )


def _exact_string(value) -> str:
    return value if type(value) is str else ""


def _message_record(data: dict) -> MessageRecord:
    request_id = data.get("requestId")
    message_id = data.get("messageId")
    if (
        type(request_id) is not str
        or not request_id
        or type(message_id) is not str
        or not message_id
    ):
        raise TransientLookupError(
            "SENS lookup returned invalid correlation IDs"
        )
    return MessageRecord(
        request_id=request_id,
        message_id=message_id,
        to=_exact_string(data.get("to")),
        request_time=_exact_string(data.get("requestTime")),
        complete_time=_exact_string(data.get("completeTime")),
        telco_code=_exact_string(data.get("telcoCode")),
        status=_exact_string(data.get("status")),
        status_code=_exact_string(data.get("statusCode")),
        status_name=_exact_string(data.get("statusName")),
        status_message=_exact_string(data.get("statusMessage")),
        message_type=_exact_string(data.get("type")),
    )
