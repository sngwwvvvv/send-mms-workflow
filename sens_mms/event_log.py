"""Durable, privacy-safe JSONL event logging."""

import json
import os
import re
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from .api import (
    MessageListResponse,
    MessageRecord,
    MessageResultResponse,
    SendResponse,
)


SEOUL = ZoneInfo("Asia/Seoul")
_DIGIT_RUN = re.compile(r"\d{4,}")
_RESPONSE_KEYS = frozenset(
    {
        "statusCode",
        "statusName",
        "requestId",
        "requestTime",
        "pageSize",
        "pageIndex",
        "itemCount",
        "hasMore",
        "messages",
        "message",
        "messageId",
        "completeTime",
        "telcoCode",
        "status",
        "statusMessage",
    }
)
_ERROR_KEYS = frozenset({"status", "message"})
_STATE_KEYS = frozenset(
    {"delivery_status", "is_sent", "attempts", "request_id", "message_id", "error"}
)
_SUMMARY_KEYS = frozenset({"total", "sent", "failed", "pending_confirmation"})
_INTERNAL_STATUS_CODES = frozenset(
    {
        "UNKNOWN",
        "VALIDATION_ERROR",
        "NETWORK_ERROR",
        "INVALID_RESPONSE",
        "AMBIGUOUS_POST_OUTCOME",
        "TRANSIENT_LOOKUP_ERROR",
    }
)
_REDACTED_MESSAGE = "redacted"
_ENVELOPE_STATUS_NAMES = frozenset({"", "success", "reserved", "fail"})
_MESSAGE_STATUS_NAMES = frozenset({"", "success", "fail"})
_MESSAGE_STATUSES = frozenset({"", "READY", "PROCESSING", "COMPLETED"})
_TELCO_CODES = frozenset({"", "SKT", "ETC"})
_SEND_TIME = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}\Z")
_MESSAGE_TIME = re.compile(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\Z")
_FORBIDDEN_KEYS = frozenset(
    {
        "recipient",
        "receiving_number",
        "number",
        "phone",
        "tel",
        "to",
        "from",
        "sender",
        "name",
        "content",
        "subject",
        "file",
        "files",
        "fileid",
        "file_id",
        "serviceid",
        "service_id",
        "headers",
        "authorization",
        "signature",
        "accesskey",
        "access_key",
        "secretkey",
        "secret_key",
        "body",
        "rawbody",
        "raw_body",
    }
)


class EventLogError(RuntimeError):
    """A safe error raised when an event-log checkpoint cannot be persisted."""


def _invalid_event_data():
    raise EventLogError("event log data invalid")


def _require_allowed_mapping(value, allowed_keys):
    if type(value) is not dict:
        _invalid_event_data()
    for key in value:
        if (
            type(key) is not str
            or key.lower() in _FORBIDDEN_KEYS
            or key not in allowed_keys
        ):
            _invalid_event_data()


def sanitize_status_code(value) -> str:
    if type(value) is str:
        if value in _INTERNAL_STATUS_CODES:
            return value
        if value.isascii() and value.isdigit() and 1 <= len(value) <= 4:
            return value
    elif type(value) is int and 0 <= value <= 9999:
        return str(value)
    return "UNKNOWN"


def redact_human_message(value) -> str:
    if value is None:
        return ""
    if type(value) is str:
        return "" if len(value) == 0 else _REDACTED_MESSAGE
    if type(value) in {list, dict}:
        return "" if len(value) == 0 else _REDACTED_MESSAGE
    return _REDACTED_MESSAGE


def _sanitize_choice(value, allowed) -> str:
    return value if type(value) is str and value in allowed else ""


def _sanitize_time(value, *, message_time: bool) -> str:
    if type(value) is not str:
        return ""
    pattern = _MESSAGE_TIME if message_time else _SEND_TIME
    if pattern.fullmatch(value) is None:
        return ""
    try:
        datetime.strptime(
            value,
            "%Y-%m-%d %H:%M:%S" if message_time else "%Y-%m-%dT%H:%M:%S.%f",
        )
    except ValueError:
        return ""
    return value


def _sanitize_page_int(value):
    return value if type(value) is int else None


def _sanitize_page_bool(value):
    return value if type(value) is bool else None


def _sanitize_correlation_id(value, *, allow_none=False):
    if value is None and allow_none:
        return None
    return value if type(value) is str else ""


def _seoul_time(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=SEOUL)
    return value.astimezone(SEOUL)


def _sanitize_response(value, *, message_context=False):
    if type(value) is dict:
        _require_allowed_mapping(value, _RESPONSE_KEYS)
        sanitized = {}
        for key, item in value.items():
            if key == "statusCode":
                sanitized[key] = sanitize_status_code(item)
            elif key == "statusName":
                sanitized[key] = _sanitize_choice(
                    item,
                    _MESSAGE_STATUS_NAMES if message_context else _ENVELOPE_STATUS_NAMES,
                )
            elif key == "status":
                sanitized[key] = _sanitize_choice(item, _MESSAGE_STATUSES)
            elif key == "telcoCode":
                sanitized[key] = _sanitize_choice(item, _TELCO_CODES)
            elif key == "statusMessage":
                sanitized[key] = redact_human_message(item)
            elif key == "message":
                sanitized[key] = (
                    _sanitize_response(item, message_context=True)
                    if type(item) is dict
                    else redact_human_message(item)
                )
            elif key == "messages":
                sanitized[key] = (
                    [
                        _sanitize_response(entry, message_context=True)
                        for entry in item
                        if type(entry) is dict
                    ]
                    if type(item) is list
                    else []
                )
            elif key in {"requestId", "messageId"}:
                sanitized[key] = _sanitize_correlation_id(item)
            elif key in {"requestTime", "completeTime"}:
                sanitized[key] = _sanitize_time(
                    item,
                    message_time=message_context or key == "completeTime",
                )
            elif key in {"pageSize", "pageIndex", "itemCount"}:
                sanitized[key] = _sanitize_page_int(item)
            elif key == "hasMore":
                sanitized[key] = _sanitize_page_bool(item)
        return sanitized
    _invalid_event_data()


def _sanitize_error(value):
    _require_allowed_mapping(value, _ERROR_KEYS)
    sanitized = {}
    for key, item in value.items():
        sanitized[key] = (
            redact_human_message(item)
            if key == "message"
            else sanitize_status_code(item)
        )
    return sanitized


def _sanitize_state(value):
    _require_allowed_mapping(value, _STATE_KEYS)
    sanitized = {}
    for key, item in value.items():
        if key == "error":
            sanitized[key] = _sanitize_error(item) if item is not None else None
        elif key in {"request_id", "message_id"}:
            sanitized[key] = _sanitize_correlation_id(item)
        elif item is None or isinstance(item, (str, int, float, bool)):
            sanitized[key] = item
        else:
            _invalid_event_data()
    return sanitized


def _sanitize_summary(value):
    _require_allowed_mapping(value, _SUMMARY_KEYS)
    if any(
        item is not None and not isinstance(item, (str, int, float, bool))
        for item in value.values()
    ):
        _invalid_event_data()
    return dict(value)


def _serialize_event_line(
    *,
    now,
    sequence,
    event,
    delivery_id=None,
    attempt=None,
    stage=None,
    api=None,
    http_status=None,
    request_id=None,
    message_id=None,
    response=None,
    error=None,
    state=None,
    summary=None,
    completed_at=None,
    result_snapshot=None,
):
    if stage is not None and stage not in {"MMS", "LMS"}:
        _invalid_event_data()
    response = _sanitize_response(response) if response is not None else None
    error = _sanitize_error(error) if error is not None else None
    state = _sanitize_state(state) if state is not None else None
    summary = _sanitize_summary(summary) if summary is not None else None
    request_id = _sanitize_correlation_id(request_id, allow_none=True)
    message_id = _sanitize_correlation_id(message_id, allow_none=True)
    row = {
        "schema_version": 1,
        "sequence": sequence,
        "logged_at": _seoul_time(now).isoformat(timespec="milliseconds"),
        "event": event,
        "delivery_id": delivery_id,
        "attempt": attempt,
        "stage": stage,
        "api": api,
        "http_status": http_status,
        "request_id": request_id,
        "message_id": message_id,
        "response": response,
        "error": error,
        "state": state,
        "summary": summary,
        "completed_at": completed_at,
        "result_snapshot": result_snapshot,
    }
    try:
        return json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
    except (TypeError, ValueError) as exc:
        raise EventLogError("event log data invalid") from exc


class JsonlEventLog:
    def __init__(self, path: Path, *, now):
        self.path = Path(path)
        self._now = now
        self._sequence = 0
        self._closed = False
        self._poisoned = False
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = self.path.open("x", encoding="utf-8", newline="\n")
        except OSError as exc:
            raise EventLogError("event log unavailable") from exc

    def write(
        self,
        event,
        *,
        delivery_id=None,
        attempt=None,
        stage=None,
        api=None,
        http_status=None,
        request_id=None,
        message_id=None,
        response=None,
        error=None,
        state=None,
        summary=None,
        completed_at=None,
        result_snapshot=None,
    ) -> None:
        if self._poisoned:
            raise EventLogError("event log unavailable")
        sequence = self._sequence + 1
        try:
            encoded = _serialize_event_line(
                now=self._now(),
                sequence=sequence,
                event=event,
                delivery_id=delivery_id,
                attempt=attempt,
                stage=stage,
                api=api,
                http_status=http_status,
                request_id=request_id,
                message_id=message_id,
                response=response,
                error=error,
                state=state,
                summary=summary,
                completed_at=completed_at,
                result_snapshot=result_snapshot,
            )
        except EventLogError:
            raise
        try:
            self._handle.write(encoded)
            self._handle.flush()
            os.fsync(self._handle.fileno())
        except (OSError, TypeError, ValueError) as exc:
            self._poisoned = True
            raise EventLogError("event log checkpoint failed") from exc
        self._sequence = sequence

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self._handle.close()
        except OSError as exc:
            raise EventLogError("event log close failed") from exc

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            self.close()
        else:
            try:
                self.close()
            except EventLogError:
                pass
        return False


def mask_digit_runs(text: str) -> str:
    return _DIGIT_RUN.sub(
        lambda match: "*" * (len(match.group()) - 4) + match.group()[-4:], text
    )


def safe_error_dict(status, message) -> dict:
    return {
        "status": sanitize_status_code(status),
        "message": redact_human_message(message),
    }


def send_to_log_dict(response: SendResponse) -> dict:
    return _sanitize_response({
        "requestId": response.request_id,
        "requestTime": response.request_time,
        "statusCode": sanitize_status_code(response.status_code),
        "statusName": response.status_name,
    })


def message_to_log_dict(message: MessageRecord) -> dict:
    return _sanitize_response({
        "requestId": message.request_id,
        "messageId": message.message_id,
        "requestTime": message.request_time,
        "completeTime": message.complete_time,
        "telcoCode": message.telco_code,
        "status": message.status,
        "statusCode": sanitize_status_code(message.status_code),
        "statusName": message.status_name,
        "statusMessage": redact_human_message(message.status_message),
    }, message_context=True)


def list_to_log_dict(response: MessageListResponse) -> dict:
    return _sanitize_response({
        "statusCode": sanitize_status_code(response.status_code),
        "statusName": response.status_name,
        "pageSize": response.page_size,
        "pageIndex": response.page_index,
        "itemCount": response.item_count,
        "hasMore": response.has_more,
        "messages": [message_to_log_dict(message) for message in response.messages],
    })


def message_result_to_log_dict(response: MessageResultResponse) -> dict:
    return _sanitize_response({
        "statusCode": sanitize_status_code(response.status_code),
        "statusName": response.status_name,
        "message": message_to_log_dict(response.message),
    })


def create_event_log(logs_dir: Path, started_at: datetime, *, now) -> JsonlEventLog:
    timestamp = _seoul_time(started_at).strftime("%Y%m%d_%H%M%S")
    logs_dir = Path(logs_dir)
    for suffix in range(1000):
        name = (
            f"delivery_{timestamp}.jsonl"
            if suffix == 0
            else f"delivery_{timestamp}_{suffix:03d}.jsonl"
        )
        path = logs_dir / name
        if path.exists():
            continue
        try:
            return JsonlEventLog(path, now=now)
        except EventLogError:
            if path.exists() and suffix < 999:
                continue
            raise
    raise EventLogError("event log unavailable")
