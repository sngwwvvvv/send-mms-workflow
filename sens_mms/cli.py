import argparse
from datetime import datetime
import json
import sys
import time
from pathlib import Path
from zoneinfo import ZoneInfo

from sens_mms.api import SensClient, UrlLibTransport
from sens_mms.config import ConfigError, load_config
from sens_mms.event_log import create_event_log, sanitize_status_code
from sens_mms.preflight import build_preflight
from sens_mms.results import (
    ResultStore,
    migrate_legacy_result,
    new_delivery_id,
)
from sens_mms.workflow import Workflow


SEOUL = ZoneInfo("Asia/Seoul")
_BLOCKED_OUTPUT = {
    "status": "BLOCKED",
    "message": "operation could not be completed safely",
}


class SystemClock:
    @staticmethod
    def monotonic():
        return time.monotonic()

    @staticmethod
    def now():
        return datetime.now(SEOUL)

    @staticmethod
    def sleep(seconds):
        time.sleep(seconds)


class SeoulClock:
    def __init__(self, delegate):
        self._delegate = delegate

    def monotonic(self):
        return self._delegate.monotonic()

    def now(self):
        return _seoul_time(self._delegate.now())

    def sleep(self, seconds):
        return self._delegate.sleep(seconds)


class SafeArgumentError(ValueError):
    pass


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        raise SafeArgumentError("invalid command arguments")


def _seoul_time(value):
    if value.tzinfo is None:
        return value.replace(tzinfo=SEOUL)
    return value.astimezone(SEOUL)


def _deidentified_error(error):
    status = sanitize_status_code(error.get("status", "UNKNOWN"))
    message = (
        "recipient validation failed"
        if status == "VALIDATION_ERROR"
        else "delivery failed"
    )
    return status, message


def _completion_dict(summary, store, approved_at):
    rows = ResultStore(summary.result_snapshot).load().values()
    error_counts = {}
    for row in rows:
        if row.delivery_status == "FAILED" and row.error:
            key = _deidentified_error(row.error)
            error_counts[key] = error_counts.get(key, 0) + 1
    return {
        "total": summary.total,
        "sent": summary.sent,
        "failed": summary.failed,
        "pending_confirmation": summary.pending,
        "failed_error_summary": [
            {"status": status, "message": message, "count": count}
            for (status, message), count in sorted(error_counts.items())
        ],
        "pending_follow_up_required": summary.pending > 0,
        "result_csv": str(store.path.resolve()),
        "live_send": True,
        "approved_at": approved_at,
    }


def main(
    argv=None,
    *,
    root=None,
    environ=None,
    transport=None,
    clock=None,
    timestamp_ms=None,
    stdout=None,
    event_log_factory=create_event_log,
    delivery_id_factory=new_delivery_id,
):
    project_root = Path(root) if root is not None else Path(__file__).parent.parent
    output = stdout or sys.stdout

    try:
        parser = SafeArgumentParser()
        parser.add_argument("command", choices=["preflight", "live"])
        parser.add_argument("--approval-token")
        parser.add_argument("--confirm-sender-registered", action="store_true")
        parser.add_argument("--resend-failed", action="store_true")
        args = parser.parse_args(argv)
        active_clock = SeoulClock(clock or SystemClock())
        config = load_config(project_root, environ)
        store = migrate_legacy_result(project_root)
        report = build_preflight(
            project_root,
            config,
            store,
            resend_failed=args.resend_failed,
        )
        if args.command == "preflight":
            print(
                json.dumps(report.to_public_dict(), ensure_ascii=False, indent=2),
                file=output,
            )
            return 0
        if not args.confirm_sender_registered:
            raise ConfigError("registered sender confirmation is required")
        if args.approval_token != report.approval_token:
            raise ConfigError("approval token mismatch")

        started_at = _seoul_time(active_clock.now())
        approved_at = started_at.isoformat(timespec="milliseconds")
        event_log = event_log_factory(
            project_root / "logs",
            started_at,
            now=active_clock.now,
        )
        try:
            active_transport = transport or UrlLibTransport()
            active_timestamp_ms = timestamp_ms or (
                lambda: str(int(time.time() * 1000))
            )
            client = SensClient(
                access_key=config.access_key,
                secret_key=config.secret_key,
                service_id=config.service_id,
                from_number=config.from_number,
                transport=active_transport,
                timestamp_ms=active_timestamp_ms,
            )
            workflow = Workflow(
                project_root,
                config,
                store,
                client,
                active_clock,
                event_log,
                delivery_id_factory,
                resend_failed=args.resend_failed,
            )
            summary = workflow.run_live(args.approval_token)
        except Exception:
            try:
                event_log.close()
            except Exception:
                pass
            raise
        event_log.close()
        print(
            json.dumps(
                _completion_dict(summary, store, approved_at),
                ensure_ascii=False,
                indent=2,
            ),
            file=output,
        )
        return 0
    except Exception:
        print(
            json.dumps(
                _BLOCKED_OUTPUT,
                ensure_ascii=False,
            ),
            file=output,
        )
        return 2
