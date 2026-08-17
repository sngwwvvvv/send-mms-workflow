from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import re
import secrets
import tempfile
import threading
from typing import Mapping, Sequence
from zoneinfo import ZoneInfo


COLUMNS = (
    "receiving_number",
    "delivery_id",
    "delivery_status",
    "is_sent",
    "attempts",
    "request_id",
    "message_id",
    "error",
)
_LEGACY_COLUMNS = (
    "receiving_number",
    "delivery_status",
    "is_sent",
    "attempts",
    "request_id",
    "message_id",
    "error",
)
_DELIVERY_ID_PATTERN = re.compile(r"[23456789ABCDEFGHJKLMNPQRSTUVWXYZ]{16}\Z")
_DELIVERY_ID_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
_SNAPSHOT_PATTERN = re.compile(
    r"result_(\d{8})_(\d{6})(?:_([0-9]{3}))?\.csv\Z"
)
_LIVE_LOCKS_GUARD = threading.Lock()
_LIVE_LOCKS: dict[str, threading.Lock] = {}


class ResultFormatError(ValueError):
    pass


class LiveExecutionLock:
    """A process-scoped OS lock serializing cooperating live workflows."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self._handle = None
        self._local_lock = None

    def __enter__(self):
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            canonical = str(self.path.resolve())
        except OSError as exc:
            raise ResultFormatError("live execution already in progress") from exc
        if os.name == "nt":
            canonical = canonical.casefold()
        with _LIVE_LOCKS_GUARD:
            local_lock = _LIVE_LOCKS.setdefault(canonical, threading.Lock())
        if not local_lock.acquire(blocking=False):
            raise ResultFormatError("live execution already in progress")
        self._local_lock = local_lock
        handle = None
        try:
            handle = self.path.open("a+b")
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
                os.fsync(handle.fileno())
            handle.seek(0)
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            try:
                if handle is not None:
                    handle.close()
            except OSError:
                pass
            self._local_lock.release()
            self._local_lock = None
            raise ResultFormatError("live execution already in progress") from exc
        self._handle = handle
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        handle = self._handle
        self._handle = None
        if handle is None:
            return False
        cleanup_error = None
        try:
            try:
                handle.seek(0)
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
            except OSError as release_error:
                cleanup_error = release_error
            try:
                handle.close()
            except OSError as close_error:
                if cleanup_error is None:
                    cleanup_error = close_error
        finally:
            if self._local_lock is not None:
                self._local_lock.release()
                self._local_lock = None
        if cleanup_error is not None and exc_type is None:
            raise ResultFormatError(
                "live execution lock release failed"
            ) from cleanup_error
        return False


def new_delivery_id(existing_ids, chooser=secrets.choice) -> str:
    existing = set(existing_ids)
    for _ in range(100):
        candidate = "".join(chooser(_DELIVERY_ID_ALPHABET) for _ in range(16))
        if not _DELIVERY_ID_PATTERN.fullmatch(candidate):
            raise ResultFormatError("delivery ID generator returned invalid data")
        if candidate not in existing:
            return candidate
    raise ResultFormatError("could not allocate a unique delivery ID")


def _snapshot_sort_key(path: Path):
    match = _SNAPSHOT_PATTERN.fullmatch(path.name)
    if match is None or match.group(3) == "000":
        return None
    try:
        timestamp = datetime.strptime(match.group(1) + match.group(2), "%Y%m%d%H%M%S")
    except ValueError:
        return None
    suffix = int(match.group(3) or "0")
    return timestamp, suffix


def select_latest_snapshot(results_dir: Path) -> Path:
    directory = Path(results_dir)
    candidates = [
        (key, path)
        for path in directory.iterdir()
        if path.is_file() and (key := _snapshot_sort_key(path)) is not None
    ]
    if not candidates:
        raise ResultFormatError("no result snapshot found")
    return max(candidates, key=lambda item: item[0])[1]


def load_failed_candidates(path: Path) -> tuple[ResultRow, ...]:
    rows = ResultStore(path).load()
    return tuple(row for row in rows.values() if row.delivery_status == "FAILED")


def collect_delivery_ids(results_dir: Path) -> set[str]:
    directory = Path(results_dir)
    if not directory.exists():
        return set()
    paths = []
    current = directory / "result.csv"
    if current.exists():
        paths.append(current)
    paths.extend(sorted(
        path for path in directory.iterdir() if _snapshot_sort_key(path) is not None
    ))

    delivery_ids = set()
    for path in paths:
        rows = ResultStore(path).load()
        delivery_ids.update(row.delivery_id for row in rows.values() if row.delivery_id)
    return delivery_ids


@dataclass
class ResultRow:
    receiving_number: str
    delivery_id: str
    delivery_status: str
    is_sent: str
    attempts: int
    request_id: str = ""
    message_id: str = ""
    error: dict | None = None

    def validate(self) -> None:
        if not all(isinstance(value, str) for value in (
            self.receiving_number,
            self.delivery_id,
            self.delivery_status,
            self.is_sent,
            self.request_id,
            self.message_id,
        )):
            raise ResultFormatError("invalid result row field")
        if self.delivery_id and not _DELIVERY_ID_PATTERN.fullmatch(self.delivery_id):
            raise ResultFormatError("invalid delivery_id")
        if self.delivery_status not in ("SENT", "FAILED", "PENDING_CONFIRMATION"):
            raise ResultFormatError("invalid delivery_status")
        if isinstance(self.attempts, bool) or not isinstance(self.attempts, int) or self.attempts < 0:
            raise ResultFormatError("invalid attempts")

        if self.delivery_status == "SENT":
            if self.is_sent != "true" or self.error is not None or self.attempts < 1:
                raise ResultFormatError("invalid SENT row")
        elif self.delivery_status == "FAILED":
            if self.is_sent != "false" or not isinstance(self.error, dict):
                raise ResultFormatError("invalid FAILED row")
            if set(self.error) != {"status", "message"} or not all(
                isinstance(self.error[key], str) for key in ("status", "message")
            ):
                raise ResultFormatError("invalid FAILED error")
        elif self.is_sent != "" or self.error is not None:
            raise ResultFormatError("invalid PENDING_CONFIRMATION row")
        elif self.attempts == 0:
            if not self.delivery_id or self.request_id or self.message_id:
                raise ResultFormatError("invalid PENDING_CONFIRMATION reservation")

    def to_dict(self) -> dict[str, str]:
        self.validate()
        return {
            "receiving_number": self.receiving_number,
            "delivery_id": self.delivery_id,
            "delivery_status": self.delivery_status,
            "is_sent": self.is_sent,
            "attempts": str(self.attempts),
            "request_id": self.request_id,
            "message_id": self.message_id,
            "error": "null" if self.error is None else json.dumps(
                self.error, ensure_ascii=False, separators=(",", ":")
            ),
        }


class ResultStore:
    def __init__(self, path: Path):
        self.path = Path(path)
        self.rows: dict[str, ResultRow] = {}

    @classmethod
    def for_root(cls, root: Path) -> "ResultStore":
        return cls(Path(root) / "results" / "result.csv")

    def load(self) -> dict[str, ResultRow]:
        if not self.path.exists():
            self.rows = {}
            return self.rows
        try:
            with self.path.open(encoding="utf-8-sig", newline="") as file:
                reader = csv.DictReader(file)
                if tuple(reader.fieldnames or ()) != COLUMNS:
                    raise ResultFormatError("result.csv columns invalid")
                rows: dict[str, ResultRow] = {}
                for fields in reader:
                    if None in fields:
                        raise ResultFormatError("result.csv row invalid")
                    number = fields["receiving_number"]
                    if number in rows:
                        raise ResultFormatError("duplicate result row")
                    error_text = fields["error"]
                    error = None if error_text == "null" else json.loads(error_text)
                    row = ResultRow(
                        receiving_number=number,
                        delivery_id=fields["delivery_id"],
                        delivery_status=fields["delivery_status"],
                        is_sent=fields["is_sent"],
                        attempts=int(fields["attempts"]),
                        request_id=fields["request_id"],
                        message_id=fields["message_id"],
                        error=error,
                    )
                    row.validate()
                    rows[number] = row
        except ResultFormatError:
            raise
        except (csv.Error, KeyError, TypeError, ValueError, UnicodeError, json.JSONDecodeError) as exc:
            raise ResultFormatError("result.csv row invalid") from exc
        self.rows = rows
        return self.rows

    def upsert(self, row: ResultRow) -> None:
        row.validate()
        self.rows[row.receiving_number] = row

    def replace_rows_atomic(
        self,
        expected: Mapping[str, ResultRow | None],
        replacements: Sequence[ResultRow],
    ) -> None:
        for number, expected_row in expected.items():
            if self.rows.get(number) != expected_row:
                raise ResultFormatError("result row does not match expected checkpoint")

        validated = []
        seen_numbers = set()
        for row in replacements:
            row.validate()
            if row.receiving_number in seen_numbers:
                raise ResultFormatError("duplicate replacement row")
            seen_numbers.add(row.receiving_number)
            validated.append(row)

        original_rows = dict(self.rows)
        try:
            for row in validated:
                self.rows[row.receiving_number] = row
            self.write_atomic()
        except Exception:
            self.rows = original_rows
            raise

    def write_atomic(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_descriptor, temporary_path = tempfile.mkstemp(
            prefix=".result-", suffix=".csv", dir=self.path.parent
        )
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=COLUMNS)
                writer.writeheader()
                for number in sorted(self.rows):
                    writer.writerow(self.rows[number].to_dict())
                file.flush()
                os.fsync(file.fileno())
            os.replace(temporary_path, self.path)
        finally:
            if os.path.exists(temporary_path):
                os.unlink(temporary_path)

    def snapshot(self, completed_at: datetime) -> Path:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        seoul_zone = ZoneInfo("Asia/Seoul")
        seoul_time = (
            completed_at.replace(tzinfo=seoul_zone)
            if completed_at.tzinfo is None
            else completed_at.astimezone(seoul_zone)
        )
        stem = seoul_time.strftime("result_%Y%m%d_%H%M%S")
        file_descriptor, temporary_path = tempfile.mkstemp(
            prefix=".snapshot-", suffix=".csv", dir=self.path.parent
        )
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=COLUMNS)
                writer.writeheader()
                for number in sorted(self.rows):
                    writer.writerow(self.rows[number].to_dict())
                file.flush()
                os.fsync(file.fileno())

            for suffix in range(1000):
                name = f"{stem}.csv" if suffix == 0 else f"{stem}_{suffix:03d}.csv"
                destination = self.path.parent / name
                try:
                    os.link(temporary_path, destination)
                except FileExistsError:
                    continue
                return destination
            raise ResultFormatError("snapshot collision limit exceeded")
        finally:
            if os.path.exists(temporary_path):
                os.unlink(temporary_path)


def _load_legacy_rows(payload: bytes) -> dict[str, ResultRow]:
    try:
        text = payload.decode("utf-8-sig")
        with io.StringIO(text, newline="") as file:
            reader = csv.DictReader(file)
            if tuple(reader.fieldnames or ()) != _LEGACY_COLUMNS:
                raise ResultFormatError("legacy result.csv columns invalid")
            rows: dict[str, ResultRow] = {}
            for fields in reader:
                if None in fields:
                    raise ResultFormatError("legacy result.csv row invalid")
                number = fields["receiving_number"]
                if number in rows:
                    raise ResultFormatError("duplicate legacy result row")
                error_text = fields["error"]
                error = None if error_text == "null" else json.loads(error_text)
                row = ResultRow(
                    receiving_number=number,
                    delivery_id="",
                    delivery_status=fields["delivery_status"],
                    is_sent=fields["is_sent"],
                    attempts=int(fields["attempts"]),
                    request_id=fields["request_id"],
                    message_id=fields["message_id"],
                    error=error,
                )
                row.validate()
                rows[number] = row
    except ResultFormatError:
        raise
    except (csv.Error, KeyError, TypeError, ValueError, UnicodeError) as exc:
        raise ResultFormatError("legacy result.csv row invalid") from exc
    return rows


def _write_store_exclusive(store: ResultStore) -> None:
    store.path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_path = tempfile.mkstemp(
        prefix=".legacy-result-", suffix=".csv", dir=store.path.parent
    )
    try:
        with os.fdopen(file_descriptor, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=COLUMNS)
            writer.writeheader()
            for number in sorted(store.rows):
                writer.writerow(store.rows[number].to_dict())
            file.flush()
            os.fsync(file.fileno())
        try:
            os.link(temporary_path, store.path)
        except FileExistsError as exc:
            raise ResultFormatError("current result appeared during migration") from exc
    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)


def _write_marker_atomic(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_path = tempfile.mkstemp(
        prefix=".legacy-marker-", suffix=".json", dir=path.parent
    )
    try:
        with os.fdopen(file_descriptor, "wb") as file:
            file.write(payload)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary_path, path)
    finally:
        if os.path.exists(temporary_path):
            os.unlink(temporary_path)


def _validate_migration_marker(marker: Path, source_hash: str) -> None:
    try:
        data = json.loads(marker.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ResultFormatError("legacy migration marker invalid") from exc
    if not isinstance(data, dict) or set(data) != {"schema_version", "source_sha256"}:
        raise ResultFormatError("legacy migration marker invalid")
    if isinstance(data["schema_version"], bool) or data["schema_version"] != 1:
        raise ResultFormatError("legacy migration marker invalid")
    recorded_hash = data["source_sha256"]
    if (
        not isinstance(recorded_hash, str)
        or re.fullmatch(r"[0-9a-f]{64}", recorded_hash) is None
        or recorded_hash != source_hash
    ):
        raise ResultFormatError("legacy migration marker invalid")


def migrate_legacy_result(root: Path) -> ResultStore:
    project_root = Path(root)
    legacy_path = project_root / "result.csv"
    store = ResultStore.for_root(project_root)
    marker = store.path.parent / ".legacy_result_migrated.json"
    legacy_exists = legacy_path.exists()
    current_exists = store.path.exists()

    if not legacy_exists and not current_exists:
        return store
    if not legacy_exists:
        store.load()
        return store

    try:
        legacy_bytes = legacy_path.read_bytes()
    except OSError as exc:
        raise ResultFormatError("legacy result.csv unreadable") from exc
    source_hash = hashlib.sha256(legacy_bytes).hexdigest()

    if current_exists:
        if not marker.exists():
            raise ResultFormatError("legacy migration marker missing")
        _validate_migration_marker(marker, source_hash)
        store.load()
        return store

    store.rows = _load_legacy_rows(legacy_bytes)
    _write_store_exclusive(store)
    marker_payload = json.dumps(
        {"schema_version": 1, "source_sha256": source_hash},
        ensure_ascii=True,
        separators=(",", ":"),
    ).encode("ascii")
    _write_marker_atomic(marker, marker_payload)
    return store
