from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import threading
from typing import Mapping, Protocol, Sequence

from .results import ResultFormatError, ResultRow, ResultStore


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def now(self) -> datetime: ...
    def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True)
class RunSettings:
    worker_count: int
    poll_interval_seconds: float
    confirmation_timeout_seconds: float
    retry_delay_seconds: float
    max_attempts: int
    rate_limit_delays_seconds: tuple[float, float]


RUN_SETTINGS = RunSettings(5, 1, 120, 10, 3, (10, 20))


class RunSafetyError(RuntimeError):
    pass


_STALE_CHECKPOINT_MISMATCH = "result row does not match expected checkpoint"


class RunCoordinator:
    def __init__(self, store: ResultStore, event_log, clock: Clock):
        self.store = store
        self.event_log = event_log
        self.clock = clock
        self._checkpoint_lock = threading.Lock()
        self._event_lock = threading.Lock()
        self._rate_lock = threading.Lock()
        self._stopped = threading.Event()
        self._blocked_until = 0.0
        self._rate_limit_count = 0

    def stop(self) -> None:
        self._stopped.set()

    def raise_if_stopped(self) -> None:
        if self._stopped.is_set():
            raise RunSafetyError("workflow safety stop is active")

    def read_rows(self) -> dict[str, ResultRow]:
        with self._checkpoint_lock:
            self.store.load()
            return dict(self.store.rows)

    def commit(self, row: ResultRow, *, expected: ResultRow | None = None) -> None:
        self.commit_batch({row.receiving_number: expected}, (row,))

    def commit_batch(
        self,
        expected: Mapping[str, ResultRow | None],
        replacements: Sequence[ResultRow],
    ) -> None:
        self.raise_if_stopped()
        with self._checkpoint_lock:
            self.raise_if_stopped()
            try:
                self.store.load()
                self.store.replace_rows_atomic(expected, replacements)
            except ResultFormatError as exc:
                if exc.args == (_STALE_CHECKPOINT_MISMATCH,):
                    raise
                self.stop()
                raise
            except Exception:
                self.stop()
                raise

    def write(self, event: str, **fields) -> None:
        self.raise_if_stopped()
        with self._event_lock:
            self.raise_if_stopped()
            try:
                self.event_log.write(event, **fields)
            except Exception:
                self.stop()
                raise

    def record_429(self) -> None:
        self.raise_if_stopped()
        with self._rate_lock:
            self.raise_if_stopped()
            self._rate_limit_count += 1
            delay = (
                RUN_SETTINGS.rate_limit_delays_seconds[0]
                if self._rate_limit_count == 1
                else RUN_SETTINGS.rate_limit_delays_seconds[1]
            )
            self._blocked_until = max(
                self._blocked_until,
                self.clock.monotonic() + delay,
            )

    def before_api_call(self) -> None:
        self.raise_if_stopped()
        with self._rate_lock:
            self.raise_if_stopped()
            remaining = self._blocked_until - self.clock.monotonic()
            if remaining > 0:
                self.clock.sleep(remaining)
        self.raise_if_stopped()

    def wait_until(self, deadline: float) -> None:
        self.raise_if_stopped()
        with self._rate_lock:
            self.raise_if_stopped()
            self._blocked_until = max(self._blocked_until, deadline)
        self.before_api_call()
