import json
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sens_mms.coordination import (
    RUN_SETTINGS,
    Clock,
    RunCoordinator,
    RunSafetyError,
    RunSettings,
)
from sens_mms.event_log import EventLogError, JsonlEventLog
from sens_mms.results import ResultFormatError, ResultRow, ResultStore


FIRST = "01010000001"
SECOND = "01010000002"
THIRD = "01010000003"
FOURTH = "01010000004"
FIFTH = "01010000005"
ID_1 = "23456789ABCDEFGH"
ID_2 = "23456789ABCDEFGJ"
ID_3 = "23456789ABCDEFGK"
ID_4 = "23456789ABCDEFGM"
ID_5 = "23456789ABCDEFGN"


def reservation(number: str, delivery_id: str) -> ResultRow:
    return ResultRow(
        receiving_number=number,
        delivery_id=delivery_id,
        delivery_status="PENDING_CONFIRMATION",
        is_sent="",
        attempts=0,
        request_id="",
        message_id="",
        error=None,
    )


class LockedManualClock(Clock):
    def __init__(self):
        self._lock = threading.Lock()
        self.elapsed = 0.0
        self.base = datetime(2026, 8, 17, 9, 0, 0, tzinfo=timezone.utc)
        self.sleeps = []

    def monotonic(self) -> float:
        with self._lock:
            return self.elapsed

    def now(self) -> datetime:
        with self._lock:
            return self.base + timedelta(seconds=self.elapsed)

    def sleep(self, seconds: float) -> None:
        with self._lock:
            self.sleeps.append(seconds)
            self.elapsed += seconds


class FailingWriteStore(ResultStore):
    def __init__(self, path: Path, failure: Exception):
        super().__init__(path)
        self.failure = failure

    def write_atomic(self) -> None:
        raise self.failure


class FailingEventLog:
    def __init__(self, failure: Exception):
        self.failure = failure
        self.path = Path(tempfile.mkdtemp()) / "logs" / "delivery.jsonl"
        self.calls = []

    def write(self, event, **fields) -> None:
        self.calls.append((event, fields))
        raise self.failure


class CoordinationTests(unittest.TestCase):
    def make_coordinator(
        self,
        *,
        clock: LockedManualClock | None = None,
        store: ResultStore | None = None,
        event_log=None,
    ) -> RunCoordinator:
        root = Path(tempfile.mkdtemp())
        clock = clock or LockedManualClock()
        store = store or ResultStore.for_root(root)
        event_log = event_log or JsonlEventLog(root / "logs" / "delivery.jsonl", now=clock.now)
        if hasattr(event_log, "close"):
            self.addCleanup(event_log.close)
        return RunCoordinator(store, event_log, clock)

    def test_fixed_settings_are_exact_and_not_runtime_options(self):
        self.assertEqual(
            RUN_SETTINGS,
            RunSettings(
                worker_count=5,
                poll_interval_seconds=1,
                confirmation_timeout_seconds=120,
                retry_delay_seconds=10,
                max_attempts=3,
                rate_limit_delays_seconds=(10, 20),
            ),
        )

    def test_batch_commit_is_all_or_nothing_against_fresh_rows(self):
        coordinator = self.make_coordinator()
        store = coordinator.store

        coordinator.commit_batch(
            {FIRST: None, SECOND: None},
            (reservation(FIRST, ID_1), reservation(SECOND, ID_2)),
        )

        self.assertEqual(set(ResultStore(store.path).load()), {FIRST, SECOND})

        with self.assertRaises(ResultFormatError):
            coordinator.commit_batch(
                {FIRST: reservation(FIRST, ID_3)},
                (reservation(FIRST, ID_4),),
            )

        self.assertEqual(ResultStore(store.path).load()[FIRST].delivery_id, ID_1)

    def test_five_concurrent_commits_and_events_keep_all_rows_and_exact_sequences(self):
        clock = LockedManualClock()
        coordinator = self.make_coordinator(clock=clock)
        barrier = threading.Barrier(6)
        errors = []
        recipients = (
            (FIRST, ID_1),
            (SECOND, ID_2),
            (THIRD, ID_3),
            (FOURTH, ID_4),
            (FIFTH, ID_5),
        )

        def worker(number: str, delivery_id: str) -> None:
            try:
                barrier.wait()
                coordinator.commit(reservation(number, delivery_id))
                coordinator.write("DELIVERY_ID_ASSIGNED", delivery_id=delivery_id)
            except Exception as exc:  # pragma: no cover - asserted through errors
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(number, delivery_id))
            for number, delivery_id in recipients
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(
            set(ResultStore(coordinator.store.path).load()),
            {FIRST, SECOND, THIRD, FOURTH, FIFTH},
        )
        sequences = [
            json.loads(line)["sequence"]
            for line in coordinator.event_log.path.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(sequences, [1, 2, 3, 4, 5])

    def test_first_429_waits_ten_and_later_429s_wait_twenty(self):
        clock = LockedManualClock()
        coordinator = self.make_coordinator(clock=clock)

        coordinator.record_429()
        coordinator.before_api_call()
        coordinator.record_429()
        coordinator.before_api_call()
        coordinator.record_429()
        coordinator.before_api_call()

        self.assertEqual(clock.sleeps, [10, 20, 20])

    def test_five_callers_share_one_active_429_wait_window(self):
        clock = LockedManualClock()
        coordinator = self.make_coordinator(clock=clock)
        barrier = threading.Barrier(6)
        passed = []
        errors = []

        coordinator.record_429()

        def worker(name: str) -> None:
            try:
                barrier.wait()
                coordinator.before_api_call()
                passed.append(name)
            except Exception as exc:  # pragma: no cover - asserted through errors
                errors.append(exc)

        threads = [
            threading.Thread(target=worker, args=(f"caller-{index}",))
            for index in range(5)
        ]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()

        self.assertEqual(errors, [])
        self.assertEqual(len(passed), 5)
        self.assertEqual(clock.sleeps, [10])

    def test_wait_until_converges_with_later_rate_limit_deadline(self):
        clock = LockedManualClock()
        coordinator = self.make_coordinator(clock=clock)

        coordinator.record_429()
        coordinator.wait_until(20)
        coordinator.before_api_call()

        self.assertEqual(clock.sleeps, [20])

    def test_checkpoint_failure_stops_later_commits_events_and_api_calls(self):
        failure = OSError("checkpoint unavailable")
        root = Path(tempfile.mkdtemp())
        clock = LockedManualClock()
        store = FailingWriteStore(ResultStore.for_root(root).path, failure)
        coordinator = self.make_coordinator(clock=clock, store=store)

        with self.assertRaises(OSError) as raised:
            coordinator.commit(reservation(FIRST, ID_1))

        self.assertIs(raised.exception, failure)
        with self.assertRaisesRegex(RunSafetyError, "workflow safety stop is active"):
            coordinator.raise_if_stopped()
        with self.assertRaises(RunSafetyError):
            coordinator.commit(reservation(SECOND, ID_2))
        with self.assertRaises(RunSafetyError):
            coordinator.write("AFTER_STOP")
        for api_name in ("upload", "send", "list", "get"):
            with self.subTest(api_name=api_name):
                with self.assertRaises(RunSafetyError):
                    coordinator.before_api_call()

    def test_event_failure_stops_later_commits_events_and_api_calls(self):
        failure = EventLogError("event log checkpoint failed")
        clock = LockedManualClock()
        coordinator = self.make_coordinator(event_log=FailingEventLog(failure), clock=clock)

        with self.assertRaises(EventLogError) as raised:
            coordinator.write("WORKFLOW_STARTED")

        self.assertIs(raised.exception, failure)
        with self.assertRaisesRegex(RunSafetyError, "workflow safety stop is active"):
            coordinator.raise_if_stopped()
        with self.assertRaises(RunSafetyError):
            coordinator.commit(reservation(FIRST, ID_1))
        with self.assertRaises(RunSafetyError):
            coordinator.write("AFTER_STOP")
        for api_name in ("upload", "send", "list", "get"):
            with self.subTest(api_name=api_name):
                with self.assertRaises(RunSafetyError):
                    coordinator.before_api_call()


if __name__ == "__main__":
    unittest.main()
