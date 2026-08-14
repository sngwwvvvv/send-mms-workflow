import tempfile
import os
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import sens_mms.results as results
from sens_mms.results import (
    COLUMNS,
    LiveExecutionLock,
    ResultFormatError,
    ResultRow,
    ResultStore,
)


VALID_ID = "23456789ABCDEFGH"
DELIVERY_ID_ALPHABET = "23456789ABCDEFGHJKLMNPQRSTUVWXYZ"
LEGACY_BYTES = (
    b"receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error\n"
    b"0101,SENT,true,1,r1,m1,null\n"
    b"0102,FAILED,false,0,,,\"{\"\"status\"\":\"\"VALIDATION_ERROR\"\",\"\"message\"\":\"\"invalid\"\"}\"\n"
)
LEGACY_HASH = "f86c4cb10c241a410b2be16c2e1166cbb9f48df727b661033658d302143085b9"
CHANGED_LEGACY_BYTES = (
    b"receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error\n"
    b"0101,SENT,true,1,changed-request,changed-message,null\n"
)


class ResultTests(unittest.TestCase):
    def test_snapshot_attaches_seoul_to_naive_time_without_host_conversion(self):
        """Catches standalone ResultStore snapshots interpreting naive time as host-local."""
        class HostSensitiveNaive(datetime):
            def astimezone(self, tz=None):
                return datetime(2026, 8, 13, 12, 1, 2, tzinfo=timezone.utc).astimezone(tz)

        store = ResultStore.for_root(Path(tempfile.mkdtemp()))

        snapshot = store.snapshot(HostSensitiveNaive(2026, 8, 14, 3, 1, 2))

        self.assertEqual(snapshot.name, "result_20260814_030102.csv")

    def test_live_lock_release_failure_blocks_success_but_preserves_active_exception(self):
        """Catches lock cleanup replacing an active workflow error or allowing completion."""
        target = "msvcrt.locking" if os.name == "nt" else "fcntl.flock"
        path = Path(tempfile.mkdtemp()) / ".live-execution.lock"
        with patch(target, side_effect=[None, OSError("unlock failed")]):
            with self.assertRaises(Exception) as raised:
                with LiveExecutionLock(path):
                    pass
        self.assertIsInstance(raised.exception, ResultFormatError)
        self.assertEqual(str(raised.exception), "live execution lock release failed")

        active = ValueError("active workflow failure")
        with patch(target, side_effect=[None, OSError("unlock failed")]):
            with self.assertRaises(ValueError) as raised:
                with LiveExecutionLock(path):
                    raise active
        self.assertIs(raised.exception, active)

    def test_live_lock_directory_failure_uses_fixed_safe_error(self):
        """Catches lock setup leaking an OS/path error before a live workflow starts."""
        parent = Path(tempfile.mkdtemp()) / "not-a-directory"
        parent.write_text("occupied", encoding="utf-8")

        with self.assertRaises(Exception) as raised:
            with LiveExecutionLock(parent / ".live-execution.lock"):
                pass

        self.assertIsInstance(raised.exception, ResultFormatError)
        self.assertEqual(str(raised.exception), "live execution already in progress")

    def test_result_row_accepts_delivery_id_keyword(self):
        try:
            row = ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="SENT",
                is_sent="true",
                attempts=1,
            )
        except TypeError as exc:
            self.fail(f"ResultRow must accept delivery_id: {exc}")

        self.assertEqual(row.delivery_id, VALID_ID)

    def test_for_root_writes_exact_new_schema_and_round_trips_a_row(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ResultStore.for_root(root)
            store.upsert(ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="SENT",
                is_sent="true",
                attempts=1,
                request_id="request-1",
                message_id="message-1",
                error=None,
            ))

            store.write_atomic()

            result_path = root / "results" / "result.csv"
            self.assertEqual(store.path, result_path)
            self.assertEqual(
                result_path.read_bytes().splitlines()[0],
                b"receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error",
            )
            loaded = ResultStore.for_root(root).load()["0101"]
            self.assertEqual(loaded.delivery_id, VALID_ID)
            self.assertEqual(loaded.request_id, "request-1")

    def test_pending_reservation_with_delivery_id_and_blank_api_ids_is_legal(self):
        row = ResultRow(
            receiving_number="0101",
            delivery_id=VALID_ID,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=0,
            request_id="",
            message_id="",
            error=None,
        )

        self.assertEqual(row.to_dict()["delivery_id"], VALID_ID)
        self.assertEqual(row.to_dict()["attempts"], "0")

    def test_pending_reservation_rejects_missing_delivery_id_or_api_ids(self):
        cases = (
            {"delivery_id": "", "request_id": "", "message_id": ""},
            {"delivery_id": VALID_ID, "request_id": "request-1", "message_id": ""},
            {"delivery_id": VALID_ID, "request_id": "", "message_id": "message-1"},
        )
        for fields in cases:
            with self.subTest(fields=fields):
                row = ResultRow(
                    receiving_number="0101",
                    delivery_id=fields["delivery_id"],
                    delivery_status="PENDING_CONFIRMATION",
                    is_sent="",
                    attempts=0,
                    request_id=fields["request_id"],
                    message_id=fields["message_id"],
                    error=None,
                )
                with self.assertRaises(ResultFormatError):
                    row.validate()

    def test_nonempty_delivery_id_must_use_exact_alphabet_and_length(self):
        invalid_ids = (
            "23456789ABCDEFG",
            "23456789ABCDEFGHI",
            "123456789ABCDEFG",
            "23456789ABCDEFGI",
            "23456789abcdefgH",
        )
        for delivery_id in invalid_ids:
            with self.subTest(delivery_id=delivery_id):
                row = ResultRow(
                    receiving_number="0101",
                    delivery_id=delivery_id,
                    delivery_status="FAILED",
                    is_sent="false",
                    attempts=0,
                    error={"status": "VALIDATION_ERROR", "message": "invalid"},
                )
                with self.assertRaises(ResultFormatError):
                    row.validate()

    def test_non_string_delivery_id_raises_safe_format_error(self):
        row = ResultRow(
            receiving_number="0101",
            delivery_id=23,
            delivery_status="FAILED",
            is_sent="false",
            attempts=0,
            error={"status": "VALIDATION_ERROR", "message": "invalid"},
        )

        with self.assertRaises(Exception) as raised:
            row.validate()
        self.assertIsInstance(raised.exception, ResultFormatError)

    def test_empty_delivery_id_remains_legal_for_validation_failure(self):
        row = ResultRow(
            receiving_number="bad-input",
            delivery_id="",
            delivery_status="FAILED",
            is_sent="false",
            attempts=0,
            error={"status": "VALIDATION_ERROR", "message": "invalid"},
        )

        row.validate()

    def test_status_invariants_are_strict(self):
        rows = (
            ResultRow(
                receiving_number="0101", delivery_id=VALID_ID,
                delivery_status="SENT", is_sent="false", attempts=1,
            ),
            ResultRow(
                receiving_number="0101", delivery_id=VALID_ID,
                delivery_status="SENT", is_sent="true", attempts=0,
            ),
            ResultRow(
                receiving_number="0101", delivery_id=VALID_ID,
                delivery_status="FAILED", is_sent="false", attempts=0, error=None,
            ),
            ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="FAILED",
                is_sent="false",
                attempts=0,
                error={"status": "X", "message": "bad", "extra": "no"},
            ),
            ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="PENDING_CONFIRMATION",
                is_sent="",
                attempts=1,
                error={"status": "X", "message": "bad"},
            ),
        )
        for row in rows:
            with self.subTest(row=row):
                with self.assertRaises(ResultFormatError):
                    row.validate()

    def test_load_rejects_bad_header_duplicate_and_corrupt_json_without_writing(self):
        fixtures = (
            b"bad\n",
            (
                b"receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error\n"
                b"0101,23456789ABCDEFGH,SENT,true,1,r1,m1,null\n"
                b"0101,ABCDEFGH23456789,SENT,true,1,r2,m2,null\n"
            ),
            (
                b"receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error\n"
                b"0101,23456789ABCDEFGH,FAILED,false,1,r1,m1,{broken}\n"
            ),
            (
                b"receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error\n"
                b"0101,23456789ABCDEFGH,SENT,true,1,r1,m1,null,unexpected\n"
            ),
        )
        for payload in fixtures:
            with self.subTest(payload=payload), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "result.csv"
                path.write_bytes(payload)

                with self.assertRaises(ResultFormatError):
                    ResultStore(path).load()

                self.assertEqual(path.read_bytes(), payload)

    def test_new_delivery_id_uses_exact_alphabet_for_sixteen_characters(self):
        generator = getattr(results, "new_delivery_id", None)
        self.assertTrue(callable(generator), "new_delivery_id must be available")
        seen_alphabets = []

        generated = generator(
            set(), chooser=lambda alphabet: seen_alphabets.append(alphabet) or "2"
        )

        self.assertEqual(generated, "2222222222222222")
        self.assertEqual(seen_alphabets, [DELIVERY_ID_ALPHABET] * 16)

    def test_new_delivery_id_retries_the_entire_id_after_collision(self):
        generator = getattr(results, "new_delivery_id", None)
        self.assertTrue(callable(generator), "new_delivery_id must be available")
        choices = iter("2" * 16 + "3" * 16)

        generated = generator(
            {"2222222222222222"}, chooser=lambda alphabet: next(choices)
        )

        self.assertEqual(generated, "3333333333333333")

    def test_new_delivery_id_blocks_after_one_hundred_collisions(self):
        generator = getattr(results, "new_delivery_id", None)
        self.assertTrue(callable(generator), "new_delivery_id must be available")

        with self.assertRaises(ResultFormatError) as raised:
            generator({"2222222222222222"}, chooser=lambda alphabet: "2")

        self.assertNotIn("2222222222222222", str(raised.exception))

    def test_snapshot_converts_to_seoul_time_and_matches_current_csv_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = ResultStore.for_root(root)
            store.upsert(ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="SENT",
                is_sent="true",
                attempts=1,
            ))
            store.write_atomic()
            snapshot = getattr(store, "snapshot", None)
            self.assertTrue(callable(snapshot), "ResultStore.snapshot must be available")

            snapshot_path = snapshot(datetime(2026, 8, 14, 15, 1, 2, tzinfo=timezone.utc))

            self.assertEqual(snapshot_path.name, "result_20260815_000102.csv")
            self.assertEqual(snapshot_path.read_bytes(), store.path.read_bytes())

    def test_snapshot_uses_next_suffix_without_overwriting_history(self):
        with tempfile.TemporaryDirectory() as directory:
            store = ResultStore.for_root(Path(directory))
            store.upsert(ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="SENT",
                is_sent="true",
                attempts=1,
            ))
            store.path.parent.mkdir(parents=True)
            base = store.path.parent / "result_20260815_000102.csv"
            first_collision = store.path.parent / "result_20260815_000102_001.csv"
            base.write_bytes(b"original-base")
            first_collision.write_bytes(b"original-001")
            snapshot = getattr(store, "snapshot", None)
            self.assertTrue(callable(snapshot), "ResultStore.snapshot must be available")

            snapshot_path = snapshot(datetime(2026, 8, 14, 15, 1, 2, tzinfo=timezone.utc))

            self.assertEqual(snapshot_path.name, "result_20260815_000102_002.csv")
            self.assertEqual(base.read_bytes(), b"original-base")
            self.assertEqual(first_collision.read_bytes(), b"original-001")
            self.assertEqual(
                snapshot_path.read_bytes().splitlines()[0],
                b"receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error",
            )

    def test_select_latest_snapshot_uses_timestamp_then_suffix_and_ignores_other_names(self):
        selector = getattr(results, "select_latest_snapshot", None)
        self.assertTrue(callable(selector), "select_latest_snapshot must be available")
        with tempfile.TemporaryDirectory() as directory:
            results_dir = Path(directory)
            latest = results_dir / "result_20260815_000000_002.csv"
            latest.write_bytes(b"latest-created-first")
            for name in (
                "result_20260814_235959_999.csv",
                "result_20260815_000000.csv",
                "result_20260815_000000_001.csv",
                "result_20260815_000000_000.csv",
                "result_20260815_000000_1000.csv",
                "result_20260230_000000.csv",
                "result_20260816_000000.csv.bak",
                "unrelated.csv",
            ):
                (results_dir / name).write_bytes(b"created-after-latest")

            selected = selector(results_dir)

            self.assertEqual(selected, latest)

    def test_corrupt_latest_snapshot_is_selected_and_failed_candidate_load_blocks(self):
        selector = getattr(results, "select_latest_snapshot", None)
        loader = getattr(results, "load_failed_candidates", None)
        self.assertTrue(callable(selector), "select_latest_snapshot must be available")
        self.assertTrue(callable(loader), "load_failed_candidates must be available")
        with tempfile.TemporaryDirectory() as directory:
            results_dir = Path(directory)
            older = results_dir / "result_20260814_120000.csv"
            older.write_text(
                ",".join(COLUMNS) + "\n"
                + "0101,23456789ABCDEFGH,FAILED,false,1,r1,m1,"
                + '"{""status"":""X"",""message"":""bad""}"\n',
                encoding="utf-8",
            )
            latest = results_dir / "result_20260814_120001.csv"
            latest.write_bytes(b"corrupt\n")

            selected = selector(results_dir)

            self.assertEqual(selected, latest)
            with self.assertRaises(ResultFormatError):
                loader(selected)

    def test_load_failed_candidates_returns_only_failed_rows_in_file_order(self):
        loader = getattr(results, "load_failed_candidates", None)
        self.assertTrue(callable(loader), "load_failed_candidates must be available")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "result_20260814_120000.csv"
            path.write_text(
                ",".join(COLUMNS) + "\n"
                + "0102,23456789ABCDEFGH,FAILED,false,3,r2,m2,"
                + '"{""status"":""X2"",""message"":""second""}"\n'
                + "0101,ABCDEFGH23456789,SENT,true,1,r1,m1,null\n"
                + "0103,23456789ABCDEFGJ,FAILED,false,0,,,"
                + '"{""status"":""X3"",""message"":""third""}"\n',
                encoding="utf-8",
            )

            failed = loader(path)

            self.assertIsInstance(failed, tuple)
            self.assertEqual([row.receiving_number for row in failed], ["0102", "0103"])
            self.assertEqual([row.error["status"] for row in failed], ["X2", "X3"])

    def test_collect_delivery_ids_reads_current_and_every_timestamped_snapshot(self):
        collector = getattr(results, "collect_delivery_ids", None)
        self.assertTrue(callable(collector), "collect_delivery_ids must be available")
        with tempfile.TemporaryDirectory() as directory:
            results_dir = Path(directory)
            header = ",".join(COLUMNS) + "\n"
            (results_dir / "result.csv").write_text(
                header + "0101,23456789ABCDEFGH,SENT,true,1,r1,m1,null\n",
                encoding="utf-8",
            )
            (results_dir / "result_20260814_120000.csv").write_text(
                header + "0102,ABCDEFGH23456789,SENT,true,1,r2,m2,null\n",
                encoding="utf-8",
            )
            (results_dir / "result_20260814_120000_001.csv").write_text(
                header + "0103,23456789ABCDEFGJ,SENT,true,1,r3,m3,null\n",
                encoding="utf-8",
            )
            (results_dir / "result_20260814_120000_000.csv").write_bytes(b"ignored")
            (results_dir / "notes.csv").write_bytes(b"ignored")

            delivery_ids = collector(results_dir)

            self.assertEqual(delivery_ids, {
                "23456789ABCDEFGH",
                "ABCDEFGH23456789",
                "23456789ABCDEFGJ",
            })

    def test_collect_delivery_ids_blocks_on_any_corrupt_historical_snapshot(self):
        collector = getattr(results, "collect_delivery_ids", None)
        self.assertTrue(callable(collector), "collect_delivery_ids must be available")
        with tempfile.TemporaryDirectory() as directory:
            results_dir = Path(directory)
            (results_dir / "result.csv").write_text(
                ",".join(COLUMNS) + "\n"
                + "0101,23456789ABCDEFGH,SENT,true,1,r1,m1,null\n",
                encoding="utf-8",
            )
            corrupt = results_dir / "result_20260813_120000.csv"
            corrupt.write_bytes(b"corrupt\n")

            with self.assertRaises(ResultFormatError):
                collector(results_dir)

            self.assertEqual(corrupt.read_bytes(), b"corrupt\n")

    def test_migrate_legacy_result_returns_empty_new_store_when_no_results_exist(self):
        migrator = getattr(results, "migrate_legacy_result", None)
        self.assertTrue(callable(migrator), "migrate_legacy_result must be available")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            store = migrator(root)

            self.assertEqual(store.path, root / "results" / "result.csv")
            self.assertEqual(store.rows, {})
            self.assertFalse(store.path.exists())

    def test_migrate_legacy_result_writes_new_schema_and_exact_marker_without_changing_source(self):
        migrator = getattr(results, "migrate_legacy_result", None)
        self.assertTrue(callable(migrator), "migrate_legacy_result must be available")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "result.csv"
            legacy.write_bytes(LEGACY_BYTES)

            store = migrator(root)

            self.assertEqual(legacy.read_bytes(), LEGACY_BYTES)
            self.assertEqual(store.path, root / "results" / "result.csv")
            self.assertEqual(
                store.path.read_bytes().splitlines()[0],
                b"receiving_number,delivery_id,delivery_status,is_sent,attempts,request_id,message_id,error",
            )
            self.assertEqual([row.delivery_id for row in store.rows.values()], ["", ""])
            self.assertEqual(
                (root / "results" / ".legacy_result_migrated.json").read_bytes(),
                (
                    b'{"schema_version":1,"source_sha256":"'
                    + LEGACY_HASH.encode("ascii")
                    + b'"}'
                ),
            )

    def test_migrate_legacy_result_validates_new_only_and_repeat_is_idempotent(self):
        migrator = getattr(results, "migrate_legacy_result", None)
        self.assertTrue(callable(migrator), "migrate_legacy_result must be available")
        with tempfile.TemporaryDirectory() as new_only_directory:
            new_only_root = Path(new_only_directory)
            new_store = ResultStore.for_root(new_only_root)
            new_store.upsert(ResultRow(
                receiving_number="0101",
                delivery_id=VALID_ID,
                delivery_status="SENT",
                is_sent="true",
                attempts=1,
            ))
            new_store.write_atomic()

            returned = migrator(new_only_root)

            self.assertEqual(returned.load()["0101"].delivery_id, VALID_ID)

        with tempfile.TemporaryDirectory() as repeat_directory:
            root = Path(repeat_directory)
            (root / "result.csv").write_bytes(LEGACY_BYTES)
            first = migrator(root)
            current_bytes = first.path.read_bytes()
            marker = root / "results" / ".legacy_result_migrated.json"
            marker_bytes = marker.read_bytes()

            second = migrator(root)

            self.assertEqual(second.path.read_bytes(), current_bytes)
            self.assertEqual(marker.read_bytes(), marker_bytes)

    def test_migrate_legacy_result_blocks_current_without_marker_and_preserves_both_files(self):
        migrator = getattr(results, "migrate_legacy_result", None)
        self.assertTrue(callable(migrator), "migrate_legacy_result must be available")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "result.csv"
            legacy.write_bytes(LEGACY_BYTES)
            current = ResultStore.for_root(root)
            current.upsert(ResultRow(
                receiving_number="0101",
                delivery_id="",
                delivery_status="SENT",
                is_sent="true",
                attempts=1,
            ))
            current.write_atomic()
            current_bytes = current.path.read_bytes()

            with self.assertRaises(ResultFormatError):
                migrator(root)

            self.assertEqual(legacy.read_bytes(), LEGACY_BYTES)
            self.assertEqual(current.path.read_bytes(), current_bytes)

    def test_migrate_legacy_result_blocks_changed_source_or_invalid_marker(self):
        migrator = getattr(results, "migrate_legacy_result", None)
        self.assertTrue(callable(migrator), "migrate_legacy_result must be available")
        marker_payloads = (
            b'{"schema_version":2,"source_sha256":"' + LEGACY_HASH.encode("ascii") + b'"}',
            b'{"schema_version":1,"source_sha256":"' + b"0" * 64 + b'"}',
            b'{"schema_version":1,"source_sha256":"' + LEGACY_HASH.encode("ascii") + b'","extra":true}',
            b"not-json",
        )
        for marker_payload in marker_payloads:
            with self.subTest(marker_payload=marker_payload), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "result.csv").write_bytes(LEGACY_BYTES)
                store = migrator(root)
                current_bytes = store.path.read_bytes()
                marker = root / "results" / ".legacy_result_migrated.json"
                marker.write_bytes(marker_payload)

                with self.assertRaises(ResultFormatError):
                    migrator(root)

                self.assertEqual(store.path.read_bytes(), current_bytes)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "result.csv"
            legacy.write_bytes(LEGACY_BYTES)
            store = migrator(root)
            current_bytes = store.path.read_bytes()
            legacy.write_bytes(LEGACY_BYTES + b"changed")

            with self.assertRaises(ResultFormatError):
                migrator(root)

            self.assertEqual(store.path.read_bytes(), current_bytes)

    def test_migrate_legacy_result_rejects_corrupt_legacy_without_creating_outputs(self):
        migrator = getattr(results, "migrate_legacy_result", None)
        self.assertTrue(callable(migrator), "migrate_legacy_result must be available")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "result.csv").write_bytes(b"bad-header\n")

            with self.assertRaises(ResultFormatError):
                migrator(root)

            self.assertFalse((root / "results" / "result.csv").exists())
            self.assertFalse((root / "results" / ".legacy_result_migrated.json").exists())

    def test_migrate_legacy_result_rejects_legacy_rows_with_extra_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "result.csv").write_bytes(
                b"receiving_number,delivery_status,is_sent,attempts,request_id,message_id,error\n"
                b"0101,SENT,true,1,r1,m1,null,unexpected\n"
            )

            with self.assertRaises(ResultFormatError):
                results.migrate_legacy_result(root)

            self.assertFalse((root / "results" / "result.csv").exists())

    def test_migration_hash_and_rows_come_from_the_same_captured_legacy_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "result.csv"
            legacy.write_bytes(LEGACY_BYTES)
            original_read_bytes = Path.read_bytes
            source_was_changed = False

            def read_then_change_source(path):
                nonlocal source_was_changed
                payload = original_read_bytes(path)
                if path == legacy and not source_was_changed:
                    source_was_changed = True
                    legacy.write_bytes(CHANGED_LEGACY_BYTES)
                return payload

            with patch.object(Path, "read_bytes", new=read_then_change_source):
                store = results.migrate_legacy_result(root)

            self.assertEqual(store.load()["0101"].request_id, "r1")
            self.assertEqual(len(store.rows), 2)
            self.assertEqual(
                (root / "results" / ".legacy_result_migrated.json").read_bytes(),
                (
                    b'{"schema_version":1,"source_sha256":"'
                    + LEGACY_HASH.encode("ascii")
                    + b'"}'
                ),
            )
            self.assertEqual(legacy.read_bytes(), CHANGED_LEGACY_BYTES)

    def test_migration_exclusive_publication_never_overwrites_a_concurrent_current(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "result.csv").write_bytes(LEGACY_BYTES)
            current = root / "results" / "result.csv"
            current.parent.mkdir(parents=True)
            current.write_bytes(b"concurrent-current")
            original_exists = Path.exists
            current_check_was_hidden = False

            def hide_initial_current_check(path):
                nonlocal current_check_was_hidden
                if path == current and not current_check_was_hidden:
                    current_check_was_hidden = True
                    return False
                return original_exists(path)

            with patch.object(Path, "exists", new=hide_initial_current_check):
                with self.assertRaises(ResultFormatError):
                    results.migrate_legacy_result(root)

            self.assertEqual(current.read_bytes(), b"concurrent-current")
            self.assertFalse((root / "results" / ".legacy_result_migrated.json").exists())


if __name__ == "__main__":
    unittest.main()
