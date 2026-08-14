import base64
import csv
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from sens_mms.config import Config
from sens_mms.inputs import MESSAGE_BODY
from sens_mms.preflight import build_preflight
from sens_mms.results import (
    ResultFormatError,
    ResultRow,
    ResultStore,
    load_failed_candidates,
)


FIRST = "01011111111"
SECOND = "01022222222"
THIRD = "01033333333"
VALID_DELIVERY_ID = "23456789ABCDEFGH"
EXPECTED_BODY = """[개업소연 안내]

안녕하세요.
세무사 윤성중 입니다.
6월 말일자로 국세청에서 퇴직하고 호연회계법인에서 새로 시작하게 되었습니다.
많은 응원 부탁드립니다.

감사합니다."""


def jpeg(comment_byte=b"A"):
    return (
        b"\xff\xd8\xff\xfe\x00\x03" + comment_byte
        + b"\xff\xc0\x00\x11\x08\x00\x0a\x00\x0a"
        + b"\x03\x01\x11\x00\x02\x11\x00\x03\x11\x00\xff\xd9"
    )


def root_with_inputs(numbers=(FIRST,)):
    root = Path(tempfile.mkdtemp())
    (root / "receiving_numbers.csv").write_text(
        "number\n" + "\n".join(numbers) + "\n", encoding="utf-8"
    )
    images = root / "mms_img"
    images.mkdir()
    (images / "mms_01_intro.jpg").write_bytes(jpeg(b"A"))
    (images / "mms_02_details.jpg").write_bytes(jpeg(b"B"))
    return root


def config():
    return Config("access", "secret", "service", "0212345678", "COMM")


def result_row(
    number,
    *,
    status="FAILED",
    attempts=3,
    delivery_id=VALID_DELIVERY_ID,
    request_id="request-1",
    message_id="message-1",
    error_status="3001",
    error_message="failed",
):
    if status == "SENT":
        return ResultRow(
            receiving_number=number,
            delivery_id=delivery_id,
            delivery_status="SENT",
            is_sent="true",
            attempts=attempts,
            request_id=request_id,
            message_id=message_id,
            error=None,
        )
    if status == "PENDING_CONFIRMATION":
        return ResultRow(
            receiving_number=number,
            delivery_id=delivery_id,
            delivery_status="PENDING_CONFIRMATION",
            is_sent="",
            attempts=attempts,
            request_id=request_id,
            message_id=message_id,
            error=None,
        )
    return ResultRow(
        receiving_number=number,
        delivery_id="" if attempts == 0 else delivery_id,
        delivery_status="FAILED",
        is_sent="false",
        attempts=attempts,
        request_id=request_id,
        message_id=message_id,
        error={"status": error_status, "message": error_message},
    )


def seed_current(root, rows):
    store = ResultStore.for_root(root)
    for row in rows:
        store.upsert(row)
    store.write_atomic()
    return store


def write_snapshot(root, name, rows):
    path = root / "results" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as file:
        fieldnames = tuple(rows[0].to_dict())
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row.to_dict())
    return path


class PreflightTests(unittest.TestCase):
    def test_public_report_has_exact_body_sender_and_only_masked_recipient(self):
        root = root_with_inputs()
        report = build_preflight(root, config(), ResultStore.for_root(root))

        public = report.to_public_dict()

        self.assertEqual(MESSAGE_BODY, EXPECTED_BODY)
        self.assertEqual(public["body"], EXPECTED_BODY)
        self.assertEqual(public["sender"], "0212345678")
        self.assertEqual(public["masked_samples"], ("*******1111",))
        self.assertEqual(public["mode"], "normal")
        self.assertIsNone(public["resend_source"])
        self.assertIsNone(public["resend_source_sha256"])
        self.assertNotIn(FIRST, json.dumps(public, ensure_ascii=False))

    def test_token_changes_when_image_bytes_change_but_size_and_dimensions_do_not(self):
        root = root_with_inputs()
        store = ResultStore.for_root(root)
        first = build_preflight(root, config(), store).approval_token

        (root / "mms_img" / "mms_01_intro.jpg").write_bytes(jpeg(b"Z"))
        second = build_preflight(root, config(), store).approval_token

        self.assertNotEqual(first, second)

    def test_build_preflight_reads_each_image_exactly_once(self):
        root = root_with_inputs()
        image_dir = root / "mms_img"
        original_read_bytes = Path.read_bytes
        reads = []

        def recording_read_bytes(path):
            if path.parent == image_dir:
                reads.append(path.name)
            return original_read_bytes(path)

        with patch.object(Path, "read_bytes", recording_read_bytes):
            build_preflight(root, config(), ResultStore.for_root(root))

        self.assertEqual(
            reads,
            ["mms_01_intro.jpg", "mms_02_details.jpg"],
        )

    def test_preflight_keeps_approved_bytes_internal_and_hashes_that_copy(self):
        root = root_with_inputs()
        approved = (root / "mms_img" / "mms_01_intro.jpg").read_bytes()

        report = build_preflight(root, config(), ResultStore.for_root(root))

        self.assertEqual(report.approved_images[0].data, approved)
        self.assertEqual(
            report.images[0]["sha256"], hashlib.sha256(approved).hexdigest()
        )
        self.assertNotIn("approved_images", report.to_public_dict())
        self.assertNotIn(
            base64.b64encode(approved).decode("ascii"),
            json.dumps(report.to_public_dict(), ensure_ascii=False),
        )

    def test_existing_failed_recipient_is_not_eligible_without_resend_mode(self):
        root = root_with_inputs()
        store = seed_current(root, (result_row(FIRST),))

        report = build_preflight(root, config(), store)

        self.assertEqual(report.mode, "normal")
        self.assertEqual(report.eligible_numbers, ())

    def test_pending_reconciliation_and_conditional_retry_are_shown_in_public_report(self):
        root = root_with_inputs()
        store = seed_current(
            root, (result_row(FIRST, status="PENDING_CONFIRMATION", attempts=1),)
        )

        public = build_preflight(root, config(), store).to_public_dict()

        self.assertEqual(public["pending_reconciliation_count"], 1)
        self.assertEqual(public["pending_masked_samples"], ("*******1111",))
        self.assertTrue(public["pending_explicit_failure_may_retry"])
        self.assertNotIn(FIRST, json.dumps(public, ensure_ascii=False))

    def test_resend_preflight_selects_every_failed_row_from_filename_latest_snapshot(self):
        root = root_with_inputs((FIRST, SECOND, THIRD))
        current = seed_current(
            root,
            (
                result_row(FIRST, status="SENT"),
                result_row(SECOND, request_id="request-2", message_id="message-2"),
                result_row(THIRD, request_id="request-3", message_id="message-3"),
            ),
        )
        older = write_snapshot(
            root,
            "result_20260814_100000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        latest = write_snapshot(
            root,
            "result_20260814_110000.csv",
            (
                result_row(THIRD, request_id="request-3", message_id="message-3"),
                result_row(FIRST, status="SENT"),
                result_row(SECOND, request_id="request-2", message_id="message-2"),
            ),
        )

        report = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(report.mode, "resend_failed")
        self.assertEqual(report.eligible_numbers, (THIRD, SECOND))
        self.assertEqual(report.resend_source, latest.name)
        self.assertNotEqual(report.resend_source, older.name)
        self.assertEqual(
            report.resend_source_sha256,
            hashlib.sha256(latest.read_bytes()).hexdigest(),
        )

    def test_resend_preflight_public_output_masks_candidates(self):
        root = root_with_inputs((SECOND, THIRD))
        current = seed_current(
            root,
            (
                result_row(SECOND, request_id="request-2", message_id="message-2"),
                result_row(THIRD, request_id="request-3", message_id="message-3"),
            ),
        )
        latest = write_snapshot(
            root,
            "result_20260814_110000.csv",
            (
                result_row(SECOND, request_id="request-2", message_id="message-2"),
                result_row(THIRD, request_id="request-3", message_id="message-3"),
            ),
        )

        public = build_preflight(
            root, config(), current, resend_failed=True
        ).to_public_dict()
        rendered = json.dumps(public, ensure_ascii=False)

        self.assertEqual(public["mode"], "resend_failed")
        self.assertEqual(public["resend_source"], latest.name)
        self.assertEqual(public["eligible_count"], 2)
        self.assertEqual(public["masked_samples"], ("*******2222", "*******3333"))
        self.assertNotIn(SECOND, rendered)
        self.assertNotIn(THIRD, rendered)

    def test_resend_token_changes_if_selected_snapshot_bytes_change(self):
        root = root_with_inputs((SECOND,))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        latest = write_snapshot(
            root,
            "result_20260814_110000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        first = build_preflight(
            root, config(), current, resend_failed=True
        ).approval_token

        latest.write_bytes(latest.read_bytes() + b"\r\n")
        second = build_preflight(
            root, config(), current, resend_failed=True
        ).approval_token

        self.assertNotEqual(first, second)

    def test_snapshot_change_during_candidate_load_blocks_with_safe_error(self):
        root = root_with_inputs((SECOND,))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        write_snapshot(
            root,
            "result_20260814_110000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )

        def load_then_rewrite(path):
            candidates = load_failed_candidates(path)
            path.write_bytes(path.read_bytes() + b"\r\n")
            return candidates

        with patch(
            "sens_mms.preflight.load_failed_candidates",
            side_effect=load_then_rewrite,
        ):
            with self.assertRaisesRegex(
                ResultFormatError,
                "^resend snapshot changed during preflight$",
            ):
                build_preflight(root, config(), current, resend_failed=True)

    def test_resend_token_binds_candidate_order_with_source_hash_held_constant(self):
        root = root_with_inputs((SECOND, THIRD))
        current = seed_current(
            root,
            (
                result_row(SECOND, request_id="request-2", message_id="message-2"),
                result_row(THIRD, request_id="request-3", message_id="message-3"),
            ),
        )
        latest = write_snapshot(
            root,
            "result_20260814_110000.csv",
            (
                result_row(SECOND, request_id="request-2", message_id="message-2"),
                result_row(THIRD, request_id="request-3", message_id="message-3"),
            ),
        )
        original_read_bytes = Path.read_bytes

        def read_with_fixed_snapshot_identity(path):
            if path == latest:
                return b"fixed-snapshot-identity"
            return original_read_bytes(path)

        with patch.object(Path, "read_bytes", read_with_fixed_snapshot_identity):
            first = build_preflight(root, config(), current, resend_failed=True)
            write_snapshot(
                root,
                latest.name,
                (
                    result_row(THIRD, request_id="request-3", message_id="message-3"),
                    result_row(SECOND, request_id="request-2", message_id="message-2"),
                ),
            )
            second = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(first.resend_source, second.resend_source)
        self.assertEqual(first.resend_source_sha256, second.resend_source_sha256)
        self.assertEqual(first.eligible_numbers, (SECOND, THIRD))
        self.assertEqual(second.eligible_numbers, (THIRD, SECOND))
        self.assertNotEqual(first.approval_token, second.approval_token)

    def test_resend_token_binds_candidate_set_with_source_hash_held_constant(self):
        root = root_with_inputs((SECOND, THIRD))
        current = seed_current(
            root,
            (
                result_row(SECOND, request_id="request-2", message_id="message-2"),
                result_row(THIRD, request_id="request-3", message_id="message-3"),
            ),
        )
        latest = write_snapshot(
            root,
            "result_20260814_110000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        original_read_bytes = Path.read_bytes

        def read_with_fixed_snapshot_identity(path):
            if path == latest:
                return b"fixed-snapshot-identity"
            return original_read_bytes(path)

        with patch.object(Path, "read_bytes", read_with_fixed_snapshot_identity):
            first = build_preflight(root, config(), current, resend_failed=True)
            write_snapshot(
                root,
                latest.name,
                (
                    result_row(SECOND, request_id="request-2", message_id="message-2"),
                    result_row(THIRD, request_id="request-3", message_id="message-3"),
                ),
            )
            second = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(first.resend_source, second.resend_source)
        self.assertEqual(first.resend_source_sha256, second.resend_source_sha256)
        self.assertEqual(first.eligible_numbers, (SECOND,))
        self.assertEqual(second.eligible_numbers, (SECOND, THIRD))
        self.assertNotEqual(first.approval_token, second.approval_token)

    def test_resend_token_binds_selected_filename_with_identical_snapshot_bytes(self):
        root = root_with_inputs((SECOND,))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        older = write_snapshot(
            root,
            "result_20260814_100000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        first = build_preflight(root, config(), current, resend_failed=True)
        latest = root / "results" / "result_20260814_110000.csv"
        latest.write_bytes(older.read_bytes())

        second = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(first.resend_source_sha256, second.resend_source_sha256)
        self.assertEqual(first.eligible_numbers, second.eligible_numbers)
        self.assertNotEqual(first.resend_source, second.resend_source)
        self.assertNotEqual(first.approval_token, second.approval_token)

    def test_corrupt_latest_snapshot_stops_without_using_older_valid_file(self):
        root = root_with_inputs((SECOND,))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        write_snapshot(
            root,
            "result_20260814_100000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        latest = root / "results" / "result_20260814_110000.csv"
        latest.write_bytes(b"corrupt")

        with self.assertRaises(ResultFormatError):
            build_preflight(root, config(), current, resend_failed=True)

    def test_missing_snapshot_blocks_resend_preflight(self):
        root = root_with_inputs((SECOND,))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )

        with self.assertRaises(ResultFormatError):
            build_preflight(root, config(), current, resend_failed=True)

    def test_resend_preflight_never_includes_validation_failure_rows(self):
        root = root_with_inputs((SECOND, "not-a-number"))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        write_snapshot(
            root,
            "result_20260814_110000.csv",
            (
                result_row(
                    "not-a-number",
                    attempts=0,
                    request_id="",
                    message_id="",
                    error_status="VALIDATION_ERROR",
                ),
                result_row(SECOND, request_id="request-2", message_id="message-2"),
            ),
        )

        report = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(report.total, 2)
        self.assertEqual(report.eligible_numbers, (SECOND,))
        self.assertNotIn("not-a-number", json.dumps(report.to_public_dict()))

    def test_resend_excludes_api_failure_recipient_absent_from_valid_input(self):
        root = root_with_inputs((FIRST,))
        current = seed_current(
            root,
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )
        write_snapshot(
            root,
            "result_20260814_110000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )

        report = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(report.eligible_numbers, ())

    def test_resend_blocks_when_send_eligible_candidate_has_no_current_row(self):
        root = root_with_inputs((SECOND,))
        current = ResultStore.for_root(root)
        write_snapshot(
            root,
            "result_20260814_110000.csv",
            (result_row(SECOND, request_id="request-2", message_id="message-2"),),
        )

        with self.assertRaisesRegex(
            ResultFormatError,
            "^resend snapshot does not match current checkpoint$",
        ):
            build_preflight(root, config(), current, resend_failed=True)

    def test_resend_snapshot_must_match_current_checkpoint_status_and_api_ids(self):
        cases = (
            {"status": "SENT", "request_id": "request-2", "message_id": "message-2"},
            {"status": "FAILED", "request_id": "different", "message_id": "message-2"},
            {"status": "FAILED", "request_id": "request-2", "message_id": "different"},
        )
        for current_fields in cases:
            with self.subTest(current_fields=current_fields):
                root = root_with_inputs((SECOND,))
                current = seed_current(root, (result_row(SECOND, **current_fields),))
                write_snapshot(
                    root,
                    "result_20260814_110000.csv",
                    (
                        result_row(
                            SECOND,
                            request_id="request-2",
                            message_id="message-2",
                        ),
                    ),
                )

                with self.assertRaisesRegex(
                    ResultFormatError,
                    "^resend snapshot does not match current checkpoint$",
                ):
                    build_preflight(root, config(), current, resend_failed=True)

    def test_resend_snapshot_must_match_candidate_delivery_id_attempts_and_error(self):
        changes = (
            {"delivery_id": "3456789ABCDEFGHJ"},
            {"attempts": 2},
            {"error_status": "3002", "error_message": "different failure"},
        )
        for changed_fields in changes:
            with self.subTest(changed_fields=changed_fields):
                root = root_with_inputs((SECOND,))
                current_row = result_row(
                    SECOND,
                    request_id="request-2",
                    message_id="message-2",
                    **changed_fields,
                )
                current = seed_current(root, (current_row,))
                write_snapshot(
                    root,
                    "result_20260814_110000.csv",
                    (
                        result_row(
                            SECOND,
                            request_id="request-2",
                            message_id="message-2",
                        ),
                    ),
                )

                with self.assertRaisesRegex(
                    ResultFormatError,
                    "^resend snapshot does not match current checkpoint$",
                ):
                    build_preflight(root, config(), current, resend_failed=True)

    def test_approval_token_binds_every_exact_existing_row_field(self):
        changes = (
            {"delivery_id": "3456789ABCDEFGHJ"},
            {"attempts": 2},
            {"error_status": "3002", "error_message": "different failure"},
        )
        for changed_fields in changes:
            with self.subTest(changed_fields=changed_fields):
                root = root_with_inputs((FIRST,))
                original = seed_current(root, (result_row(FIRST),))
                first_token = build_preflight(root, config(), original).approval_token
                changed = seed_current(
                    root,
                    (result_row(FIRST, **changed_fields),),
                )

                second_token = build_preflight(
                    root, config(), changed
                ).approval_token

                self.assertNotEqual(first_token, second_token)


if __name__ == "__main__":
    unittest.main()
