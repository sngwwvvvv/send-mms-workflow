import base64
import csv
import hashlib
import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from typing import get_type_hints
from unittest.mock import patch

from sens_mms.config import Config
from sens_mms.coordination import RUN_SETTINGS
from sens_mms.inputs import ImageInfo, MESSAGE_BODY
from sens_mms.preflight import ApprovedWork, PreflightReport, build_preflight
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
국세청에서의 오랜 경험을 바탕으로 호연회계법인에서 새로운 출발을 하게 된 윤성중 세무사입니다.

그동안 보내주신 관심과 성원에 감사드리며, 앞으로도 많은 관심과 응원 부탁드립니다.

새로운 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다.
"""


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


def pending_with_ids(
    number,
    *,
    attempts=1,
    delivery_id=VALID_DELIVERY_ID,
    request_id="request-1",
    message_id="message-1",
):
    return result_row(
        number,
        status="PENDING_CONFIRMATION",
        attempts=attempts,
        delivery_id=delivery_id,
        request_id=request_id,
        message_id=message_id,
    )


def pending_reservation(number, *, delivery_id=VALID_DELIVERY_ID):
    return pending_with_ids(
        number,
        attempts=0,
        delivery_id=delivery_id,
        request_id="",
        message_id="",
    )


def pending_blank_ids(number, *, attempts=1, delivery_id=VALID_DELIVERY_ID):
    return pending_with_ids(
        number,
        attempts=attempts,
        delivery_id=delivery_id,
        request_id="",
        message_id="",
    )


def validation_failure_row(number):
    return result_row(
        number,
        attempts=0,
        request_id="",
        message_id="",
        error_status="VALIDATION_ERROR",
        error_message="수신번호 검증 실패",
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
    def test_preflight_report_exposes_immutable_approved_work_items(self):
        """Catches callers losing the ApprovedWork action contract to bare tuple."""
        hints = get_type_hints(PreflightReport)

        self.assertEqual(hints["work_items"], tuple[ApprovedWork, ...])

    def test_preflight_classifies_pending_without_weakening_no_resend(self):
        root = root_with_inputs((FIRST, SECOND, THIRD))
        seed_current(
            root,
            (
                pending_with_ids(FIRST, attempts=1, delivery_id="23456789ABCDEFGH"),
                pending_reservation(SECOND, delivery_id="3456789ABCDEFGHJ"),
                pending_blank_ids(THIRD, attempts=1, delivery_id="456789ABCDEFGHJK"),
            ),
        )

        report = build_preflight(root, config(), ResultStore.for_root(root))

        self.assertEqual(
            tuple(
                (
                    work.receiving_number,
                    work.action,
                    work.allow_retry_after_explicit_failure,
                    work.source_delivery_id,
                )
                for work in report.work_items
            ),
            (
                (FIRST, "RECONCILE", True, "23456789ABCDEFGH"),
                (SECOND, "RESUME_RESERVATION", False, "3456789ABCDEFGHJ"),
                (THIRD, "HOLD_AMBIGUOUS", False, "456789ABCDEFGHJK"),
            ),
        )
        self.assertEqual(report.pending_numbers, (FIRST, SECOND, THIRD))

    def test_preflight_includes_every_absent_durable_pending_before_new_input(self):
        """Catches durable pending work disappearing when it leaves the current CSV."""
        fourth = "01044444444"
        root = root_with_inputs((SECOND,))
        seed_current(
            root,
            (
                pending_with_ids(
                    FIRST,
                    attempts=1,
                    delivery_id="23456789ABCDEFGH",
                ),
                pending_reservation(
                    THIRD,
                    delivery_id="3456789ABCDEFGHJ",
                ),
                pending_blank_ids(
                    fourth,
                    attempts=1,
                    delivery_id="456789ABCDEFGHJK",
                ),
            ),
        )

        report = build_preflight(root, config(), ResultStore.for_root(root))
        public = report.to_public_dict()

        self.assertEqual(
            tuple(
                (
                    item.receiving_number,
                    item.action,
                    item.allow_retry_after_explicit_failure,
                )
                for item in report.work_items
            ),
            (
                (FIRST, "RECONCILE", False),
                (THIRD, "HOLD_AMBIGUOUS", False),
                (fourth, "HOLD_AMBIGUOUS", False),
                (SECOND, "START_FRESH", True),
            ),
        )
        self.assertEqual(report.pending_numbers, (FIRST, THIRD, fourth))
        self.assertEqual(report.total, 4)
        self.assertEqual(public["pending_reconciliation_count"], 1)
        self.assertEqual(public["pending_masked_samples"], ("*******1111",))
        self.assertFalse(public["pending_explicit_failure_may_retry"])
        self.assertEqual(public["reservation_resume_count"], 0)
        self.assertEqual(public["ambiguous_hold_count"], 2)
        self.assertEqual(
            public["ambiguous_hold_masked_samples"],
            ("*******3333", "*******4444"),
        )
        rendered = json.dumps(public, ensure_ascii=False)
        for number in (FIRST, SECOND, THIRD, fourth):
            self.assertNotIn(number, rendered)

    def test_preflight_classifies_new_input_as_start_fresh_with_retry_allowed(self):
        root = root_with_inputs((FIRST,))

        report = build_preflight(root, config(), ResultStore.for_root(root))

        self.assertEqual(
            report.work_items,
            (ApprovedWork(FIRST, "START_FRESH", True, ""),),
        )
        self.assertEqual(report.eligible_numbers, (FIRST,))
        self.assertEqual(report.content_type, "COMM")

    def test_preflight_excludes_sent_failed_and_validation_failure_rows_without_resend(self):
        root = root_with_inputs((FIRST, SECOND, THIRD))
        store = seed_current(
            root,
            (
                result_row(FIRST, status="SENT"),
                result_row(SECOND),
                validation_failure_row(THIRD),
            ),
        )

        report = build_preflight(root, config(), store)

        self.assertEqual(report.work_items, ())
        self.assertEqual(report.eligible_numbers, ())

    def test_preflight_classifies_pending_request_only_as_reconcile(self):
        root = root_with_inputs((FIRST,))
        seed_current(
            root,
            (
                pending_with_ids(
                    FIRST,
                    attempts=2,
                    request_id="request-only",
                    message_id="",
                    delivery_id="23456789ABCDEFGH",
                ),
            ),
        )

        report = build_preflight(root, config(), ResultStore.for_root(root))

        self.assertEqual(
            report.work_items,
            (ApprovedWork(FIRST, "RECONCILE", True, "23456789ABCDEFGH"),),
        )

    def test_preflight_rejects_pending_message_without_request_id(self):
        root = root_with_inputs((FIRST,))
        seed_current(
            root,
            (
                pending_with_ids(
                    FIRST,
                    request_id="",
                    message_id="message-without-request",
                ),
            ),
        )

        with self.assertRaisesRegex(
            ResultFormatError,
            "^pending result row has message_id without request_id$",
        ):
            build_preflight(root, config(), ResultStore.for_root(root))

    def test_resend_preflight_classifies_snapshot_failed_rows_as_start_fresh(self):
        root = root_with_inputs((FIRST, SECOND))
        current = seed_current(
            root,
            (
                result_row(FIRST, request_id="request-1", message_id="message-1"),
                result_row(SECOND, request_id="request-2", message_id="message-2"),
            ),
        )
        write_snapshot(
            root,
            "result_20260814_110000.csv",
            (
                validation_failure_row("not-a-number"),
                result_row(FIRST, request_id="request-1", message_id="message-1"),
                result_row(SECOND, request_id="request-2", message_id="message-2"),
            ),
        )

        report = build_preflight(root, config(), current, resend_failed=True)

        self.assertEqual(
            report.work_items,
            (
                ApprovedWork(FIRST, "START_FRESH", True, VALID_DELIVERY_ID),
                ApprovedWork(SECOND, "START_FRESH", True, VALID_DELIVERY_ID),
            ),
        )
        self.assertEqual(report.eligible_numbers, (FIRST, SECOND))

    def test_public_report_has_exact_body_sender_settings_and_only_masked_samples(self):
        root = root_with_inputs((FIRST, SECOND, THIRD))
        seed_current(
            root,
            (
                pending_with_ids(FIRST, delivery_id="23456789ABCDEFGH"),
                pending_reservation(SECOND, delivery_id="3456789ABCDEFGHJ"),
                pending_blank_ids(THIRD, delivery_id="456789ABCDEFGHJK"),
            ),
        )

        report = build_preflight(root, config(), ResultStore.for_root(root))
        public = report.to_public_dict()

        self.assertEqual(MESSAGE_BODY, EXPECTED_BODY)
        self.assertEqual(public["body"], EXPECTED_BODY)
        self.assertEqual(public["sender"], "0212345678")
        self.assertEqual(public["type"], "MMS")
        self.assertIsNone(public["subject"])
        self.assertEqual(public["contentType"], "COMM")
        self.assertEqual(
            public["settings"],
            {
                "worker_count": 5,
                "poll_interval_seconds": 1,
                "confirmation_timeout_seconds": 120,
                "retry_delay_seconds": 10,
                "max_attempts": 3,
                "rate_limit_delays_seconds": [10, 20],
            },
        )
        self.assertEqual(public["pending_reconciliation_count"], 1)
        self.assertEqual(public["pending_masked_samples"], ("*******1111",))
        self.assertEqual(public["reservation_resume_count"], 1)
        self.assertEqual(public["reservation_resume_masked_samples"], ("*******2222",))
        self.assertEqual(public["ambiguous_hold_count"], 1)
        self.assertEqual(public["ambiguous_hold_masked_samples"], ("*******3333",))
        self.assertEqual(public["eligible_count"], 0)
        self.assertEqual(public["masked_samples"], ())
        self.assertEqual(
            public["images"],
            (
                {
                    "order": 1,
                    "name": "mms_01_intro.jpg",
                    "bytes": len(jpeg(b"A")),
                    "width": 10,
                    "height": 10,
                    "sha256": hashlib.sha256(jpeg(b"A")).hexdigest(),
                },
                {
                    "order": 2,
                    "name": "mms_02_details.jpg",
                    "bytes": len(jpeg(b"B")),
                    "width": 10,
                    "height": 10,
                    "sha256": hashlib.sha256(jpeg(b"B")).hexdigest(),
                },
            ),
        )
        self.assertEqual(
            tuple(image.data for image in report.approved_images),
            (jpeg(b"A"), jpeg(b"B")),
        )

    def test_public_report_never_emits_private_identifiers(self):
        root = root_with_inputs((FIRST, SECOND))
        seed_current(
            root,
            (
                pending_with_ids(FIRST, delivery_id="23456789ABCDEFGH"),
                pending_reservation(SECOND, delivery_id="3456789ABCDEFGHJ"),
            ),
        )

        public = build_preflight(
            root, config(), ResultStore.for_root(root)
        ).to_public_dict()
        rendered = json.dumps(public, ensure_ascii=False)

        for forbidden in (
            FIRST,
            SECOND,
            "23456789ABCDEFGH",
            "3456789ABCDEFGHJ",
            "request-1",
            "message-1",
        ):
            self.assertNotIn(forbidden, rendered)

    def test_approved_work_hides_private_fields_in_repr(self):
        work = ApprovedWork(FIRST, "RECONCILE", True, VALID_DELIVERY_ID)

        self.assertNotIn(FIRST, repr(work))
        self.assertNotIn(VALID_DELIVERY_ID, repr(work))

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

    def test_token_changes_when_work_item_action_retry_permission_or_source_delivery_id_changes(self):
        root = root_with_inputs((FIRST,))
        baseline_store = seed_current(root, (pending_with_ids(FIRST),))
        first = build_preflight(root, config(), baseline_store).approval_token

        hold_store = seed_current(root, (pending_blank_ids(FIRST),))
        hold = build_preflight(root, config(), hold_store).approval_token

        no_retry_store = seed_current(root, (pending_with_ids(FIRST, attempts=3),))
        no_retry = build_preflight(root, config(), no_retry_store).approval_token

        different_delivery_store = seed_current(
            root,
            (
                pending_with_ids(
                    FIRST,
                    delivery_id="3456789ABCDEFGHJ",
                ),
            ),
        )
        different_delivery = build_preflight(
            root, config(), different_delivery_store
        ).approval_token

        self.assertNotEqual(first, hold)
        self.assertNotEqual(first, no_retry)
        self.assertNotEqual(first, different_delivery)

    def test_token_changes_when_each_fixed_run_setting_changes(self):
        """Catches a canonical token that omits any worker run setting."""
        root = root_with_inputs((FIRST,))
        store = ResultStore.for_root(root)
        first = build_preflight(root, config(), store).approval_token
        mutations = (
            {"worker_count": 6},
            {"poll_interval_seconds": 2},
            {"confirmation_timeout_seconds": 121},
            {"retry_delay_seconds": 11},
            {"max_attempts": 4},
            {"rate_limit_delays_seconds": (11, 20)},
            {"rate_limit_delays_seconds": (10, 21)},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                with patch(
                    "sens_mms.preflight.RUN_SETTINGS",
                    replace(RUN_SETTINGS, **mutation),
                ):
                    changed = build_preflight(root, config(), store).approval_token

                self.assertNotEqual(first, changed)

    def test_token_changes_when_body_changes(self):
        """Catches a canonical token that omits the operator-approved body."""
        root = root_with_inputs((FIRST,))
        store = ResultStore.for_root(root)
        first = build_preflight(root, config(), store).approval_token

        with patch("sens_mms.preflight.MESSAGE_BODY", "operator-approved body changed"):
            changed_report = build_preflight(root, config(), store)

        self.assertEqual(changed_report.body, "operator-approved body changed")
        self.assertNotEqual(first, changed_report.approval_token)

    def test_token_changes_when_public_image_metadata_changes(self):
        """Catches canonicalization that omits image name, size, dimensions, hash, or order."""
        root = root_with_inputs((FIRST,))
        store = ResultStore.for_root(root)
        first = build_preflight(root, config(), store)
        original_images = first.approved_images
        mutations = (
            ImageInfo(
                "renamed.jpg", original_images[0].path, original_images[0].data, 10, 10
            ),
            ImageInfo(
                "mms_01_intro.jpg",
                original_images[0].path,
                jpeg(b"Z") + b"\x00",
                10,
                10,
            ),
            ImageInfo(
                "mms_01_intro.jpg", original_images[0].path, original_images[0].data, 11, 10
            ),
            ImageInfo(
                "mms_01_intro.jpg", original_images[0].path, original_images[0].data, 10, 11
            ),
        )
        for mutated_first_image in mutations:
            with self.subTest(image=mutated_first_image):
                with patch(
                    "sens_mms.preflight.validate_images",
                    return_value=(mutated_first_image, original_images[1]),
                ):
                    changed = build_preflight(root, config(), store)

                self.assertNotEqual(first.approval_token, changed.approval_token)

        with patch(
            "sens_mms.preflight.validate_images",
            return_value=(original_images[1], original_images[0]),
        ):
            reordered = build_preflight(root, config(), store)
        self.assertNotEqual(first.approval_token, reordered.approval_token)

    def test_token_changes_when_approved_content_type_changes(self):
        root = root_with_inputs((FIRST,))
        store = ResultStore.for_root(root)
        first = build_preflight(root, config(), store).approval_token

        with patch("sens_mms.preflight.APPROVED_CONTENT_TYPE", "AD"):
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

    def test_approval_token_binds_every_valid_result_row_field(self):
        """Catches an approval token that omits a checkpoint identity field."""
        root = root_with_inputs((FIRST, SECOND))
        original = seed_current(root, (result_row(FIRST),))
        first_token = build_preflight(root, config(), original).approval_token
        valid_mutations = (
            result_row(SECOND),
            result_row(FIRST, delivery_id="3456789ABCDEFGHJ"),
            result_row(FIRST, status="SENT"),
            result_row(FIRST, status="PENDING_CONFIRMATION", attempts=1),
            result_row(FIRST, attempts=2),
            result_row(FIRST, request_id="request-2"),
            result_row(FIRST, message_id="message-2"),
            result_row(
                FIRST,
                error_status="3002",
                error_message="different failure",
            ),
        )
        for changed_row in valid_mutations:
            with self.subTest(changed_row=changed_row):
                changed = seed_current(root, (changed_row,))
                changed_token = build_preflight(root, config(), changed).approval_token

                self.assertNotEqual(first_token, changed_token)


if __name__ == "__main__":
    unittest.main()
