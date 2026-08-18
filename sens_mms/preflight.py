import base64
from dataclasses import asdict, dataclass, field
from pathlib import Path
import hashlib, json
from typing import Literal

from .coordination import RUN_SETTINGS
from .inputs import (
    MESSAGE_BODY,
    MESSAGE_CONTENT,
    MESSAGE_SUBJECT,
    load_recipients,
    validate_images,
)
from .results import (
    ResultFormatError,
    ResultRow,
    load_failed_candidates,
    select_latest_snapshot,
)


APPROVED_CONTENT_TYPE = "COMM"
WorkAction = Literal[
    "RECONCILE",
    "RESUME_RESERVATION",
    "START_FRESH",
    "HOLD_AMBIGUOUS",
    "RETRY_EXPLICIT",
]


@dataclass(frozen=True)
class ApprovedWork:
    receiving_number: str = field(repr=False)
    action: WorkAction
    allow_retry_after_explicit_failure: bool
    source_delivery_id: str = field(default="", repr=False)
    retry_not_before: float | None = field(default=None, repr=False)


@dataclass(frozen=True)
class PreflightReport:
    total: int
    eligible_numbers: tuple
    eligible_rows: tuple
    pending_numbers: tuple
    work_items: tuple[ApprovedWork, ...]
    masked_samples: tuple
    sender: str
    body: str
    subject: str
    content: str
    content_type: str
    images: tuple
    approved_images: tuple = field(repr=False)
    approval_token: str
    mode: str
    resend_source: str | None
    resend_source_sha256: str | None
    approved_result_state: str = field(repr=False)

    def to_public_dict(self):
        work_groups = {
            "RECONCILE": tuple(
                work for work in self.work_items if work.action == "RECONCILE"
            ),
            "RESUME_RESERVATION": tuple(
                work for work in self.work_items if work.action == "RESUME_RESERVATION"
            ),
            "HOLD_AMBIGUOUS": tuple(
                work for work in self.work_items if work.action == "HOLD_AMBIGUOUS"
            ),
            "START_FRESH": tuple(
                work for work in self.work_items if work.action == "START_FRESH"
            ),
        }
        return {
            "total": self.total,
            "eligible_count": len(self.eligible_numbers),
            "mode": self.mode,
            "resend_source": self.resend_source,
            "resend_source_sha256": self.resend_source_sha256,
            "masked_samples": self.masked_samples,
            "fresh_start_count": len(work_groups["START_FRESH"]),
            "fresh_start_masked_samples": tuple(
                mask_number(work.receiving_number)
                for work in work_groups["START_FRESH"][:3]
            ),
            "pending_reconciliation_count": len(work_groups["RECONCILE"]),
            "pending_masked_samples": tuple(
                mask_number(work.receiving_number)
                for work in work_groups["RECONCILE"][:3]
            ),
            "pending_explicit_failure_may_retry": any(
                work.allow_retry_after_explicit_failure
                for work in work_groups["RECONCILE"]
            ),
            "reservation_resume_count": len(work_groups["RESUME_RESERVATION"]),
            "reservation_resume_masked_samples": tuple(
                mask_number(work.receiving_number)
                for work in work_groups["RESUME_RESERVATION"][:3]
            ),
            "ambiguous_hold_count": len(work_groups["HOLD_AMBIGUOUS"]),
            "ambiguous_hold_masked_samples": tuple(
                mask_number(work.receiving_number)
                for work in work_groups["HOLD_AMBIGUOUS"][:3]
            ),
            "sender": self.sender,
            "body": self.body,
            "subject": self.subject,
            "content": self.content,
            "type": "MMS",
            "contentType": self.content_type,
            "images": self.images,
            "settings": {
                "worker_count": RUN_SETTINGS.worker_count,
                "poll_interval_seconds": RUN_SETTINGS.poll_interval_seconds,
                "confirmation_timeout_seconds": RUN_SETTINGS.confirmation_timeout_seconds,
                "retry_delay_seconds": RUN_SETTINGS.retry_delay_seconds,
                "max_attempts": RUN_SETTINGS.max_attempts,
                "rate_limit_delays_seconds": list(
                    RUN_SETTINGS.rate_limit_delays_seconds
                ),
            },
            "approval_token": self.approval_token,
        }


def mask_number(n):
    return "*" * max(0, len(n) - 4) + n[-4:]


def result_row_identity(row):
    return {
        "receiving_number": row.receiving_number,
        "delivery_id": row.delivery_id,
        "delivery_status": row.delivery_status,
        "is_sent": row.is_sent,
        "attempts": row.attempts,
        "request_id": row.request_id,
        "message_id": row.message_id,
        "error": row.error,
    }


def canonical_result_state(rows) -> str:
    return json.dumps(
        [
            result_row_identity(row)
            for row in sorted(rows, key=lambda row: row.receiving_number)
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def classify_existing(row: ResultRow) -> ApprovedWork:
    if row.delivery_status != "PENDING_CONFIRMATION":
        raise ResultFormatError("result row is not pending")
    if row.attempts == 0:
        return ApprovedWork(
            row.receiving_number,
            "RESUME_RESERVATION",
            False,
            row.delivery_id,
        )
    if not row.request_id:
        if row.message_id:
            raise ResultFormatError("pending result row has message_id without request_id")
        return ApprovedWork(
            row.receiving_number,
            "HOLD_AMBIGUOUS",
            False,
            row.delivery_id,
        )
    return ApprovedWork(
        row.receiving_number,
        "RECONCILE",
        row.attempts < RUN_SETTINGS.max_attempts,
        row.delivery_id,
    )


def classify_absent_pending(row: ResultRow) -> ApprovedWork:
    classified = classify_existing(row)
    if classified.action == "RECONCILE":
        return ApprovedWork(
            row.receiving_number,
            "RECONCILE",
            False,
            row.delivery_id,
        )
    return ApprovedWork(
        row.receiving_number,
        "HOLD_AMBIGUOUS",
        False,
        row.delivery_id,
    )


def build_preflight(root, config, result_store, *, resend_failed=False):
    project_root = Path(root)
    recipients = load_recipients(project_root / "receiving_numbers.csv")
    images = validate_images(project_root / "mms_img")
    existing = result_store.load()
    valid_numbers = set(recipients.valid_numbers)
    source_path = None
    source_hash = None
    if resend_failed:
        try:
            source_path = select_latest_snapshot(project_root / "results")
            source_bytes = source_path.read_bytes()
            source_hash = hashlib.sha256(source_bytes).hexdigest()
        except OSError as exc:
            raise ResultFormatError("no result snapshot found") from exc
        try:
            failed_candidates = load_failed_candidates(source_path)
        except ResultFormatError:
            try:
                source_changed = source_path.read_bytes() != source_bytes
            except OSError:
                source_changed = True
            if source_changed:
                raise ResultFormatError(
                    "resend snapshot changed during preflight"
                ) from None
            raise
        try:
            source_changed = source_path.read_bytes() != source_bytes
        except OSError:
            source_changed = True
        if source_changed:
            raise ResultFormatError(
                "resend snapshot changed during preflight"
            ) from None
        eligible_rows = []
        for row in failed_candidates:
            if (
                row.attempts == 0
                and row.error is not None
                and row.error["status"] == "VALIDATION_ERROR"
            ):
                continue
            if row.receiving_number not in valid_numbers:
                continue
            current = existing.get(row.receiving_number)
            if current != row:
                raise ResultFormatError(
                    "resend snapshot does not match current checkpoint"
                )
            eligible_rows.append(row)
        eligible = tuple(row.receiving_number for row in eligible_rows)
    else:
        eligible_rows = []
        eligible = ()
    image_data = tuple(
        {
            "order": order,
            "name": i.name,
            "bytes": i.bytes,
            "width": i.width,
            "height": i.height,
            "sha256": hashlib.sha256(i.data).hexdigest(),
        }
        for order, i in enumerate(images, start=1)
    )
    work_items = []
    pending = []
    resend_numbers = {row.receiving_number for row in eligible_rows}
    for number in sorted(existing):
        row = existing[number]
        if row.delivery_status != "PENDING_CONFIRMATION":
            continue
        pending.append(number)
        work_items.append(
            classify_existing(row)
            if number in valid_numbers
            else classify_absent_pending(row)
        )
    for number in recipients.valid_numbers:
        row = existing.get(number)
        if row is None:
            if not resend_failed:
                work_items.append(ApprovedWork(number, "START_FRESH", True, ""))
                eligible += (number,)
            continue
        if row.delivery_status == "PENDING_CONFIRMATION":
            continue
        if resend_failed and number in resend_numbers:
            work_items.append(ApprovedWork(number, "START_FRESH", True, row.delivery_id))
    pending = tuple(pending)
    work_items = tuple(work_items)
    canonical = {
        "sender": config.from_number,
        "body": MESSAGE_BODY,
        "subject": MESSAGE_SUBJECT,
        "content": MESSAGE_CONTENT,
        "type": "MMS",
        "contentType": APPROVED_CONTENT_TYPE,
        "images": [
            {
                **metadata,
                "dataBase64": base64.b64encode(image.data).decode("ascii"),
            }
            for metadata, image in zip(image_data, images)
        ],
        "workItems": [
            {
                "receivingNumber": work.receiving_number,
                "action": work.action,
                "allowRetryAfterExplicitFailure": work.allow_retry_after_explicit_failure,
                "sourceDeliveryId": work.source_delivery_id,
            }
            for work in work_items
        ],
        "settings": asdict(RUN_SETTINGS),
        "existing": [
            result_row_identity(existing[number]) for number in sorted(existing)
        ],
    }
    canonical.update({
        "mode": "resend_failed" if resend_failed else "normal",
        "resendSource": source_path.name if source_path else None,
        "resendSourceSha256": source_hash if source_path else None,
        "eligible": eligible,
    })
    token = hashlib.sha256(
        json.dumps(
            canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    return PreflightReport(
        total=(
            len(recipients.valid_numbers)
            + len(recipients.failures)
            + sum(
                row.delivery_status == "PENDING_CONFIRMATION"
                and number not in valid_numbers
                for number, row in existing.items()
            )
        ),
        eligible_numbers=eligible,
        eligible_rows=tuple(eligible_rows),
        pending_numbers=pending,
        work_items=work_items,
        masked_samples=tuple(mask_number(n) for n in eligible[:3]),
        sender=config.from_number,
        body=MESSAGE_BODY,
        subject=MESSAGE_SUBJECT,
        content=MESSAGE_CONTENT,
        content_type=APPROVED_CONTENT_TYPE,
        images=image_data,
        approved_images=images,
        approval_token=token,
        mode="resend_failed" if resend_failed else "normal",
        resend_source=source_path.name if source_path else None,
        resend_source_sha256=source_hash,
        approved_result_state=canonical_result_state(existing.values()),
    )
