from dataclasses import dataclass, field
from pathlib import Path
import hashlib, json
from .inputs import load_recipients, validate_images, MESSAGE_BODY
from .results import (
    ResultFormatError,
    load_failed_candidates,
    select_latest_snapshot,
)


@dataclass(frozen=True)
class PreflightReport:
    total: int
    eligible_numbers: tuple
    eligible_rows: tuple
    pending_numbers: tuple
    masked_samples: tuple
    sender: str
    body: str
    images: tuple
    approved_images: tuple = field(repr=False)
    approval_token: str
    mode: str
    resend_source: str | None
    resend_source_sha256: str | None

    def to_public_dict(self):
        return {
            "total": self.total,
            "eligible_count": len(self.eligible_numbers),
            "mode": self.mode,
            "resend_source": self.resend_source,
            "resend_source_sha256": self.resend_source_sha256,
            "masked_samples": self.masked_samples,
            "pending_reconciliation_count": len(self.pending_numbers),
            "pending_masked_samples": tuple(
                mask_number(n) for n in self.pending_numbers[:3]
            ),
            "pending_explicit_failure_may_retry": bool(self.pending_numbers),
            "sender": self.sender,
            "body": self.body,
            "type": "MMS",
            "contentType": "COMM",
            "images": self.images,
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


def build_preflight(root, config, result_store, *, resend_failed=False):
    project_root = Path(root)
    recipients = load_recipients(project_root / "receiving_numbers.csv")
    images = validate_images(project_root / "mms_img")
    existing = result_store.load()
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
        valid_numbers = set(recipients.valid_numbers)
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
        eligible = tuple(n for n in recipients.valid_numbers if n not in existing)
    image_data = tuple(
        {
            "name": i.name,
            "bytes": i.bytes,
            "width": i.width,
            "height": i.height,
            "sha256": hashlib.sha256(i.data).hexdigest(),
        }
        for i in images
    )
    pending = tuple(
        n
        for n in recipients.valid_numbers
        if n in existing and existing[n].delivery_status == "PENDING_CONFIRMATION"
    )
    canonical = {
        "sender": config.from_number,
        "body": MESSAGE_BODY,
        "contentType": "COMM",
        "images": image_data,
        "pending": pending,
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
        total=len(recipients.valid_numbers) + len(recipients.failures),
        eligible_numbers=eligible,
        eligible_rows=tuple(eligible_rows),
        pending_numbers=pending,
        masked_samples=tuple(mask_number(n) for n in eligible[:3]),
        sender=config.from_number,
        body=MESSAGE_BODY,
        images=image_data,
        approved_images=images,
        approval_token=token,
        mode="resend_failed" if resend_failed else "normal",
        resend_source=source_path.name if source_path else None,
        resend_source_sha256=source_hash,
    )
