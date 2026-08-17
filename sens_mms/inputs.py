from dataclasses import dataclass
from pathlib import Path
import csv, re

MESSAGE_BODY = """[개업소연 안내]

안녕하세요.
국세청에서의 오랜 경험을 바탕으로 호연회계법인에서 새로운 출발을 하게 된 윤성중 세무사입니다.

그동안 보내주신 관심과 성원에 감사드리며, 앞으로도 많은 관심과 응원 부탁드립니다.

새로운 시작을 기쁜 마음으로 함께해 주시면 감사하겠습니다.
"""


@dataclass(frozen=True)
class ValidationFailure:
    receiving_number: str
    attempts: int = 0
    error_status: str = "VALIDATION_ERROR"
    error_message: str = "수신번호 검증 실패"


@dataclass(frozen=True)
class RecipientSet:
    valid_numbers: tuple
    failures: tuple


@dataclass(frozen=True)
class ImageInfo:
    name: str
    path: Path
    data: bytes
    width: int
    height: int

    @property
    def bytes(self) -> int:
        return len(self.data)


def load_recipients(path):
    data = Path(path).read_bytes()
    try:
        raw = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raw = data.decode("cp949")
    first = raw.splitlines()[0]
    delim = "\t" if "\t" in first else ","
    rows = list(csv.DictReader(raw.splitlines(), delimiter=delim))
    if not rows or "number" not in rows[0]:
        raise ValueError("number column missing")
    seen = set()
    valid = []
    failures = []
    for row in rows:
        original = str(row.get("number", ""))
        n = re.sub(r"[\s-]", "", original.strip())
        if not n or not n.isdigit():
            failures.append(ValidationFailure(original))
            continue
        if n not in seen:
            seen.add(n)
            valid.append(n)
    return RecipientSet(tuple(valid), tuple(failures))


def _jpeg_size(data):
    if data[:2] != b"\xff\xd8":
        raise ValueError("not JPEG")
    i = 2
    while i < len(data) - 9:
        if data[i] != 0xFF:
            i += 1
            continue
        marker = data[i + 1]
        i += 2
        if marker in (0xD8, 0xD9):
            continue
        ln = int.from_bytes(data[i : i + 2], "big")
        if 0xC0 <= marker <= 0xC3:
            return int.from_bytes(data[i + 5 : i + 7], "big"), int.from_bytes(
                data[i + 3 : i + 5], "big"
            )
        i += ln
    raise ValueError("JPEG dimensions unavailable")


def validate_images(directory):
    infos = []
    for name in ("mms_01_intro.jpg", "mms_02_details.jpg"):
        p = Path(directory) / name
        if not p.exists():
            raise ValueError(f"missing image: {name}")
        data = p.read_bytes()
        w, h = _jpeg_size(data)
        if len(data) > 300 * 1024 or w > 1500 or h > 1440:
            raise ValueError(f"image limits exceeded: {name}")
        infos.append(ImageInfo(name, p, data, w, h))
    extras = [
        p
        for p in Path(directory).iterdir()
        if p.is_file()
        and p.suffix.lower() in (".jpg", ".jpeg")
        and p.name not in ("mms_01_intro.jpg", "mms_02_details.jpg")
    ]
    if extras:
        raise ValueError("exactly two JPEG images required")
    return tuple(infos)
