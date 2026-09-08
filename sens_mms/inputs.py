from dataclasses import dataclass
from pathlib import Path
import csv, re
import unicodedata


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


class TemplateError(ValueError):
    """A fixed, safe local template validation error."""


@dataclass(frozen=True)
class LoadedTemplate:
    name: str
    message_bytes: bytes
    content: str
    images: tuple[ImageInfo, ...]


def _safe_child(parent, name):
    if (
        type(name) is not str or not name or name in {".", ".."}
        or any(c in name for c in '/\\:')
        or name.endswith((".", " "))
        or any(unicodedata.category(c).startswith("C") for c in name)
    ):
        raise TemplateError("template and file names must be safe direct names")
    path = parent / name
    if (
        path.is_symlink() or path.is_junction()
        or path.resolve().parent != parent.resolve()
    ):
        raise TemplateError("template folders and files must stay inside input")
    return path


def _input_directory(root):
    directory = _safe_child(Path(root).resolve(), "input")
    if not directory.is_dir():
        raise TemplateError("create input/NAME with message.txt and one or two JPEGs")
    return directory


def list_templates(root):
    directory = _input_directory(root)
    templates = []
    for entry in sorted(directory.iterdir(), key=lambda path: path.name):
        entry = _safe_child(directory, entry.name)
        if entry.is_dir():
            files = [_safe_child(entry, file.name) for file in entry.iterdir()]
            count = sum(
                file.is_file() and file.suffix.lower() in {".jpg", ".jpeg"}
                for file in files
            )
            templates.append((entry.name, count))
    if not templates:
        raise TemplateError("create input/NAME with message.txt and one or two JPEGs")
    return tuple(templates)


def validate_images(directory):
    directory = Path(directory)
    files = [
        _safe_child(directory, file.name)
        for file in sorted(directory.iterdir(), key=lambda path: path.name)
    ]
    files = [file for file in files if file.suffix.lower() in {".jpg", ".jpeg"}]
    if not 1 <= len(files) <= 2:
        raise TemplateError("template requires one or two JPEG images")
    infos = []
    for path in files:
        if not path.is_file():
            raise TemplateError("template JPEG must be a regular file")
        data = path.read_bytes()
        try:
            w, h = _jpeg_size(data)
        except ValueError:
            raise TemplateError("template image must be a readable JPEG") from None
        if len(data) > 300 * 1024 or not 0 < w <= 1500 or not 0 < h <= 1440:
            raise TemplateError("JPEG limits are 300 KB and 1500 x 1440 positive pixels")
        infos.append(ImageInfo(path.name, path, data, w, h))
    return tuple(infos)


def load_template(root, template_name):
    directory = _safe_child(_input_directory(root), template_name)
    if not directory.is_dir():
        raise TemplateError("selected template folder does not exist")
    message = _safe_child(directory, "message.txt")
    if not message.is_file():
        raise TemplateError("selected template requires UTF-8 message.txt")
    raw = message.read_bytes()
    try:
        content = raw.decode("utf-8-sig")
        encoded = content.encode("euc-kr")
    except UnicodeError:
        raise TemplateError("message.txt must be UTF-8 text supported by EUC-KR") from None
    if not content.strip():
        raise TemplateError("message.txt must not be blank")
    if len(encoded) > 2000:
        raise TemplateError("MMS content must not exceed 2000 EUC-KR bytes")
    return LoadedTemplate(template_name, raw, content, validate_images(directory))
