from dataclasses import dataclass
from pathlib import Path
import os, re

APPROVED_KEYS = (
    "NCP_ACCESS_KEY_ID",
    "NCP_SECRET_KEY",
    "NCP_SENS_SERVICE_ID",
    "NCP_SENS_FROM_NUMBER",
    "SENS_CONTENT_TYPE",
)


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Config:
    access_key: str
    secret_key: str
    service_id: str
    from_number: str
    content_type: str


def _parse(path):
    out = {}
    if not path.exists():
        return out
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$", line)
        if not m:
            raise ConfigError(f".env syntax error at line {n}")
        v = m.group(2).strip()
        out[m.group(1)] = (
            v[1:-1] if len(v) >= 2 and v[0] == v[-1] and v[0] in "'\"" else v
        )
    return out


def load_config(root: Path, environ=None, env_path=None):
    e = dict(os.environ if environ is None else environ)
    f = _parse(env_path or root / ".env")
    vals = {k: e.get(k) or f.get(k) for k in APPROVED_KEYS}
    missing = [k for k, v in vals.items() if not v]
    if missing:
        raise ConfigError("Missing required configuration: " + ", ".join(missing))
    if vals["SENS_CONTENT_TYPE"] != "COMM":
        raise ConfigError("SENS_CONTENT_TYPE must be COMM")
    return Config(
        vals["NCP_ACCESS_KEY_ID"],
        vals["NCP_SECRET_KEY"],
        vals["NCP_SENS_SERVICE_ID"],
        vals["NCP_SENS_FROM_NUMBER"],
        vals["SENS_CONTENT_TYPE"],
    )
