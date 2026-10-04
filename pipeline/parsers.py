"""Parse the three raw sources.

Every parsed record keeps `raw_line` (the exact text from the source) and
`raw_hash` = SHA-256(raw_line). That hash travels unchanged into the curated
timeline, so any curated row can be traced back to, and re-verified against,
the exact source text it came from.

The per-line parsers are also used by the serving layer to re-derive curated
facts from raw text at query time (chain-of-custody check).
"""
import hashlib
import re
from datetime import datetime, timezone
from pathlib import Path

BADGE_HEADER = ["badge_id", "employee_id", "door_id", "timestamp", "direction"]


def sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_ts(value: str) -> datetime:
    """Normalize '2026-08-14 22:31:12Z' / '2026-08-14T22:31:12Z' to naive UTC."""
    dt = datetime.fromisoformat(value.strip().replace("Z", "+00:00").replace(" ", "T"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


# ---------- Badge access (CSV) ----------
def parse_badge_line(line: str, header: list[str] = BADGE_HEADER) -> dict:
    row = dict(zip(header, (v.strip() for v in line.split(","))))
    return {
        "badge_id": row["badge_id"],
        "employee_id": row["employee_id"],
        "door_id": row["door_id"],
        "event_ts": parse_ts(row["timestamp"]),
        "direction": row["direction"].upper(),
        "raw_line": line,
        "raw_hash": sha256(line),
    }


def parse_badge(path: Path) -> list[dict]:
    lines = path.read_text(encoding="utf-8").splitlines()
    header = [h.strip() for h in lines[0].split(",")]
    return [parse_badge_line(line, header) for line in lines[1:] if line.strip()]


# ---------- Device logs (syslog-style key=value) ----------
_LOG_RE = re.compile(r"^(?P<ts>\S+)\s+(?P<host>\S+)\s+(?P<kv>.*)$")
_KV_RE = re.compile(r"(\w+)=(\S+)")


def parse_device_line(line: str) -> dict:
    m = _LOG_RE.match(line)
    if not m:
        raise ValueError(f"Unparseable device log line: {line!r}")
    kv = dict(_KV_RE.findall(m["kv"]))
    return {
        "hostname": m["host"],
        "user_id": kv.get("user"),
        "event_code": kv.get("event"),
        "event_ts": parse_ts(m["ts"]),
        "source_ip": kv.get("src"),
        "action": kv.get("action"),
        "file_path": kv.get("path"),
        "email_to": kv.get("to"),
        "raw_line": line,
        "raw_hash": sha256(line),
    }


def parse_device(path: Path) -> list[dict]:
    return [parse_device_line(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


# ---------- Building transactions (PostgreSQL INSERT dump) ----------
_TUPLE_RE = re.compile(
    r"\(\s*'(?P<txn>[^']+)'\s*,\s*'(?P<emp>[^']+)'\s*,\s*(?P<amt>[\d.]+)\s*,"
    r"\s*'(?P<ts>[^']+)'\s*,\s*'(?P<term>[^']+)'\s*\)"
)


def _txn_from_match(m: re.Match) -> dict:
    raw = m.group(0)
    return {
        "txn_id": m["txn"],
        "employee_id": m["emp"],
        "amount": m["amt"],
        "event_ts": parse_ts(m["ts"]),
        "terminal_id": m["term"],
        "raw_line": raw,
        "raw_hash": sha256(raw),
    }


def parse_transaction_tuple(text: str) -> dict:
    m = _TUPLE_RE.fullmatch(text.strip())
    if not m:
        raise ValueError(f"Unparseable transaction tuple: {text!r}")
    return _txn_from_match(m)


def parse_transactions(path: Path) -> list[dict]:
    """The source is a PostgreSQL dump; we extract the INSERT value tuples."""
    return [_txn_from_match(m) for m in _TUPLE_RE.finditer(path.read_text(encoding="utf-8"))]


# raw table -> (line parser, field holding the employee id)
RAW_PARSERS = {
    "raw.badge_access": (parse_badge_line, "employee_id"),
    "raw.device_logs": (parse_device_line, "user_id"),
    "raw.building_transactions": (parse_transaction_tuple, "employee_id"),
}
