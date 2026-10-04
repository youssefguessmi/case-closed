"""Hash-chained, append-only log of every investigator question and the SQL run for it.

entry_hash = SHA-256(prev_entry_hash + canonical JSON of the entry). Editing or
removing any past entry breaks every hash after it, which verify_chain() detects.
UPDATE/DELETE on the table are also blocked by triggers (see pipeline/load.py).
"""
import json
from datetime import datetime, timezone

from pipeline.parsers import sha256
from serving.db import audit_conn

GENESIS = "0" * 64
_FIELDS = ("asked_at", "question", "generated_sql", "status", "row_count",
           "result_sha256", "records_verified", "records_failed")


def result_hash(columns: list[str], rows: list[tuple]) -> str:
    return sha256(json.dumps({"columns": columns, "rows": rows}, default=str, sort_keys=True))


def _entry_hash(prev: str, entry: dict) -> str:
    payload = {k: entry[k] for k in _FIELDS}
    payload["asked_at"] = payload["asked_at"].isoformat(timespec="microseconds")
    return sha256(prev + json.dumps(payload, default=str, sort_keys=True))


def log_query(question, generated_sql, status, row_count=None, result_sha256=None,
              records_verified=None, records_failed=None) -> int:
    entry = {
        "asked_at": datetime.now(timezone.utc).replace(tzinfo=None),
        "question": question, "generated_sql": generated_sql, "status": status,
        "row_count": row_count, "result_sha256": result_sha256,
        "records_verified": records_verified, "records_failed": records_failed,
    }
    conn = audit_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT GET_LOCK('audit_chain', 5)")
            try:
                cur.execute("SELECT entry_hash FROM query_log ORDER BY log_id DESC LIMIT 1")
                row = cur.fetchone()
                prev = row[0] if row else GENESIS
                cur.execute(
                    f"INSERT INTO query_log ({', '.join(_FIELDS)}, prev_entry_hash, entry_hash) "
                    f"VALUES ({', '.join(['%s'] * (len(_FIELDS) + 2))})",
                    (*[entry[k] for k in _FIELDS], prev, _entry_hash(prev, entry)),
                )
                return cur.lastrowid
            finally:
                cur.execute("SELECT RELEASE_LOCK('audit_chain')")
    finally:
        conn.close()


def verify_chain() -> tuple[bool, int, str]:
    """Returns (intact, entries_checked, message)."""
    conn = audit_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT log_id, {', '.join(_FIELDS)}, prev_entry_hash, entry_hash "
                        f"FROM query_log ORDER BY log_id")
            rows = cur.fetchall()
    finally:
        conn.close()
    prev = GENESIS
    for row in rows:
        log_id, *values, stored_prev, stored_hash = row
        entry = dict(zip(_FIELDS, values))
        if stored_prev != prev or _entry_hash(prev, entry) != stored_hash:
            return False, len(rows), f"Chain broken at entry #{log_id}"
        prev = stored_hash
    return True, len(rows), f"{len(rows)} entries, chain intact"
