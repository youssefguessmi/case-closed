"""Chain-of-custody checks run at query time.

For every curated event an investigator sees, we:
  1. fetch the original raw source text it was derived from,
  2. recompute SHA-256(raw text) and compare it with raw_event_hash,
  3. re-parse the raw text and confirm the curated employee_id and timestamp
     still match what the original source says.
If any curated row (or its raw source) was edited after ingestion, it fails.
"""
from dataclasses import dataclass, field

import config
from pipeline.parsers import RAW_PARSERS, file_sha256, sha256
from serving.db import readonly_conn


@dataclass
class CustodyReport:
    checked: int = 0
    verified: int = 0
    failures: list[dict] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.checked > 0 and not self.failures


_VERIFY_SQL = """
SELECT t.raw_event_hash, t.employee_id, t.event_timestamp, t.raw_table,
       COALESCE(b.raw_line, d.raw_line, x.raw_line) AS raw_line
FROM curated.employee_activity_timeline t
LEFT JOIN `raw`.badge_access b
       ON t.raw_table = 'raw.badge_access' AND b.raw_id = t.raw_id
LEFT JOIN `raw`.device_logs d
       ON t.raw_table = 'raw.device_logs' AND d.raw_id = t.raw_id
LEFT JOIN `raw`.building_transactions x
       ON t.raw_table = 'raw.building_transactions' AND x.raw_id = t.raw_id
WHERE t.raw_event_hash IN ({placeholders})
"""


def verify_hashes(hashes: list[str]) -> CustodyReport:
    hashes = sorted({h for h in hashes if h})
    report = CustodyReport()
    if not hashes:
        return report
    conn = readonly_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(_VERIFY_SQL.format(placeholders=",".join(["%s"] * len(hashes))), hashes)
            found = {row[0]: row for row in cur.fetchall()}
    finally:
        conn.close()

    for h in hashes:
        report.checked += 1
        row = found.get(h)
        if row is None:
            report.failures.append({"hash": h, "reason": "no curated row carries this hash"})
            continue
        _, emp, ts, raw_table, raw_line = row
        if raw_line is None:
            report.failures.append({"hash": h, "reason": f"raw source row missing in {raw_table}"})
            continue
        if sha256(raw_line) != h:
            report.failures.append({"hash": h, "reason": "raw text no longer matches its hash"})
            continue
        parser, emp_field = RAW_PARSERS[raw_table]
        try:
            parsed = parser(raw_line)
        except ValueError as e:
            report.failures.append({"hash": h, "reason": f"raw text unparseable: {e}"})
            continue
        if parsed[emp_field] != emp or parsed["event_ts"] != ts:
            report.failures.append({"hash": h, "reason": "curated row differs from original source"})
            continue
        report.verified += 1
    return report


def verify_source_files() -> list[dict]:
    """Compare each source file on disk with the SHA-256 recorded at ingestion."""
    conn = readonly_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT m.source, m.file_name, m.file_sha256, m.ingested_at
                FROM `raw`.ingestion_manifest m
                JOIN (SELECT source, MAX(batch_id) AS b FROM `raw`.ingestion_manifest GROUP BY source) last
                  ON last.b = m.batch_id
                ORDER BY m.source""")
            rows = cur.fetchall()
    finally:
        conn.close()
    out = []
    for source, file_name, recorded, ingested_at in rows:
        path = config.DATA_FILES.get(source)
        current = file_sha256(path) if path and path.exists() else None
        out.append({
            "source": source, "file": file_name, "ingested_at": ingested_at,
            "recorded_sha256": recorded, "current_sha256": current,
            "status": "missing" if current is None else ("unchanged" if current == recorded else "CHANGED"),
        })
    return out
