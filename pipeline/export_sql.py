"""Generate a standalone MySQL script from the three data files.

No database connection is needed to generate it, and no Python is needed to run it:

    python -m pipeline.export_sql                 # writes sql/blackwood.sql
    python -m pipeline.export_sql --users         # also writes sql/users.sql (passwords from .env)

    mysql -u root -p < sql/blackwood.sql          # build raw + curated + audit schemas
    mysql -u root -p < sql/users.sql              # create the chatbot's least-privilege users

The script uses the same DDL / curated-build SQL as pipeline/load.py, so both
paths produce identical databases (including identical raw_event_hash values).
"""
import argparse
from datetime import datetime, timezone
from decimal import Decimal

from pymysql.converters import escape_string

import config
from pipeline.load import (ALIBIS, CURATED_BUILD, RAW_SOURCES, SCHEMA_DDL, all_locations,
                           classify_location, parse_all, trigger_statements)
from pipeline.parsers import file_sha256

OUT_DIR = config.ROOT / "sql"


def lit(v) -> str:
    """Render a Python value as a MySQL literal."""
    if v is None:
        return "NULL"
    if isinstance(v, (int, float, Decimal)):
        return str(v)
    if isinstance(v, datetime):
        return f"'{v.isoformat(sep=' ', timespec='microseconds' if v.microsecond else 'seconds')}'"
    return f"'{escape_string(str(v))}'"


def insert(table: str, columns: list[str], rows: list[tuple], chunk: int = 100) -> list[str]:
    out = []
    for i in range(0, len(rows), chunk):
        values = ",\n  ".join("(" + ", ".join(lit(v) for v in row) + ")" for row in rows[i:i + chunk])
        out.append(f"INSERT INTO {table} ({', '.join(columns)}) VALUES\n  {values}")
    return out


def build_script() -> str:
    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    parsed = parse_all()
    stmts = [
        "SET NAMES utf8mb4",
        "SET time_zone = '+00:00'",
        "DROP DATABASE IF EXISTS curated",
        "DROP DATABASE IF EXISTS `raw`",
        "DROP DATABASE IF EXISTS audit",
        *SCHEMA_DDL,
    ]

    # Raw layer: one manifest batch per source file, then its records.
    for batch_id, (source, records) in enumerate(parsed.items(), start=1):
        path = config.DATA_FILES[source]
        table, columns = RAW_SOURCES[source]
        stmts += insert("`raw`.ingestion_manifest",
                        ["batch_id", "source", "file_name", "file_sha256", "rows_parsed", "rows_inserted", "ingested_at"],
                        [(batch_id, source, path.name, file_sha256(path), len(records), len(records), now)])
        seen, rows = set(), []
        for r in records:  # dedup on raw hash, same as the live loader
            if r["raw_hash"] not in seen:
                seen.add(r["raw_hash"])
                rows.append((batch_id, *[r[c] for c in columns], r["raw_line"], r["raw_hash"], now))
        stmts += insert(f"`raw`.{table}", ["batch_id", *columns, "raw_line", "raw_hash", "ingested_at"], rows)

    # Curated layer: reference tables, then the unified timeline derived from raw.
    stmts += insert("curated.locations", ["location", "location_type", "floor", "description"],
                    [(loc, *classify_location(loc)) for loc in all_locations(parsed)])
    stmts += insert("curated.alibi_statements", ["employee_id", "statement"], list(ALIBIS.items()))
    stmts += CURATED_BUILD

    # Triggers last, so the inserts above are not affected.
    stmts += trigger_statements()

    header = (
        "-- Blackwood case: raw + curated + audit schemas for MySQL 8+\n"
        f"-- Generated {now:%Y-%m-%d %H:%M:%S} UTC by `python -m pipeline.export_sql`. Do not edit by hand.\n"
        "-- Sources: " + ", ".join(f"{p.name} (sha256 {file_sha256(p)[:12]}...)" for p in config.DATA_FILES.values()) +
        "\n-- Run:  mysql -u root -p < sql/blackwood.sql\n"
        "-- WARNING: drops and recreates the `raw`, curated and audit databases.\n\n"
    )
    body = ";\n\n".join(s.strip() for s in stmts) + ";\n"
    footer = ("\n-- Sanity check\nSELECT event_source, COUNT(*) AS events "
              "FROM curated.employee_activity_timeline GROUP BY event_source;\n")
    return header + body + footer


def build_users_script() -> str:
    lines = ["-- Least-privilege users for the chatbot. Contains passwords from .env: do not commit.\n"]
    users = [
        (config.MYSQL_RO_USER, config.MYSQL_RO_PASSWORD,
         ["GRANT SELECT ON curated.* TO {u}", "GRANT SELECT ON `raw`.* TO {u}"]),
        (config.MYSQL_AUDIT_USER, config.MYSQL_AUDIT_PASSWORD,
         ["GRANT SELECT, INSERT ON audit.query_log TO {u}"]),
    ]
    for name, pwd, grants in users:
        for host in ("localhost", "127.0.0.1"):
            u = f"'{name}'@'{host}'"
            lines.append(f"CREATE USER IF NOT EXISTS {u} IDENTIFIED BY {lit(pwd)};")
            lines.append(f"ALTER USER {u} IDENTIFIED BY {lit(pwd)};")
            lines += [g.format(u=u) + ";" for g in grants]
    return "\n".join(lines) + "\n"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--users", action="store_true", help="also write sql/users.sql using passwords from .env")
    args = ap.parse_args()
    OUT_DIR.mkdir(exist_ok=True)
    out = OUT_DIR / "blackwood.sql"
    out.write_text(build_script(), encoding="utf-8")
    print(f"Wrote {out.relative_to(config.ROOT)}")
    if args.users:
        users = OUT_DIR / "users.sql"
        users.write_text(build_users_script(), encoding="utf-8")
        print(f"Wrote {users.relative_to(config.ROOT)} (contains passwords - gitignored)")


if __name__ == "__main__":
    main()
