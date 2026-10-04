"""Load the three sources into MySQL: raw (immutable) -> curated (unified timeline).

This is a minimal stand-in for the ingestion / cleaning / storage layers so the
serving layer (the chatbot) has real data to query. Run:

    python -m pipeline.load            # idempotent: re-running skips duplicates
    python -m pipeline.load --reset    # drop and rebuild everything
"""
import argparse
import sys
from datetime import datetime, timezone

import pymysql

import config
from pipeline.parsers import file_sha256, parse_badge, parse_device, parse_transactions

SCHEMA_DDL = [
    "CREATE DATABASE IF NOT EXISTS `raw`",
    "CREATE DATABASE IF NOT EXISTS curated",
    "CREATE DATABASE IF NOT EXISTS audit",
    # ---------- RAW: exact source rows, write-once ----------
    """CREATE TABLE IF NOT EXISTS `raw`.ingestion_manifest (
        batch_id      INT AUTO_INCREMENT PRIMARY KEY,
        source        VARCHAR(20)  NOT NULL,
        file_name     VARCHAR(255) NOT NULL,
        file_sha256   CHAR(64)     NOT NULL,
        rows_parsed   INT          NOT NULL,
        rows_inserted INT          NOT NULL,
        ingested_at   DATETIME(6)  NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS `raw`.badge_access (
        raw_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
        batch_id    INT          NOT NULL,
        badge_id    VARCHAR(12)  NOT NULL,
        employee_id VARCHAR(10)  NOT NULL,
        door_id     VARCHAR(30)  NOT NULL,
        event_ts    DATETIME     NOT NULL COMMENT 'UTC',
        direction   VARCHAR(3)   NOT NULL,
        raw_line    TEXT         NOT NULL,
        raw_hash    CHAR(64)     NOT NULL UNIQUE,
        ingested_at DATETIME(6)  NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS `raw`.device_logs (
        raw_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
        batch_id    INT          NOT NULL,
        hostname    VARCHAR(40)  NOT NULL,
        user_id     VARCHAR(10),
        event_code  VARCHAR(30),
        event_ts    DATETIME     NOT NULL COMMENT 'UTC',
        source_ip   VARCHAR(45),
        action      VARCHAR(20),
        file_path   VARCHAR(255),
        email_to    VARCHAR(40),
        raw_line    TEXT         NOT NULL,
        raw_hash    CHAR(64)     NOT NULL UNIQUE,
        ingested_at DATETIME(6)  NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS `raw`.building_transactions (
        raw_id      BIGINT AUTO_INCREMENT PRIMARY KEY,
        batch_id    INT           NOT NULL,
        txn_id      VARCHAR(12)   NOT NULL,
        employee_id VARCHAR(10)   NOT NULL,
        amount      DECIMAL(8,2)  NOT NULL,
        event_ts    DATETIME      NOT NULL COMMENT 'UTC',
        terminal_id VARCHAR(20)   NOT NULL,
        raw_line    TEXT          NOT NULL,
        raw_hash    CHAR(64)      NOT NULL UNIQUE,
        ingested_at DATETIME(6)   NOT NULL
    )""",
    # ---------- CURATED: unified, query-friendly ----------
    """CREATE TABLE IF NOT EXISTS curated.locations (
        location      VARCHAR(40) PRIMARY KEY,
        location_type VARCHAR(20) NOT NULL,
        floor         VARCHAR(4)  NULL,
        description   VARCHAR(255)
    )""",
    """CREATE TABLE IF NOT EXISTS curated.employee_activity_timeline (
        event_id         BIGINT AUTO_INCREMENT PRIMARY KEY,
        event_source     VARCHAR(12)  NOT NULL COMMENT 'badge | device | transaction',
        employee_id      VARCHAR(10)  NOT NULL,
        event_timestamp  DATETIME     NOT NULL COMMENT 'UTC',
        event_type       VARCHAR(32)  NOT NULL,
        location         VARCHAR(40)  NOT NULL,
        floor            VARCHAR(4)   NULL,
        action           VARCHAR(20)  NULL,
        details          VARCHAR(512) NULL,
        amount           DECIMAL(8,2) NULL,
        raw_table        VARCHAR(40)  NOT NULL,
        raw_id           BIGINT       NOT NULL,
        raw_event_hash   CHAR(64)     NOT NULL UNIQUE,
        ingested_at      DATETIME(6)  NOT NULL,
        INDEX ix_emp_ts (employee_id, event_timestamp),
        INDEX ix_ts (event_timestamp)
    )""",
    """CREATE TABLE IF NOT EXISTS curated.alibi_statements (
        employee_id VARCHAR(10) PRIMARY KEY,
        statement   TEXT NOT NULL
    )""",
    # ---------- AUDIT: hash-chained log of every investigator query ----------
    """CREATE TABLE IF NOT EXISTS audit.query_log (
        log_id           BIGINT AUTO_INCREMENT PRIMARY KEY,
        asked_at         DATETIME(6) NOT NULL,
        question         TEXT        NOT NULL,
        generated_sql    TEXT        NULL,
        status           VARCHAR(16) NOT NULL,
        row_count        INT         NULL,
        result_sha256    CHAR(64)    NULL,
        records_verified INT         NULL,
        records_failed   INT         NULL,
        prev_entry_hash  CHAR(64)    NOT NULL,
        entry_hash       CHAR(64)    NOT NULL
    )""",
]

WRITE_ONCE_TABLES = [
    ("raw", "ingestion_manifest"),
    ("raw", "badge_access"),
    ("raw", "device_logs"),
    ("raw", "building_transactions"),
    ("audit", "query_log"),
]

ALIBIS = {
    "EMP-0047": "I was on the 3rd floor working late on the audit files. Left the building around midnight.",
    "EMP-0031": "I was in the server room fixing a batch job. Left before 11 PM.",
    "EMP-0092": "I came back to grab something from my desk after dinner. I was in and out.",
}

# Curated rows are DERIVED from raw with plain SQL, so the lineage is auditable.
# raw_hash is copied verbatim into raw_event_hash.
CURATED_BUILD = [
    """INSERT IGNORE INTO curated.employee_activity_timeline
        (event_source, employee_id, event_timestamp, event_type, location, floor,
         action, details, amount, raw_table, raw_id, raw_event_hash, ingested_at)
       SELECT 'badge', b.employee_id, b.event_ts,
              CONCAT('BADGE_', b.direction), b.door_id, l.floor, b.direction,
              CONCAT('badge ', b.badge_id, ' ', b.direction, ' at ', b.door_id),
              NULL, 'raw.badge_access', b.raw_id, b.raw_hash, b.ingested_at
       FROM `raw`.badge_access b
       LEFT JOIN curated.locations l ON l.location = b.door_id""",
    """INSERT IGNORE INTO curated.employee_activity_timeline
        (event_source, employee_id, event_timestamp, event_type, location, floor,
         action, details, amount, raw_table, raw_id, raw_event_hash, ingested_at)
       SELECT 'device', d.user_id, d.event_ts, d.event_code, d.hostname, l.floor, d.action,
              CONCAT_WS(' ', CONCAT('src=', d.source_ip), CONCAT('path=', d.file_path),
                        CONCAT('to=', d.email_to)),
              NULL, 'raw.device_logs', d.raw_id, d.raw_hash, d.ingested_at
       FROM `raw`.device_logs d
       LEFT JOIN curated.locations l ON l.location = d.hostname
       WHERE d.user_id IS NOT NULL""",
    """INSERT IGNORE INTO curated.employee_activity_timeline
        (event_source, employee_id, event_timestamp, event_type, location, floor,
         action, details, amount, raw_table, raw_id, raw_event_hash, ingested_at)
       SELECT 'transaction', t.employee_id, t.event_ts,
              CASE WHEN t.terminal_id LIKE 'PARKING-EXIT%'  THEN 'PARKING_EXIT'
                   WHEN t.terminal_id LIKE 'PARKING-ENTRY%' THEN 'PARKING_ENTRY'
                   ELSE 'PURCHASE' END,
              t.terminal_id, l.floor, NULL,
              CONCAT(t.txn_id, ' $', t.amount, ' at ', t.terminal_id),
              t.amount, 'raw.building_transactions', t.raw_id, t.raw_hash, t.ingested_at
       FROM `raw`.building_transactions t
       LEFT JOIN curated.locations l ON l.location = t.terminal_id""",
]


# source -> (raw table, parsed fields stored as columns)
RAW_SOURCES = {
    "badge": ("badge_access", ["badge_id", "employee_id", "door_id", "event_ts", "direction"]),
    "device": ("device_logs", ["hostname", "user_id", "event_code", "event_ts", "source_ip",
                               "action", "file_path", "email_to"]),
    "transaction": ("building_transactions", ["txn_id", "employee_id", "amount", "event_ts", "terminal_id"]),
}


def parse_all() -> dict[str, list[dict]]:
    f = config.DATA_FILES
    return {"badge": parse_badge(f["badge"]), "device": parse_device(f["device"]),
            "transaction": parse_transactions(f["transaction"])}


def all_locations(parsed: dict[str, list[dict]]) -> list[str]:
    locs = ({r["door_id"] for r in parsed["badge"]} | {r["hostname"] for r in parsed["device"]}
            | {r["terminal_id"] for r in parsed["transaction"]})
    return sorted(locs)


def classify_location(loc: str) -> tuple[str, str | None, str]:
    """(location_type, floor, description). Floors are only set where the name states it."""
    if loc == "D-LOBBY":
        return "door", "1", "Main lobby door - building entry/exit"
    if loc == "D-EXEC-3F":
        return "door", "3", "Executive suite door, 3rd floor"
    if loc == "D-SERVER":
        return "door", None, "Server room door (floor not recorded)"
    if loc.startswith("D-STAIRWELL"):
        return "door", None, f"Stairwell door {loc.split('-')[-1]}"
    if loc.startswith("PARKING-"):
        return "parking", "P", "Parking gate " + ("exit" if "EXIT" in loc else "entry")
    if loc.startswith("VENDING-"):
        suffix = loc.split("-", 1)[1]
        floor = "1" if suffix == "LOBBY" else suffix.rstrip("F") if suffix.endswith("F") else None
        return "vending", floor, f"Vending machine ({suffix.lower()})"
    if loc.startswith("CAFETERIA-"):
        return "cafeteria", None, "Cafeteria till (floor not recorded)"
    if loc.startswith(("LAPTOP-", "WORKSTATION-")):
        return "device", None, "Employee computer - shows activity, not the physical room"
    if loc.startswith("D-"):
        return "door", None, "Door"
    return "other", None, ""


def admin_conn(db=None):
    return pymysql.connect(
        host=config.MYSQL_HOST, port=config.MYSQL_PORT, user=config.MYSQL_ADMIN_USER,
        password=config.MYSQL_ADMIN_PASSWORD, database=db, autocommit=True,
    )


def create_schema(cur, reset: bool):
    if reset:
        for db in ("curated", "`raw`", "audit"):
            cur.execute(f"DROP DATABASE IF EXISTS {db}")
    for stmt in SCHEMA_DDL + trigger_statements():
        cur.execute(stmt)


def trigger_statements() -> list[str]:
    """Write-once guard: block UPDATE/DELETE at the database level."""
    stmts = []
    for db, table in WRITE_ONCE_TABLES:
        for op in ("UPDATE", "DELETE"):
            trg = f"trg_{table}_no_{op.lower()}"
            stmts.append(f"DROP TRIGGER IF EXISTS `{db}`.{trg}")
            stmts.append(
                f"CREATE TRIGGER `{db}`.{trg} BEFORE {op} ON `{db}`.{table} FOR EACH ROW "
                f"SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = '{db}.{table} is write-once ({op} blocked)'"
            )
    return stmts


def ingest(cur, source, path, records, table, columns):
    """Append-only ingest. Records already present (same raw_hash) are skipped (dedup)."""
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    cur.execute(f"SELECT raw_hash FROM `raw`.{table}")
    existing = {h for (h,) in cur.fetchall()}
    seen, new = set(), []
    for r in records:
        if r["raw_hash"] not in existing and r["raw_hash"] not in seen:
            seen.add(r["raw_hash"])
            new.append(r)

    # Manifest is write-once, so it is written once with the final counts.
    cur.execute(
        "INSERT INTO `raw`.ingestion_manifest (source, file_name, file_sha256, rows_parsed, rows_inserted, ingested_at)"
        " VALUES (%s,%s,%s,%s,%s,%s)",
        (source, path.name, file_sha256(path), len(records), len(new), now),
    )
    batch_id = cur.lastrowid
    cols = ["batch_id", *columns, "raw_line", "raw_hash", "ingested_at"]
    sql = (f"INSERT INTO `raw`.{table} ({', '.join(cols)}) "
           f"VALUES ({', '.join(['%s'] * len(cols))})")
    cur.executemany(sql, [(batch_id, *[r[c] for c in columns], r["raw_line"], r["raw_hash"], now) for r in new])
    print(f"  {source:<12} {path.name:<28} parsed={len(records):<4} new={len(new)}")


def create_users(cur):
    users = [
        (config.MYSQL_RO_USER, config.MYSQL_RO_PASSWORD,
         ["GRANT SELECT ON curated.* TO {u}", "GRANT SELECT ON `raw`.* TO {u}"]),
        (config.MYSQL_AUDIT_USER, config.MYSQL_AUDIT_PASSWORD,
         ["GRANT SELECT, INSERT ON audit.query_log TO {u}"]),
    ]
    for name, pwd, grants in users:
        for host in ("localhost", "127.0.0.1"):
            u = f"'{name}'@'{host}'"
            cur.execute(f"CREATE USER IF NOT EXISTS {u} IDENTIFIED BY %s", (pwd,))
            cur.execute(f"ALTER USER {u} IDENTIFIED BY %s", (pwd,))
            for g in grants:
                cur.execute(g.format(u=u))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="drop and rebuild all schemas")
    args = ap.parse_args()

    files = config.DATA_FILES
    parsed = parse_all()

    try:
        conn = admin_conn()
    except pymysql.err.OperationalError as e:
        sys.exit(f"Cannot connect to MySQL as {config.MYSQL_ADMIN_USER}: {e}\n"
                 f"Set MYSQL_ADMIN_USER / MYSQL_ADMIN_PASSWORD in .env")

    with conn.cursor() as cur:
        print("Creating schemas...")
        create_schema(cur, args.reset)

        print("Ingesting raw sources...")
        for source, records in parsed.items():
            table, columns = RAW_SOURCES[source]
            ingest(cur, source, files[source], records, table, columns)

        print("Building curated layer...")
        for loc in all_locations(parsed):
            cur.execute("REPLACE INTO curated.locations VALUES (%s,%s,%s,%s)", (loc, *classify_location(loc)))
        for emp, stmt in ALIBIS.items():
            cur.execute("REPLACE INTO curated.alibi_statements VALUES (%s,%s)", (emp, stmt))
        for stmt in CURATED_BUILD:
            cur.execute(stmt)
        cur.execute("SELECT event_source, COUNT(*) FROM curated.employee_activity_timeline GROUP BY event_source")
        for src, n in cur.fetchall():
            print(f"  curated {src:<12} {n} events")

        print("Creating least-privilege users...")
        create_users(cur)
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()
