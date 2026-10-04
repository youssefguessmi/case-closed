"""
Case Closed - Data Engineering Pipeline

What this script does:
1. Hashes the original badge CSV and device log with SHA-256.
2. Makes an evidence copy of each original file and verifies the copy hash.
3. Reads only from the evidence copies.
4. Transforms badge, device, and PostgreSQL transaction data into one common schema.
5. Uploads the normalized events into MySQL.
6. Periodically polls PostgreSQL for NEW transaction rows using a saved watermark.
7. Provides simple timeline and badge-vs-device comparison commands.

Required packages:
    pip install psycopg2-binary mysql-connector-python

Database settings are read from environment variables. See the configuration
section below for variable names and defaults.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# -----------------------------------------------------------------------------
# FILE CONFIGURATION
# -----------------------------------------------------------------------------

BADGE_FILE = Path(os.getenv("BADGE_FILE", "badge_access.csv"))
DEVICE_FILE = Path(os.getenv("DEVICE_FILE", "device_logs.log"))
ARCHIVE_ROOT = Path(os.getenv("ARCHIVE_ROOT", "raw_evidence"))
LOCAL_MANIFEST_FILE = Path(os.getenv("LOCAL_MANIFEST_FILE", "chain_of_custody_manifest.csv"))


# -----------------------------------------------------------------------------
# POSTGRESQL CONFIGURATION
# -----------------------------------------------------------------------------
# Change these with environment variables or edit the defaults for your setup.
# The exact PostgreSQL table name was not provided in the case, so the default
# is "building_transactions". Change PG_TABLE if your database uses another name.

PG_CONFIG = {
    "host": os.getenv("PG_HOST", "localhost"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DATABASE", "ccdb"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "991852413"),
}

PG_TABLE = os.getenv("PG_TABLE", "building_transactions")
PG_POLL_SECONDS = int(os.getenv("PG_POLL_SECONDS", "30"))
PG_BATCH_SIZE = int(os.getenv("PG_BATCH_SIZE", "500"))


# -----------------------------------------------------------------------------
# MYSQL CONFIGURATION
# -----------------------------------------------------------------------------

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "port": int(os.getenv("MYSQL_PORT", "3306")),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "991852413"),
}

MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "sys")


# -----------------------------------------------------------------------------
# HASHING / CHAIN OF CUSTODY
# -----------------------------------------------------------------------------

def sha256_file(path: Path) -> str:
    """Return the SHA-256 hash of a file without changing it."""
    digest = hashlib.sha256()

    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def canonical_hash(data: Dict[str, Any]) -> str:
    """Hash a record after converting it to stable JSON."""
    text = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return sha256_text(text)


def archive_source_file(source: Path) -> Dict[str, Any]:
    """
    Hash the original source, copy it into a timestamped evidence folder,
    hash the copy, and verify both hashes are identical.

    The rest of the pipeline reads from the archived copy, not the original.
    """
    if not source.exists():
        raise FileNotFoundError(f"Source file not found: {source}")

    original_hash = sha256_file(source)

    run_folder = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    archive_folder = ARCHIVE_ROOT / run_folder
    archive_folder.mkdir(parents=True, exist_ok=True)

    archived_path = archive_folder / source.name
    shutil.copy2(source, archived_path)

    copied_hash = sha256_file(archived_path)
    verified = original_hash == copied_hash

    if not verified:
        raise RuntimeError(
            f"Hash verification failed for {source}. "
            "The evidence copy does not match the original."
        )

    manifest = {
        "original_path": str(source.resolve()),
        "archived_path": str(archived_path.resolve()),
        "sha256_original": original_hash,
        "sha256_archived_copy": copied_hash,
        "verified": True,
        "archived_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    append_local_manifest(manifest)
    return manifest


def append_local_manifest(manifest: Dict[str, Any]) -> None:
    """Keep a local audit record of every evidence copy we create."""
    columns = [
        "original_path",
        "archived_path",
        "sha256_original",
        "sha256_archived_copy",
        "verified",
        "archived_at_utc",
    ]

    file_exists = LOCAL_MANIFEST_FILE.exists()

    with LOCAL_MANIFEST_FILE.open("a", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        if not file_exists:
            writer.writeheader()
        writer.writerow({column: manifest[column] for column in columns})


# -----------------------------------------------------------------------------
# TIME / NORMALIZATION HELPERS
# -----------------------------------------------------------------------------

def parse_utc_timestamp(value: Any) -> datetime:
    """Convert a string or datetime to a timezone-aware UTC datetime."""
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)

    # Case samples use UTC (Z). If PostgreSQL returns a naive datetime,
    # this script treats it as UTC.
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def mysql_utc_datetime(value: datetime) -> datetime:
    """MySQL DATETIME stores no timezone, so insert normalized UTC without tzinfo."""
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def make_source_event_key(source: str, source_record_id: str, raw_event_hash: str) -> str:
    return sha256_text(f"{source}|{source_record_id}|{raw_event_hash}")


# -----------------------------------------------------------------------------
# BADGE TRANSFORMATION
# -----------------------------------------------------------------------------

def transform_badge_file(archived_file: Path, source_file_hash: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    with archived_file.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)

        required = {"badge_id", "employee_id", "door_id", "timestamp", "direction"}
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Badge CSV is missing columns: {sorted(missing)}")

        for row in reader:
            raw_record = {
                "badge_id": row["badge_id"],
                "employee_id": row["employee_id"],
                "door_id": row["door_id"],
                "timestamp": row["timestamp"],
                "direction": row["direction"],
            }

            raw_event_hash = canonical_hash(raw_record)
            timestamp = parse_utc_timestamp(row["timestamp"])
            direction = row["direction"].strip().upper()

            source_record_id = "|".join(
                [
                    row["badge_id"].strip(),
                    timestamp.isoformat(),
                    row["door_id"].strip(),
                    direction,
                ]
            )

            events.append(
                {
                    "source_event_key": make_source_event_key(
                        "BADGE", source_record_id, raw_event_hash
                    ),
                    "source_record_id": source_record_id,
                    "employee_id": row["employee_id"].strip().upper(),
                    "event_timestamp_utc": timestamp,
                    "event_source": "BADGE",
                    "event_type": f"BADGE_{direction}",
                    "location": row["door_id"].strip(),
                    "details": f"badge_id={row['badge_id'].strip()} direction={direction}",
                    "raw_event_hash": raw_event_hash,
                    "source_file_hash": source_file_hash,
                }
            )

    return events


# -----------------------------------------------------------------------------
# DEVICE LOG TRANSFORMATION
# -----------------------------------------------------------------------------

def transform_device_file(archived_file: Path, source_file_hash: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []

    with archived_file.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            raw_line = line.rstrip("\r\n")
            if not raw_line.strip():
                continue

            parts = raw_line.split()
            if len(parts) < 3:
                print(f"Skipping malformed device log line {line_number}: {raw_line}")
                continue

            timestamp_text = parts[0]
            hostname = parts[1]
            values: Dict[str, str] = {}

            for part in parts[2:]:
                if "=" in part:
                    key, value = part.split("=", 1)
                    values[key] = value

            employee_id = values.get("user")
            event_type = values.get("event")

            if not employee_id or not event_type:
                print(f"Skipping incomplete device log line {line_number}: {raw_line}")
                continue

            timestamp = parse_utc_timestamp(timestamp_text)
            raw_event_hash = sha256_text(raw_line)

            # A device log has no transaction ID, so its raw line hash is its stable ID.
            source_record_id = raw_event_hash

            extra_fields = []
            for key, value in values.items():
                if key not in {"user", "event"}:
                    extra_fields.append(f"{key}={value}")

            events.append(
                {
                    "source_event_key": make_source_event_key(
                        "DEVICE", source_record_id, raw_event_hash
                    ),
                    "source_record_id": source_record_id,
                    "employee_id": employee_id.strip().upper(),
                    "event_timestamp_utc": timestamp,
                    "event_source": "DEVICE",
                    "event_type": event_type.strip().upper(),
                    "location": hostname.strip(),
                    "details": " ".join(extra_fields),
                    "raw_event_hash": raw_event_hash,
                    "source_file_hash": source_file_hash,
                }
            )

    return events


# -----------------------------------------------------------------------------
# MYSQL SETUP / WRITES
# -----------------------------------------------------------------------------

def get_mysql_connector():
    try:
        import mysql.connector
        return mysql.connector
    except ImportError as exc:
        raise RuntimeError(
            "mysql-connector-python is not installed. Run: "
            "pip install mysql-connector-python"
        ) from exc


def create_mysql_database_if_needed() -> None:
    mysql = get_mysql_connector()

    connection = mysql.connect(**MYSQL_CONFIG)
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{MYSQL_DATABASE}`")
        connection.commit()
        cursor.close()
    finally:
        connection.close()


def mysql_connect():
    mysql = get_mysql_connector()

    config = dict(MYSQL_CONFIG)
    config["database"] = MYSQL_DATABASE

    connection = mysql.connect(**config)

    cursor = connection.cursor()
    cursor.execute("SET time_zone = '+00:00'")
    cursor.close()

    return connection


def setup_mysql_tables() -> None:
    create_mysql_database_if_needed()

    connection = mysql_connect()
    try:
        cursor = connection.cursor()

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS employee_activity_timeline (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                source_event_key CHAR(64) NOT NULL,
                source_record_id VARCHAR(255) NOT NULL,
                employee_id VARCHAR(64) NOT NULL,
                event_timestamp_utc DATETIME(6) NOT NULL,
                event_source VARCHAR(32) NOT NULL,
                event_type VARCHAR(64) NOT NULL,
                location VARCHAR(255),
                details TEXT,
                raw_event_hash CHAR(64) NOT NULL,
                source_file_hash CHAR(64),
                ingested_at_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
                UNIQUE KEY uq_source_event_key (source_event_key),
                INDEX idx_employee_time (employee_id, event_timestamp_utc),
                INDEX idx_source_time (event_source, event_timestamp_utc)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS chain_of_custody_files (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                original_path TEXT NOT NULL,
                archived_path TEXT NOT NULL,
                sha256_original CHAR(64) NOT NULL,
                sha256_archived_copy CHAR(64) NOT NULL,
                verified BOOLEAN NOT NULL,
                archived_at_utc DATETIME(6) NOT NULL,
                UNIQUE KEY uq_original_hash (sha256_original)
            )
            """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_state (
                source_name VARCHAR(64) PRIMARY KEY,
                last_timestamp_utc DATETIME(6),
                last_record_id VARCHAR(255),
                updated_at_utc DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
                   ON UPDATE CURRENT_TIMESTAMP(6)
            )
            """
        )

        connection.commit()
        cursor.close()
    finally:
        connection.close()


def record_file_manifest_in_mysql(manifest: Dict[str, Any]) -> None:
    connection = mysql_connect()
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT IGNORE INTO chain_of_custody_files (
                original_path,
                archived_path,
                sha256_original,
                sha256_archived_copy,
                verified,
                archived_at_utc
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                manifest["original_path"],
                manifest["archived_path"],
                manifest["sha256_original"],
                manifest["sha256_archived_copy"],
                bool(manifest["verified"]),
                mysql_utc_datetime(parse_utc_timestamp(manifest["archived_at_utc"])),
            ),
        )
        connection.commit()
        cursor.close()
    finally:
        connection.close()


def insert_events_into_mysql(events: Iterable[Dict[str, Any]]) -> Tuple[int, int]:
    events = list(events)
    if not events:
        return 0, 0

    connection = mysql_connect()
    try:
        cursor = connection.cursor()

        sql = """
            INSERT IGNORE INTO employee_activity_timeline (
                source_event_key,
                source_record_id,
                employee_id,
                event_timestamp_utc,
                event_source,
                event_type,
                location,
                details,
                raw_event_hash,
                source_file_hash
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted = 0
        skipped = 0

        for event in events:
            cursor.execute(
                sql,
                (
                    event["source_event_key"],
                    event["source_record_id"],
                    event["employee_id"],
                    mysql_utc_datetime(event["event_timestamp_utc"]),
                    event["event_source"],
                    event["event_type"],
                    event.get("location"),
                    event.get("details"),
                    event["raw_event_hash"],
                    event.get("source_file_hash"),
                ),
            )

            if cursor.rowcount == 1:
                inserted += 1
            else:
                skipped += 1

        connection.commit()
        cursor.close()
        return inserted, skipped
    finally:
        connection.close()


def get_pipeline_state(source_name: str) -> Tuple[datetime, str]:
    connection = mysql_connect()
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT last_timestamp_utc, last_record_id
            FROM pipeline_state
            WHERE source_name = %s
            """,
            (source_name,),
        )
        row = cursor.fetchone()
        cursor.close()

        if row is None or row[0] is None:
            return datetime(1970, 1, 1, tzinfo=timezone.utc), ""

        last_timestamp = row[0].replace(tzinfo=timezone.utc)
        last_record_id = row[1] or ""
        return last_timestamp, last_record_id
    finally:
        connection.close()


def set_pipeline_state(source_name: str, timestamp: datetime, record_id: str) -> None:
    connection = mysql_connect()
    try:
        cursor = connection.cursor()
        cursor.execute(
            """
            INSERT INTO pipeline_state (
                source_name,
                last_timestamp_utc,
                last_record_id
            ) VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                last_timestamp_utc = VALUES(last_timestamp_utc),
                last_record_id = VALUES(last_record_id)
            """,
            (source_name, mysql_utc_datetime(timestamp), record_id),
        )
        connection.commit()
        cursor.close()
    finally:
        connection.close()


# -----------------------------------------------------------------------------
# POSTGRESQL POLLING / TRANSFORMATION
# -----------------------------------------------------------------------------

def validate_pg_table_name(name: str) -> str:
    """Allow schema.table or table names, but reject unsafe SQL text."""
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?", name):
        raise ValueError(f"Unsafe PostgreSQL table name: {name}")
    return name


def get_psycopg2():
    try:
        import psycopg2
        return psycopg2
    except ImportError as exc:
        raise RuntimeError(
            "psycopg2-binary is not installed. Run: pip install psycopg2-binary"
        ) from exc


def fetch_new_postgres_transactions(
    last_timestamp: datetime,
    last_record_id: str,
    limit: int,
) -> List[Dict[str, Any]]:
    psycopg2 = get_psycopg2()
    table = validate_pg_table_name(PG_TABLE)

    connection = psycopg2.connect(**PG_CONFIG)
    try:
        cursor = connection.cursor()

        query = f"""
            SELECT txn_id, employee_id, amount, timestamp, terminal_id
            FROM {table}
            WHERE timestamp > %s
               OR (timestamp = %s AND txn_id > %s)
            ORDER BY timestamp ASC, txn_id ASC
            LIMIT %s
        """

        cursor.execute(
            query,
            (
                last_timestamp,
                last_timestamp,
                last_record_id,
                limit,
            ),
        )

        rows = cursor.fetchall()
        cursor.close()

        transactions: List[Dict[str, Any]] = []
        for txn_id, employee_id, amount, timestamp_value, terminal_id in rows:
            transactions.append(
                {
                    "txn_id": str(txn_id),
                    "employee_id": str(employee_id),
                    "amount": amount,
                    "timestamp": timestamp_value,
                    "terminal_id": str(terminal_id),
                }
            )

        return transactions
    finally:
        connection.close()


def transform_postgres_transaction(row: Dict[str, Any]) -> Dict[str, Any]:
    timestamp = parse_utc_timestamp(row["timestamp"])

    amount = row["amount"]
    if isinstance(amount, Decimal):
        amount_text = format(amount, "f")
    else:
        amount_text = str(amount)

    raw_record = {
        "txn_id": row["txn_id"],
        "employee_id": row["employee_id"],
        "amount": amount_text,
        "timestamp": timestamp.isoformat(),
        "terminal_id": row["terminal_id"],
    }

    raw_event_hash = canonical_hash(raw_record)
    source_record_id = str(row["txn_id"])

    return {
        "source_event_key": make_source_event_key(
            "TRANSACTION", source_record_id, raw_event_hash
        ),
        "source_record_id": source_record_id,
        "employee_id": str(row["employee_id"]).strip().upper(),
        "event_timestamp_utc": timestamp,
        "event_source": "TRANSACTION",
        "event_type": "TRANSACTION",
        "location": str(row["terminal_id"]),
        "details": f"txn_id={row['txn_id']} amount={amount_text}",
        "raw_event_hash": raw_event_hash,
        "source_file_hash": None,
    }


def poll_postgres_once() -> int:
    """
    Pull every PostgreSQL row that is newer than the saved MySQL watermark.
    Data is fetched in batches so a backlog does not require one giant query.
    """
    source_name = "POSTGRES_TRANSACTIONS"
    total_inserted = 0

    while True:
        last_timestamp, last_record_id = get_pipeline_state(source_name)

        rows = fetch_new_postgres_transactions(
            last_timestamp,
            last_record_id,
            PG_BATCH_SIZE,
        )

        if not rows:
            break

        events = [transform_postgres_transaction(row) for row in rows]
        inserted, skipped = insert_events_into_mysql(events)
        total_inserted += inserted

        # Move the watermark only after the MySQL insert succeeded.
        newest_row = rows[-1]
        newest_timestamp = parse_utc_timestamp(newest_row["timestamp"])
        newest_record_id = str(newest_row["txn_id"])
        set_pipeline_state(source_name, newest_timestamp, newest_record_id)

        print(
            f"PostgreSQL batch: {len(rows)} fetched, "
            f"{inserted} inserted, {skipped} already present."
        )

        if len(rows) < PG_BATCH_SIZE:
            break

    return total_inserted


# -----------------------------------------------------------------------------
# FILE INGESTION PIPELINE
# -----------------------------------------------------------------------------

def ingest_original_files() -> None:
    """
    Archive + hash the two file sources, transform the archived copies,
    then upload the unified records to MySQL.
    """
    badge_manifest = archive_source_file(BADGE_FILE)
    device_manifest = archive_source_file(DEVICE_FILE)

    record_file_manifest_in_mysql(badge_manifest)
    record_file_manifest_in_mysql(device_manifest)

    print("\nChain of custody:")
    print(f"Badge SHA-256 : {badge_manifest['sha256_original']}")
    print(f"Badge copy    : {badge_manifest['archived_path']}")
    print(f"Device SHA-256: {device_manifest['sha256_original']}")
    print(f"Device copy   : {device_manifest['archived_path']}")
    print("Both evidence copies verified against their originals.\n")

    badge_events = transform_badge_file(
        Path(badge_manifest["archived_path"]),
        badge_manifest["sha256_original"],
    )

    device_events = transform_device_file(
        Path(device_manifest["archived_path"]),
        device_manifest["sha256_original"],
    )

    badge_inserted, badge_skipped = insert_events_into_mysql(badge_events)
    device_inserted, device_skipped = insert_events_into_mysql(device_events)

    print(
        f"Badge events: {badge_inserted} inserted, "
        f"{badge_skipped} already present."
    )
    print(
        f"Device events: {device_inserted} inserted, "
        f"{device_skipped} already present."
    )


# -----------------------------------------------------------------------------
# INVESTIGATION / COMPARISON HELPERS
# -----------------------------------------------------------------------------

def fetch_employee_timeline(
    employee_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[Tuple[Any, ...]]:
    employee_id = employee_id.strip().upper()

    sql = """
        SELECT
            event_timestamp_utc,
            event_source,
            event_type,
            location,
            details,
            raw_event_hash
        FROM employee_activity_timeline
        WHERE employee_id = %s
    """
    params: List[Any] = [employee_id]

    if start:
        sql += " AND event_timestamp_utc >= %s"
        params.append(mysql_utc_datetime(parse_utc_timestamp(start)))

    if end:
        sql += " AND event_timestamp_utc <= %s"
        params.append(mysql_utc_datetime(parse_utc_timestamp(end)))

    sql += " ORDER BY event_timestamp_utc, event_source"

    connection = mysql_connect()
    try:
        cursor = connection.cursor()
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()
        cursor.close()
        return rows
    finally:
        connection.close()


def print_employee_timeline(
    employee_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> None:
    rows = fetch_employee_timeline(employee_id, start, end)

    print(f"\nTIMELINE FOR {employee_id.upper()}")
    print("=" * 125)
    print(
        f"{'TIME (UTC)':<27} {'SOURCE':<13} {'EVENT':<18} "
        f"{'LOCATION':<22} DETAILS"
    )
    print("-" * 125)

    if not rows:
        print("No events found.")
        return

    for timestamp, source, event_type, location, details, raw_hash in rows:
        print(
            f"{str(timestamp):<27} {source:<13} {event_type:<18} "
            f"{str(location or ''):<22} {details or ''}"
        )


def compare_badge_and_device(employee_id: str, minutes: int = 10) -> None:
    employee_id = employee_id.strip().upper()

    connection = mysql_connect()
    try:
        cursor = connection.cursor(dictionary=True)
        cursor.execute(
            """
            SELECT event_timestamp_utc, event_source, event_type, location, details
            FROM employee_activity_timeline
            WHERE employee_id = %s
              AND event_source IN ('BADGE', 'DEVICE')
            ORDER BY event_timestamp_utc
            """,
            (employee_id,),
        )
        rows = cursor.fetchall()
        cursor.close()
    finally:
        connection.close()

    badges = [row for row in rows if row["event_source"] == "BADGE"]
    devices = [row for row in rows if row["event_source"] == "DEVICE"]

    print(f"\nBADGE VS DEVICE FOR {employee_id} (within {minutes} minutes)")
    print("=" * 130)

    if not badges or not devices:
        print("Not enough badge/device data for that employee.")
        return

    matches = 0

    for device in devices:
        closest_badge = min(
            badges,
            key=lambda badge: abs(
                (badge["event_timestamp_utc"] - device["event_timestamp_utc"]).total_seconds()
            ),
        )

        difference = abs(
            (closest_badge["event_timestamp_utc"] - device["event_timestamp_utc"]).total_seconds()
        ) / 60

        if difference <= minutes:
            matches += 1
            print(
                f"DEVICE {device['event_timestamp_utc']}  "
                f"{device['event_type']:<15} {device['location']}"
            )
            print(
                f"BADGE  {closest_badge['event_timestamp_utc']}  "
                f"{closest_badge['event_type']:<15} {closest_badge['location']}"
            )
            print(f"Difference: {difference:.2f} minutes")
            print("-" * 130)

    if matches == 0:
        print(f"No badge and device events were within {minutes} minutes.")


# -----------------------------------------------------------------------------
# COMMANDS
# -----------------------------------------------------------------------------

def run_once() -> None:
    setup_mysql_tables()
    ingest_original_files()

    print("\nChecking PostgreSQL for new transaction records...")
    inserted = poll_postgres_once()
    print(f"PostgreSQL polling complete. {inserted} new transaction event(s) inserted.")


def run_watch() -> None:
    setup_mysql_tables()
    ingest_original_files()

    print(
        f"\nWatching PostgreSQL every {PG_POLL_SECONDS} seconds. "
        "Press Ctrl+C to stop."
    )

    while True:
        try:
            inserted = poll_postgres_once()
            now = datetime.now(timezone.utc).isoformat(timespec="seconds")
            print(f"[{now}] PostgreSQL check complete: {inserted} new event(s).")
            time.sleep(PG_POLL_SECONDS)
        except KeyboardInterrupt:
            print("\nStopped PostgreSQL polling.")
            break
        except Exception as exc:
            print(f"PostgreSQL poll failed: {exc}", file=sys.stderr)
            print(f"Retrying in {PG_POLL_SECONDS} seconds...", file=sys.stderr)
            time.sleep(PG_POLL_SECONDS)


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Case Closed data engineering pipeline")

    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--watch",
        action="store_true",
        help="Load file evidence, then poll PostgreSQL continuously",
    )
    mode.add_argument(
        "--postgres-once",
        action="store_true",
        help="Poll PostgreSQL once without reloading the files",
    )
    mode.add_argument(
        "--compare",
        metavar="EMPLOYEE_ID",
        help="Compare badge events to nearby device events from MySQL",
    )
    mode.add_argument(
        "--timeline",
        metavar="EMPLOYEE_ID",
        help="Show one employee's complete unified MySQL timeline",
    )

    parser.add_argument(
        "--minutes",
        type=int,
        default=10,
        help="Time window for --compare (default: 10 minutes)",
    )
    parser.add_argument("--start", help="Optional UTC start time for --timeline")
    parser.add_argument("--end", help="Optional UTC end time for --timeline")

    return parser


def main() -> None:
    args = build_argument_parser().parse_args()

    if args.watch:
        run_watch()
        return

    if args.postgres_once:
        setup_mysql_tables()
        inserted = poll_postgres_once()
        print(f"Inserted {inserted} new PostgreSQL transaction event(s).")
        return

    if args.compare:
        setup_mysql_tables()
        compare_badge_and_device(args.compare, args.minutes)
        return

    if args.timeline:
        setup_mysql_tables()
        print_employee_timeline(args.timeline, args.start, args.end)
        return

    # Default mode: run one complete ETL pass.
    run_once()


if __name__ == "__main__":
    main()
