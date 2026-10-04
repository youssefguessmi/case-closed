"""Tests that need neither MySQL nor Gemma:  python -m pytest tests"""
import pytest

import config
from pipeline.parsers import (RAW_PARSERS, parse_badge, parse_device, parse_transactions, sha256)
from serving.sql_guard import UnsafeSQLError, extract_sql, validate


# ---------- parsers ----------
def test_parsers_row_counts_and_hashes():
    badge = parse_badge(config.DATA_FILES["badge"])
    device = parse_device(config.DATA_FILES["device"])
    txns = parse_transactions(config.DATA_FILES["transaction"])
    assert len(badge) == 50 and len(device) == 50 and len(txns) == 50
    for r in badge + device + txns:
        assert r["raw_hash"] == sha256(r["raw_line"])
    assert len({r["raw_hash"] for r in badge + device + txns}) == 150


def test_timestamps_normalized_to_utc():
    device = parse_device(config.DATA_FILES["device"])
    usb = next(r for r in device if r["event_code"] == "USB_INSERTED")
    assert usb["event_ts"].isoformat() == "2026-08-14T23:44:17"
    assert usb["user_id"] == "EMP-0011"


def test_raw_line_reparses_to_same_record():
    for key, table in [("badge", "raw.badge_access"), ("device", "raw.device_logs"),
                       ("transaction", "raw.building_transactions")]:
        parse_file = {"badge": parse_badge, "device": parse_device, "transaction": parse_transactions}[key]
        parser, _ = RAW_PARSERS[table]
        for r in parse_file(config.DATA_FILES[key]):
            assert parser(r["raw_line"]) == r


# ---------- SQL guard ----------
def test_extract_sql_from_fenced_reply():
    assert extract_sql("Sure!\n```sql\nSELECT 1 FROM curated.locations;\n```") == "SELECT 1 FROM curated.locations"


def test_iso_z_literals_normalized_and_limit_added():
    out = validate("SELECT * FROM curated.employee_activity_timeline "
                   "WHERE event_timestamp > '2026-08-14T22:00:00Z'")
    assert "'2026-08-14 22:00:00'" in out
    assert out.rstrip().endswith(f"LIMIT {config.MAX_ROWS}")


def test_unqualified_table_gets_curated_schema():
    assert "curated.locations" in validate("SELECT * FROM locations")


def test_large_limit_is_capped():
    assert f"LIMIT {config.MAX_ROWS}" in validate("SELECT * FROM curated.locations LIMIT 999999")


@pytest.mark.parametrize("sql", [
    "DELETE FROM curated.employee_activity_timeline",
    "UPDATE curated.locations SET floor = '1'",
    "DROP TABLE curated.locations",
    "SELECT 1; DROP TABLE curated.locations",
    "SELECT * FROM raw.badge_access",
    "SELECT * FROM audit.query_log",
    "SELECT * FROM mysql.user",
    "SELECT * FROM information_schema.tables",
    "SELECT SLEEP(5)",
    "SELECT BENCHMARK(1000000, MD5('x'))",
    "SELECT LOAD_FILE('/etc/passwd')",
    "SELECT * FROM curated.locations INTO OUTFILE '/tmp/x'",
    "INSERT INTO curated.locations VALUES ('x','y','1','z')",
    "SELECT * FROM curated.locations WHERE location IN (SELECT user FROM mysql.user)",
])
def test_unsafe_sql_rejected(sql):
    with pytest.raises(UnsafeSQLError):
        validate(sql)


def test_cte_and_union_allowed():
    validate("WITH x AS (SELECT employee_id FROM curated.alibi_statements) SELECT * FROM x")
    validate("SELECT location FROM curated.locations UNION SELECT employee_id FROM curated.alibi_statements")


# ---------- SQL export ----------
def test_export_script_contains_all_rows_and_hashes():
    from pipeline.export_sql import build_script
    script = build_script()
    for r in parse_badge(config.DATA_FILES["badge"]):
        assert r["raw_hash"] in script
    assert script.count("INSERT INTO `raw`.ingestion_manifest") == 3
    assert "CREATE TRIGGER" in script and "curated.employee_activity_timeline" in script
