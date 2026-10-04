"""MySQL connections for the serving layer. No admin credentials here."""
import pymysql

import config


def readonly_conn():
    """Connection used to run investigator queries: SELECT-only account, read-only txn, timeout."""
    conn = pymysql.connect(
        host=config.MYSQL_HOST, port=config.MYSQL_PORT,
        user=config.MYSQL_RO_USER, password=config.MYSQL_RO_PASSWORD,
        database="curated", autocommit=False, connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute("SET SESSION MAX_EXECUTION_TIME = %s", (config.QUERY_TIMEOUT_MS,))
        cur.execute("SET SESSION TRANSACTION READ ONLY")
    return conn


def audit_conn():
    return pymysql.connect(
        host=config.MYSQL_HOST, port=config.MYSQL_PORT,
        user=config.MYSQL_AUDIT_USER, password=config.MYSQL_AUDIT_PASSWORD,
        database="audit", autocommit=True, connect_timeout=5,
    )


def run_select(sql: str) -> tuple[list[str], list[tuple]]:
    conn = readonly_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            columns = [d[0] for d in cur.description or []]
            rows = list(cur.fetchmany(config.MAX_ROWS))
        conn.rollback()
        return columns, rows
    finally:
        conn.close()
