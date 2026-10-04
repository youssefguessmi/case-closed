"""Validate LLM-generated SQL before it touches the database.

Defence in depth: the chatbot also connects with a SELECT-only MySQL account,
but we never rely on the model "behaving". A query must pass all of:
  * exactly one statement, and it is a SELECT (CTEs / UNION allowed)
  * only allow-listed curated tables are referenced
  * no dangerous functions (SLEEP, LOAD_FILE, ...) and no SELECT ... INTO
  * a row LIMIT is enforced
What gets executed is the re-serialised AST, i.e. exactly what was validated.
"""
import re

import sqlglot
from sqlglot import exp

import config

ALLOWED_TABLES = {
    "curated.employee_activity_timeline",
    "curated.locations",
    "curated.alibi_statements",
}
DEFAULT_DB = "curated"

BLOCKED_FUNCTIONS = {
    "SLEEP", "BENCHMARK", "LOAD_FILE", "GET_LOCK", "RELEASE_LOCK", "RELEASE_ALL_LOCKS",
    "IS_FREE_LOCK", "IS_USED_LOCK", "SYS_EXEC", "SYS_EVAL", "UUID_SHORT", "MASTER_POS_WAIT",
    "SOURCE_POS_WAIT", "WAIT_FOR_EXECUTED_GTID_SET", "SET_USER_ID",
}
BLOCKED_NODES = (
    exp.Insert, exp.Update, exp.Delete, exp.Drop, exp.Create, exp.Alter, exp.Command,
    exp.Into, exp.Merge, exp.TruncateTable, exp.Grant, exp.Set, exp.Use, exp.Lock,
)


class UnsafeSQLError(ValueError):
    pass


_ISO_Z = re.compile(r"'(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}(?::\d{2})?)(?:\.\d+)?Z'")


def extract_sql(text: str) -> str:
    """Pull SQL out of an LLM reply (```sql fences, stray prose, trailing ;)."""
    m = re.search(r"```(?:sql|mysql)?\s*(.*?)```", text, re.S | re.I)
    sql = m.group(1) if m else text
    m = re.search(r"\b(WITH|SELECT)\b.*", sql, re.S | re.I)
    sql = m.group(0) if m else sql
    return sql.strip().rstrip(";").strip()


def normalize(sql: str) -> str:
    # MySQL DATETIME literals cannot carry a 'Z'; all stored times are already UTC.
    return _ISO_Z.sub(lambda m: f"'{m[1]} {m[2]}'", sql)


def validate(sql: str, max_rows: int = config.MAX_ROWS) -> str:
    sql = normalize(sql)
    try:
        statements = [s for s in sqlglot.parse(sql, read="mysql") if s is not None]
    except sqlglot.errors.ParseError as e:
        raise UnsafeSQLError(f"SQL could not be parsed: {e}") from e
    if len(statements) != 1:
        raise UnsafeSQLError("Only one SQL statement is allowed.")
    root = statements[0]

    if not isinstance(root, (exp.Select, exp.SetOperation)):
        raise UnsafeSQLError(f"Only SELECT queries are allowed (got {root.key.upper()}).")

    for node in root.walk():
        if isinstance(node, BLOCKED_NODES):
            raise UnsafeSQLError(f"Forbidden SQL construct: {node.key.upper()}")
        if isinstance(node, exp.Func):
            name = (node.name if isinstance(node, exp.Anonymous) else node.sql_name()).upper()
            if name in BLOCKED_FUNCTIONS:
                raise UnsafeSQLError(f"Forbidden function: {name}")

    cte_names = {cte.alias_or_name.lower() for cte in root.find_all(exp.CTE)}
    for table in root.find_all(exp.Table):
        name = table.name.lower()
        if not table.args.get("db") and name in cte_names:
            continue
        if table.args.get("catalog"):
            raise UnsafeSQLError(f"Table not allowed: {table.sql('mysql')}")
        db = (table.db or DEFAULT_DB).lower()
        full = f"{db}.{name}"
        if full not in ALLOWED_TABLES:
            raise UnsafeSQLError(
                f"Table not allowed: {full}. Allowed: {', '.join(sorted(ALLOWED_TABLES))}")
        table.set("db", exp.to_identifier(db))

    limit = root.args.get("limit")
    if limit is None:
        root = root.limit(max_rows)
    else:
        try:
            if int(limit.expression.name) > max_rows:
                root = root.limit(max_rows)
        except (AttributeError, ValueError):
            root = root.limit(max_rows)

    return root.sql(dialect="mysql", pretty=True)
