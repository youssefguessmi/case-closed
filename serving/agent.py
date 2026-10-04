"""Investigator agent: question -> Gemma writes SQL -> guard -> MySQL (read-only)
-> chain-of-custody check -> audit log -> Gemma explains the result."""
from collections.abc import Iterator
from dataclasses import dataclass, field

import pymysql

import config
from serving import audit, prompts
from serving.custody import CustodyReport, verify_hashes
from serving.db import run_select
from serving.llm import GemmaClient
from serving.sql_guard import UnsafeSQLError, extract_sql, validate


@dataclass
class QueryResult:
    question: str
    sql: str | None = None
    columns: list[str] = field(default_factory=list)
    rows: list[tuple] = field(default_factory=list)
    error: str | None = None
    attempts: list[dict] = field(default_factory=list)  # [{'sql':..., 'error':...}]
    custody: CustodyReport = field(default_factory=CustodyReport)
    audit_id: int | None = None
    audit_error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None

    def custody_note(self) -> str:
        c = self.custody
        if "raw_event_hash" not in self.columns:
            return "Not applicable (aggregate result without per-event hashes)."
        if c.checked == 0:
            return "No events to verify."
        if c.ok:
            return f"All {c.verified} events verified against original source records."
        return f"WARNING: {len(c.failures)} of {c.checked} events FAILED verification."


class InvestigatorAgent:
    def __init__(self, llm: GemmaClient | None = None):
        self.llm = llm or GemmaClient()

    def run(self, question: str, history: list[dict] | None = None,
            sql_override: str | None = None) -> QueryResult:
        """sql_override runs a known SQL (e.g. a saved query) through the same guard/audit path."""
        res = QueryResult(question=question)
        messages = prompts.sql_messages(question, history or [])

        for attempt in range(config.MAX_SQL_RETRIES + 1):
            if sql_override and attempt == 0:
                raw = sql_override
            else:
                reply = self.llm.chat(messages)
                messages.append({"role": "assistant", "content": reply})
                raw = extract_sql(reply)
            try:
                sql = validate(raw)
                columns, rows = run_select(sql)
            except UnsafeSQLError as e:
                res.attempts.append({"sql": raw, "error": str(e)})
                messages.append(prompts.repair_message(str(e)))
                continue
            except pymysql.MySQLError as e:
                msg = e.args[1] if len(e.args) > 1 else str(e)
                res.attempts.append({"sql": sql, "error": msg})
                messages.append(prompts.repair_message(msg))
                continue
            res.sql, res.columns, res.rows = sql, columns, rows
            res.attempts.append({"sql": sql, "error": None})
            break
        else:
            res.error = res.attempts[-1]["error"] if res.attempts else "No SQL generated"
            res.sql = res.attempts[-1]["sql"] if res.attempts else None

        if res.ok and "raw_event_hash" in res.columns:
            i = res.columns.index("raw_event_hash")
            res.custody = verify_hashes([r[i] for r in res.rows])

        try:
            res.audit_id = audit.log_query(
                question=question, generated_sql=res.sql,
                status="ok" if res.ok else "failed",
                row_count=len(res.rows) if res.ok else None,
                result_sha256=audit.result_hash(res.columns, res.rows) if res.ok else None,
                records_verified=res.custody.verified if res.custody.checked else None,
                records_failed=len(res.custody.failures) if res.custody.checked else None,
            )
        except pymysql.MySQLError as e:
            res.audit_error = str(e)
        return res

    def explain(self, res: QueryResult) -> Iterator[str]:
        if not res.ok:
            yield (f"I couldn't build a working query for that after {len(res.attempts)} attempt(s). "
                   f"Last error: {res.error}\n\nTry rephrasing, or name the employee ID and time window.")
            return
        yield from self.llm.stream(
            prompts.answer_messages(res.question, res.sql, res.columns, res.rows, res.custody_note()),
            temperature=0.2,
        )
