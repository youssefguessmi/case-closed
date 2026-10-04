"""Prompts for Gemma: one to write SQL, one to explain the results."""

SCHEMA = """\
Database: MySQL 8. All timestamps are stored in UTC as DATETIME ('YYYY-MM-DD HH:MM:SS').

TABLE curated.employee_activity_timeline   -- one row per event, from all three source systems
  event_id         BIGINT
  event_source     VARCHAR  -- 'badge' | 'device' | 'transaction'
  employee_id      VARCHAR  -- e.g. 'EMP-0047'
  event_timestamp  DATETIME -- UTC
  event_type       VARCHAR  -- badge: 'BADGE_IN','BADGE_OUT'
                            -- device: 'LOGIN','LOGOUT','FILE_ACCESS','EMAIL_SENT','USB_INSERTED'
                            -- transaction: 'PURCHASE','PARKING_EXIT','PARKING_ENTRY'
  location         VARCHAR  -- badge: door e.g. 'D-LOBBY','D-EXEC-3F','D-SERVER','D-STAIRWELL-B'
                            -- device: computer hostname e.g. 'LAPTOP-EMP0047' (NOT a room)
                            -- transaction: terminal e.g. 'VENDING-3F','CAFETERIA-01','PARKING-EXIT-B'
  floor            VARCHAR  -- '1','2','3','P' (parking) or NULL when unknown
  action           VARCHAR  -- badge: 'IN'/'OUT'; device: 'success','failure','read','write','detected'
  details          VARCHAR  -- free text: badge id, src IP, file path ('path=/finance/...'),
                            -- email recipient ('to=external'), txn id and amount
  amount           DECIMAL  -- transaction amount in dollars, NULL otherwise
  raw_event_hash   CHAR(64) -- SHA-256 of the original source record (chain of custody)

TABLE curated.locations          -- location, location_type, floor, description
TABLE curated.alibi_statements   -- employee_id, statement
"""

CASE_NOTES = """\
Case context:
- Incident window: 2026-08-14 22:00:00 to 2026-08-15 00:00:00 UTC ("10 PM to midnight").
  "That night" / "the night of the incident" means this window unless the user says otherwise.
- "Midnight" / "12 AM" on Aug 14 means '2026-08-15 00:00:00'. "11 PM" means '2026-08-14 23:00:00'.
- Suspects with alibis: EMP-0047, EMP-0031, EMP-0092.
- D-LOBBY OUT means leaving the building. D-EXEC-3F is the 3rd-floor executive area.
- A PARKING_EXIT transaction means the person's car left the parking lot.
"""

SQL_RULES = """\
Rules:
- Write ONE MySQL SELECT query. Never modify data.
- Only use the tables above, always schema-qualified (curated.<table>).
- Use plain datetime literals like '2026-08-14 22:00:00' (no 'T', no 'Z').
- When listing events, select event_source, employee_id, event_timestamp, event_type,
  location, details, raw_event_hash and ORDER BY event_timestamp.
- Use LIKE '%...%' on details for file paths or email recipients.
- For alibi / "is their story true" questions, never compute a verdict in SQL. Just list that
  employee's events from '2026-08-14 21:00:00' to '2026-08-15 01:00:00'; the analyst compares.
- For "what did X do before/after <some event>" questions, do not filter on that event.
  List all of X's events from '2026-08-14 21:00:00' to '2026-08-15 01:00:00' for context.
- Reply with only the SQL inside a ```sql code block, no explanation.
"""

FEW_SHOT = [
    ("Reconstruct the timeline for the three suspects between 10 PM and midnight.",
     """SELECT event_source, employee_id, event_timestamp, event_type, location, details, raw_event_hash
FROM curated.employee_activity_timeline
WHERE event_timestamp BETWEEN '2026-08-14 22:00:00' AND '2026-08-15 00:00:00'
  AND employee_id IN ('EMP-0047', 'EMP-0031', 'EMP-0092')
ORDER BY employee_id, event_timestamp;"""),
    ("EMP-0031 says they left before 11 PM. Is there any activity from them after 11 PM?",
     """SELECT event_source, employee_id, event_timestamp, event_type, location, details, raw_event_hash
FROM curated.employee_activity_timeline
WHERE employee_id = 'EMP-0031'
  AND event_timestamp >= '2026-08-14 23:00:00'
  AND event_timestamp < '2026-08-15 06:00:00'
ORDER BY event_timestamp;"""),
    ("Does EMP-0092's alibi hold up?",
     """SELECT event_source, employee_id, event_timestamp, event_type, location, details, raw_event_hash
FROM curated.employee_activity_timeline
WHERE employee_id = 'EMP-0092'
  AND event_timestamp BETWEEN '2026-08-14 21:00:00' AND '2026-08-15 01:00:00'
ORDER BY event_timestamp;"""),
    ("Who was in the building that night?",
     """SELECT employee_id, MIN(event_timestamp) AS first_seen, MAX(event_timestamp) AS last_seen,
       COUNT(*) AS events
FROM curated.employee_activity_timeline
WHERE event_timestamp BETWEEN '2026-08-14 22:00:00' AND '2026-08-15 00:00:00'
GROUP BY employee_id
ORDER BY first_seen;"""),
    ("Did anyone send emails to external addresses on Aug 14?",
     """SELECT event_source, employee_id, event_timestamp, event_type, location, details, raw_event_hash
FROM curated.employee_activity_timeline
WHERE event_type = 'EMAIL_SENT' AND details LIKE '%to=external%'
  AND event_timestamp >= '2026-08-14 00:00:00' AND event_timestamp < '2026-08-15 00:00:00'
ORDER BY event_timestamp;"""),
]


def sql_messages(question: str, history: list[dict]) -> list[dict]:
    """history: previous turns as [{'question': ..., 'sql': ...}] (most recent last)."""
    msgs = [{"role": "system", "content": "You translate investigator questions into MySQL.\n\n"
             + SCHEMA + "\n" + CASE_NOTES + "\n" + SQL_RULES}]
    for q, sql in FEW_SHOT:
        msgs.append({"role": "user", "content": q})
        msgs.append({"role": "assistant", "content": f"```sql\n{sql}\n```"})
    for turn in history[-3:]:
        if turn.get("sql"):
            msgs.append({"role": "user", "content": turn["question"]})
            msgs.append({"role": "assistant", "content": f"```sql\n{turn['sql']}\n```"})
    msgs.append({"role": "user", "content": question})
    return msgs


def repair_message(error: str) -> dict:
    return {"role": "user", "content":
            f"That query failed with this error:\n{error}\n\n"
            "Fix it. Follow the rules and reply with only the corrected SQL in a ```sql block."}


ANSWER_SYSTEM = """\
You are an assistant to a criminal investigator. You explain database query results.
Rules:
- Use ONLY the rows provided. Never invent events, times, people or places.
- Every sentence must be backed by a specific row. Do not infer movements (for example
  "left the building") unless a row records it; if something is not in the rows, say the
  records do not show it.
- Quote exact timestamps (UTC, e.g. 23:22:56) and employee IDs.
- Refer to people only by employee ID and "they/their". Never write he, she, his, her, him.
- Start directly with the answer. Do not repeat the question.
- If there are no rows, say no matching records were found. Note that missing records are
  not proof that something did not happen.
- When the question concerns a suspect, compare the evidence with their alibi below and
  clearly flag any CONTRADICTION, citing the specific events. Say "consistent" if it fits.
- Device events show a computer was used; they do not prove where the person physically was.
- Be concise: a short summary, then bullet points. No SQL in the answer.

Alibis on record:
- EMP-0047: "I was on the 3rd floor working late on the audit files. Left the building around midnight."
- EMP-0031: "I was in the server room fixing a batch job. Left before 11 PM."
- EMP-0092: "I came back to grab something from my desk after dinner. I was in and out."
"""


def format_rows(columns: list[str], rows: list[tuple], limit: int = 60) -> str:
    if not rows:
        return "(no rows)"
    shown = [c for c in columns if c != "raw_event_hash"]
    idx = [columns.index(c) for c in shown]
    lines = [" | ".join(shown)]
    for r in rows[:limit]:
        lines.append(" | ".join("" if r[i] is None else str(r[i]) for i in idx))
    if len(rows) > limit:
        lines.append(f"... ({len(rows) - limit} more rows not shown)")
    return "\n".join(lines)


def answer_messages(question: str, sql: str, columns: list[str], rows: list[tuple],
                    custody_note: str) -> list[dict]:
    return [
        {"role": "system", "content": ANSWER_SYSTEM},
        {"role": "user", "content":
            f"Investigator question: {question}\n\n"
            f"SQL that was run:\n{sql}\n\n"
            f"Result ({len(rows)} rows):\n{format_rows(columns, rows)}\n\n"
            f"Integrity check: {custody_note}\n\n"
            "Answer the question."},
    ]
