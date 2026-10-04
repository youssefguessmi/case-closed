"""Hand-written, reviewed queries for the core investigation questions.

They run through the same guard / custody / audit path as Gemma's SQL, and give
a deterministic baseline to compare the model's output against.
"""

TIMELINE = """\
SELECT event_source, employee_id, event_timestamp, event_type, location, details, raw_event_hash
FROM curated.employee_activity_timeline
WHERE event_timestamp BETWEEN '2026-08-14 22:00:00' AND '2026-08-15 00:00:00'
  AND employee_id IN ('EMP-0047', 'EMP-0031', 'EMP-0092')
ORDER BY employee_id, event_timestamp"""

# Each alibi encoded as a testable claim:
#   claimed_floor / claimed_location : where they said they were (NULL = no claim)
#   claimed_departure                : when they said they left (NULL = no claim)
ALIBI_CONTRADICTIONS = """\
WITH claims AS (
  SELECT 'EMP-0047' AS employee_id, '3' AS claimed_floor, NULL AS claimed_location,
         CAST('2026-08-15 00:00:00' AS DATETIME) AS claimed_departure, 30 AS tolerance_min
  UNION ALL
  SELECT 'EMP-0031', NULL, 'D-SERVER', CAST('2026-08-14 23:00:00' AS DATETIME), 0
  UNION ALL
  SELECT 'EMP-0092', NULL, NULL, NULL, 0
)
SELECT t.employee_id, t.event_timestamp, t.event_source, t.event_type, t.location, t.details,
  CASE
    WHEN c.claimed_departure IS NOT NULL
         AND t.event_timestamp > c.claimed_departure + INTERVAL c.tolerance_min MINUTE
      THEN 'Activity after claimed departure time'
    WHEN c.claimed_departure IS NOT NULL AND t.event_type = 'BADGE_OUT'
         AND (t.location = c.claimed_location OR t.floor = c.claimed_floor)
         AND t.event_timestamp < c.claimed_departure - INTERVAL 30 MINUTE
      THEN 'Left claimed location well before claimed departure'
    WHEN c.claimed_floor IS NOT NULL AND t.event_source IN ('badge', 'transaction')
         AND t.floor IS NOT NULL AND t.floor <> c.claimed_floor
      THEN 'Physically recorded away from claimed floor'
    WHEN c.claimed_location IS NOT NULL AND t.event_source = 'badge'
         AND t.location <> c.claimed_location
      THEN 'Badge used away from claimed location'
  END AS contradiction,
  t.raw_event_hash
FROM curated.employee_activity_timeline t
JOIN claims c ON c.employee_id = t.employee_id
WHERE t.event_timestamp BETWEEN '2026-08-14 22:00:00' AND '2026-08-15 01:00:00'
HAVING contradiction IS NOT NULL
ORDER BY t.employee_id, t.event_timestamp"""

ON_SITE = """\
SELECT employee_id, MIN(event_timestamp) AS first_seen, MAX(event_timestamp) AS last_seen,
       COUNT(*) AS events, GROUP_CONCAT(DISTINCT event_source) AS sources
FROM curated.employee_activity_timeline
WHERE event_timestamp BETWEEN '2026-08-14 22:00:00' AND '2026-08-15 00:00:00'
GROUP BY employee_id
ORDER BY first_seen"""

SAVED = {
    "Timeline: 3 suspects, 10 PM - midnight": (
        "Reconstruct the timeline for EMP-0047, EMP-0031 and EMP-0092 between 10 PM and midnight.",
        TIMELINE),
    "Flag alibi contradictions": (
        "Which events contradict the three alibis?", ALIBI_CONTRADICTIONS),
    "Everyone on-site 10 PM - midnight": (
        "Who had any recorded activity between 10 PM and midnight?", ON_SITE),
}
