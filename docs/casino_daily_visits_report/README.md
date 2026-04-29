# Casino Daily Visits Report

Automated daily Excel export of yesterday's visit-sessions data, delivered by email.

## What this is

Each morning, the daily Casino DW pipeline produces a spreadsheet that lists, for the prior gaming day, every casino visit joined to any overlapping player gaming sessions. The report replaces a manual SSMS query that an analyst was running and emailing by hand.

- **Recipient (current, testing phase):** `data@tourinvestgroup.com`
- **Recipient (future, after validation):** team distribution (see `.env.example`)
- **Cadence:** daily, automatic, after the 06:00 gold refresh
- **Format:** `.xlsx`, 8 columns, matches the legacy SSMS export sample
- **Failure mode:** silent — a failed report does not fail the pipeline; check Prefect logs

## Isolation guarantee — does NOT touch the data pipeline

This report is a **strictly read-only consumer** of the warehouse. Nothing it does can affect bronze/silver/gold loads, schema state, or the daily pipeline status. Specific guarantees, enforced in code:

| Concern | Mitigation | Where |
|---|---|---|
| Could it write to the DB? | `gold.fn_visit_sessions` is `STABLE`, returns rows from CTEs only — no INSERT/UPDATE/DELETE/DDL anywhere | [32_gold_visit_sessions_view.sql](../../medallion_pg/sql/32_gold_visit_sessions_view.sql) |
| Could it create/drop schema objects? | The script issues SELECT only against the function | [export_visit_sessions.py](../../medallion_pg/orchestration/python/export_visit_sessions.py) |
| Could it mark the pipeline failed? | The report task runs **outside** the pipeline-health `try/except` block. Even if the task raises, status updates are already committed | [prefect_flow.py](../../medallion_pg/orchestration/python/prefect_flow.py) flow body |
| Could it trigger a false alarm email? | Same as above — the failure email path can only fire on bronze/silver/gold failures | [prefect_flow.py](../../medallion_pg/orchestration/python/prefect_flow.py) `send_status_email` |
| Could it hang the daily flow? | Subprocess hard-timeout of 5 minutes; SMTP timeout 30s | `_run_python_script(..., timeout=300)` |
| Could it overload SMTP retries? | `retries=0` on the Prefect task; one attempt, swallow on failure | `@task(name="export-visits-report", retries=0)` |
| Could it block tomorrow's pipeline? | No — it is the **last** step and runs even if it raises | flow ordering |

If the team distribution is misconfigured or the SMTP server is down, the data pipeline keeps running cleanly tomorrow. The team simply doesn't get a report email that morning, visible in Prefect logs as a `WARNING`.

## Components

| Layer | Path | Role |
|---|---|---|
| SQL | [`medallion_pg/sql/32_gold_visit_sessions_view.sql`](../../medallion_pg/sql/32_gold_visit_sessions_view.sql) | `gold.fn_visit_sessions(from_date, to_date)` — the data source |
| Script | [`medallion_pg/orchestration/python/export_visit_sessions.py`](../../medallion_pg/orchestration/python/export_visit_sessions.py) | Queries the function, writes xlsx, sends email |
| Orchestration | [`medallion_pg/orchestration/python/prefect_flow.py`](../../medallion_pg/orchestration/python/prefect_flow.py) | `run_visits_report` task, runs after `run_gold_refresh` |
| Output | `medallion_pg/reports/casino_daily_visits_YYYY-MM-DD.xlsx` | Local copy, gitignored |
| Config | `medallion_pg/orchestration/python/.env` | `REPORT_TO_EMAILS`, reuses existing `SMTP_*` vars |

## Excel template (locked)

| Col | Header | Type | Format | NULL handling |
|---|---|---|---|---|
| A | GamingDay | datetime | `mm-dd-yy` | n/a (always present) |
| B | Membership | int | General | n/a |
| C | VisitNo | int | General | n/a |
| D | CasinoEntryTime | time | `h:mm` | n/a |
| E | CasinoExitTime | time | `h:mm` | n/a |
| F | SessionStart | time or text | `h:mm` | literal `"NULL"` if no session |
| G | SessionFinish | time or text | `h:mm` | literal `"NULL"` if no session |
| H | averBet | number or text | General | literal `"NULL"` if no session |

The literal `"NULL"` strings are intentional — they match the original SSMS-export the team has been receiving. Do not switch to empty cells without team agreement (downstream filters/formulas may rely on them).

## Operations

### Install / first-time setup

```bash
# 1. Install the SQL function in PostgreSQL
psql -h <host> -U <user> -d casino_dw -f medallion_pg/sql/32_gold_visit_sessions_view.sql

# 2. Install the new Python dependency
cd medallion_pg/orchestration/python
pip install -r requirements.txt

# 3. Add REPORT_TO_EMAILS to .env (see .env.example)
```

### Manual one-off run

```bash
cd medallion_pg/orchestration/python
python export_visit_sessions.py                             # yesterday, email
python export_visit_sessions.py --no-email                  # yesterday, file only
python export_visit_sessions.py --from-date 2026-04-22 \
                                --to-date   2026-04-22      # specific day
```

### Promoting to the team distribution list

When the report is validated and ready to fan out:

1. Update `REPORT_TO_EMAILS` in `medallion_pg/orchestration/python/.env` to include the team addresses listed in `.env.example`.
2. Run a manual test: `python export_visit_sessions.py`
3. Once confirmed, leave it alone — the daily flow uses the same env var.

## Folder layout

```
docs/casino_daily_visits_report/
├── README.md         <- this file
├── user_stories.md   <- the team's request and acceptance criteria
├── workflows.md      <- step-by-step daily workflow with timing
└── Archive/          <- superseded decisions / replaced versions
```
