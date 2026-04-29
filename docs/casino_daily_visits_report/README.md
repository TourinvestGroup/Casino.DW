# Casino Daily Visits Report

Automated daily Excel export of yesterday's visit-sessions data, delivered by email.

## What this is

Each morning, the daily Casino DW pipeline produces a spreadsheet that lists, for the prior gaming day, every casino visit joined to any overlapping player gaming sessions. The report replaces a manual SSMS query that an analyst was running and emailing by hand.

- **SAFE recipient (always):** `data@tourinvestgroup.com`
- **TEAM recipients (only on healthy runs):** 5 cibatumi.com team addresses (see `.env.example`)
- **Cadence:** **separate** scheduled task at 07:00 daily — one hour after the data pipeline at 06:00, decoupling data freshness SLA from report-delivery SLA
- **Format:** `.xlsx`, 8 columns, matches the legacy SSMS export sample
- **Failure mode:** silent — a failed report does NOT fail the pipeline; degraded runs (no fresh data) drop the team list and tag the email `[DEGRADED]`

## Isolation guarantee — does NOT touch the data pipeline

This report is a **strictly read-only consumer** of the warehouse. Nothing it does can affect bronze/silver/gold loads, schema state, or the daily pipeline status. Specific guarantees, enforced in code:

| Concern | Mitigation | Where |
|---|---|---|
| Could it write to the DB? | `gold.fn_visit_sessions` is `STABLE`, returns rows from CTEs only — no INSERT/UPDATE/DELETE/DDL anywhere | [32_gold_visit_sessions_view.sql](../../medallion_pg/sql/32_gold_visit_sessions_view.sql) |
| Could it create/drop schema objects? | The script issues SELECT only against the function and `gold.fact_membership_day` (count check) | [export_visit_sessions.py](../../medallion_pg/orchestration/python/export_visit_sessions.py) |
| Could it mark the pipeline failed? | The report runs as a **separate** Task Scheduler entry at 07:00. The 06:00 pipeline finishes and exits before the report process starts | scheduling |
| Could it trigger a false alarm email? | The report uses its own SAFE/TEAM recipients (`REPORT_TO_EMAILS_*`); pipeline status email is unrelated | env split |
| Could it hang the pipeline? | Different processes — the pipeline can't be hung by a process that hasn't started yet | scheduling |
| Could it spam team on broken days? | Health-check (`gold.fact_membership_day` row count) drops the TEAM list when degraded — only SAFE list gets the alert | `pipeline_is_healthy()` |

If the SMTP server is down or credentials change, the data pipeline keeps running cleanly tomorrow. The team simply doesn't get a report email that morning; details land in `logs/daily_report.log`.

## Components

| Layer | Path | Role |
|---|---|---|
| SQL function | [`medallion_pg/sql/32_gold_visit_sessions_view.sql`](../../medallion_pg/sql/32_gold_visit_sessions_view.sql) | `gold.fn_visit_sessions(from_date, to_date)` — the data source |
| Health check | (inline in script) | `SELECT COUNT(*) FROM gold.fact_membership_day WHERE gamingday = ?` |
| Script | [`medallion_pg/orchestration/python/export_visit_sessions.py`](../../medallion_pg/orchestration/python/export_visit_sessions.py) | Queries the function, writes xlsx, sends email |
| Bat wrapper | [`scripts/run_daily_report.bat`](../../scripts/run_daily_report.bat) | What Task Scheduler runs at 07:00 |
| Task registration | [`scripts/register_visits_report_task.ps1`](../../scripts/register_visits_report_task.ps1) | One-time admin script that creates `CasinoDW_VisitsReport` |
| Output | `medallion_pg/reports/casino_daily_visits_YYYY-MM-DD.xlsx` | Local copy, gitignored |
| Logs | `logs/daily_report.log` | gitignored |
| Config | `medallion_pg/orchestration/python/.env` | `REPORT_TO_EMAILS_SAFE`, `REPORT_TO_EMAILS_TEAM`, reuses existing `SMTP_*` vars |

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

## Recipient routing

```
                          ┌───────────────────────────────────────┐
                          │  SELECT COUNT(*)                       │
                          │   FROM gold.fact_membership_day        │
                          │   WHERE gamingday = <target>            │
                          └────────────────────┬───────────────────┘
                                               │
                  ┌────────────────────────────┴────────────────────────────┐
                  │ count > 0 (healthy)                                      │ count = 0 (degraded)
                  ▼                                                          ▼
   recipients = SAFE + TEAM (6 people)                          recipients = SAFE only (1 person)
   subject = "Casino Daily Visits Report - <date>"              subject = "[DEGRADED] Casino Daily Visits Report - <date>"
                                                                body includes a warning about missing data
```

## Operations

### First-time setup

```powershell
# 1. Install the SQL function in PostgreSQL
psql -h <host> -U <user> -d Casino.DW -f medallion_pg/sql/32_gold_visit_sessions_view.sql

# 2. Install the Python dependency
.venv/Scripts/python.exe -m pip install -r medallion_pg/orchestration/python/requirements.txt

# 3. Add REPORT_TO_EMAILS_SAFE and REPORT_TO_EMAILS_TEAM to .env (see .env.example)

# 4. Register the scheduled task (admin elevation required, run once)
powershell -ExecutionPolicy Bypass -File scripts\register_visits_report_task.ps1
```

### Manual one-off run

```bash
cd medallion_pg/orchestration/python
python export_visit_sessions.py                             # yesterday, email
python export_visit_sessions.py --no-email                  # yesterday, file only
python export_visit_sessions.py --from-date 2026-04-22 \
                                --to-date   2026-04-22      # specific day
python export_visit_sessions.py --force-degraded            # test the SAFE-only path
```

### Trigger a manual run via Task Scheduler

```powershell
Start-ScheduledTask -TaskName CasinoDW_VisitsReport
Get-Content logs\daily_report.log -Tail 30
```

### Promoting to / demoting from the team distribution list

The team is currently in the TEAM list. To temporarily disable team distribution (e.g., during a known data issue), comment out `REPORT_TO_EMAILS_TEAM` in `.env`. The script will fall back to SAFE only without code changes.

## Folder layout

```
docs/casino_daily_visits_report/
├── README.md         <- this file
├── user_stories.md   <- the team's request and acceptance criteria
├── workflows.md      <- step-by-step daily workflow with timing
├── compare_to_legacy.py  <- validator for legacy SSMS xlsx vs. generated
└── Archive/          <- superseded decisions / replaced versions
```
