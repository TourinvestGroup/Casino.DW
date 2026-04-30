# Workflows — Casino Daily Visits Report

**Last updated:** 2026-04-29

## Daily automated workflow

The data pipeline and the report are now **separate scheduled tasks** with a 1-hour buffer:

```
┌─────────────────────────────────────────────────────────────────────────┐
│  06:00 local (UTC+4)                                                     │
│    Windows Task Scheduler fires  CasinoDW_DailyRun                       │
│      └── scripts/run_daily_flow.bat                                      │
│            └── prefect_flow.py :: daily_casino_dw()                      │
│                  ├── run_bronze_load                                     │
│                  ├── run_bronze_egd_load                                 │
│                  ├── run_gold_refresh                                    │
│                  ├── update_refresh_note                                 │
│                  └── send_status_email  → nino + ninomotskobili          │
│                                                                          │
│  Pipeline finishes ~06:10–06:25                                          │
│  ▼                                                                       │
│  07:00 local (UTC+4)                                                     │
│    Windows Task Scheduler fires  CasinoDW_VisitsReport                   │
│      └── scripts/run_daily_report.bat                                    │
│            └── export_visit_sessions.py                                  │
│                  ├── SELECT * FROM gold.fn_visit_sessions(yest, yest)    │
│                  ├── pipeline_is_healthy(yest) ?                         │
│                  │     ├── True  → recipients = SAFE + TEAM (6)          │
│                  │     └── False → recipients = SAFE (1) + [DEGRADED]    │
│                  ├── write medallion_pg/reports/...xlsx                  │
│                  └── SMTP send  → REPORT_FROM_EMAIL                      │
└─────────────────────────────────────────────────────────────────────────┘
```

### Timing assumptions
- 06:00 trigger → pipeline finishes ~06:10–06:25 on a normal day
- 07:00 buffer ensures gold tables are fully written before the report queries them
- If the pipeline runs unusually long (catchup mode after a missed day), the 07:00 report still runs — health-check decides routing
- Report covers **only yesterday**, even if the pipeline backfilled multiple days

### Failure handling matrix

| Failure point | Effect on pipeline | Effect on report |
|---|---|---|
| Bronze / Gold load fails | Pipeline marked `failed`, status email goes to nino + ninomotskobili | At 07:00 the report runs, sees no gold data for yesterday, flips to **degraded** mode → emails SAFE only with `[DEGRADED]` tag |
| Gold load succeeds but visits/sessions tables empty for that day | Pipeline `success` | Report is healthy, sends to all 6 with row count = 0 (likely a real low-activity day, e.g. closure) |
| `gold.fn_visit_sessions` errors (rare) | Pipeline `success` | Report fails; entry in `logs/daily_report.log`. No email sent. Manual re-run available. |
| SMTP server unreachable | Pipeline `success` | xlsx is written locally, no email delivered. Operator notices via missing email. |
| PC logged out at 06:00 / 07:00 | Neither task runs (LogonType Interactive) | Once user logs in, run manually via Start-ScheduledTask |

The two tasks are fully independent — neither failure mode propagates to the other.

---

## Manual / on-demand workflow

### Use cases
- Re-send a missed day after an incident
- Validate the script against historical data
- Generate a file without sending email (for review)
- Test the degraded-mode safety path

### Steps
```bash
cd medallion_pg/orchestration/python

# Standard: yesterday, full healthy delivery
python export_visit_sessions.py

# Specific date, with email
python export_visit_sessions.py --from-date 2026-04-22 --to-date 2026-04-22

# Specific date, no email (file only — for spot-checks)
python export_visit_sessions.py --from-date 2026-04-22 --to-date 2026-04-22 --no-email

# Force degraded mode — sends to SAFE only with [DEGRADED] tag
python export_visit_sessions.py --force-degraded

# Multi-day window (for backfill resends)
python export_visit_sessions.py --from-date 2026-04-20 --to-date 2026-04-22
```

Output lands in `medallion_pg/reports/casino_daily_visits_<TO_DATE>.xlsx`.

### Trigger via Task Scheduler

```powershell
Start-ScheduledTask -TaskName CasinoDW_VisitsReport
Get-Content logs\daily_report.log -Tail 30
```

---

## Recipient management workflow

### Adding / removing team members
Edit `REPORT_TO_EMAILS_TEAM` in `medallion_pg/orchestration/python/.env`. Comma-separated. No code change needed.

### Temporarily silencing the team (e.g., during a known data issue)
Comment out `REPORT_TO_EMAILS_TEAM` in `.env`. The script will fall back to SAFE only. Re-enable when issue resolves.

### Changing the From: address
Update `REPORT_FROM_EMAIL` in `.env`. If the new sender is on a domain not authorized by the global SMTP relay, also configure the `REPORT_SMTP_*` profile (see "SMTP credential rotation" below) — otherwise the relay will reject the send with `5.7.1 Sender address rejected`.

### SMTP credential rotation (REPORT_SMTP_*)
The report can use its own mail server, separate from the pipeline-status relay:

```ini
REPORT_SMTP_HOST=smtp.office365.com
REPORT_SMTP_PORT=587
REPORT_SMTP_USERNAME=data@tourinvestgroup.com
REPORT_SMTP_PASSWORD=<App Password>
REPORT_SMTP_USE_TLS=true
```

When credentials rotate, edit `.env` and re-run a manual test (`Start-ScheduledTask -TaskName CasinoDW_VisitsReport`). On rollback, comment out all `REPORT_SMTP_*` lines and the script falls back to the global `SMTP_*` profile automatically.

### Rotating recipients
Snapshot the current `.env` recipient lines into `Archive/<dated>/` first, then edit. The Archive convention preserves the rollout history.

---

## Migration to S4U logon (planned)

Currently both `CasinoDW_DailyRun` and `CasinoDW_VisitsReport` are `LogonType Interactive` — they only run while a user is logged in (locked PC is fine; logged-out PC is not). When the planned `scripts/_register_s4u.ps1` migration runs:

1. The daily-pipeline task moves to S4U
2. Mirror the change in `scripts/register_visits_report_task.ps1` (one-line change: `-LogonType S4U`)
3. Re-run the registration script as admin to apply

After S4U: both tasks run regardless of user-login state.

---

## Change log

| Date | Change | Why |
|---|---|---|
| 2026-04-29 | Initial automation built (in-flow report task) | Replaced manual SSMS export the team was running by hand |
| 2026-04-29 | **Decoupled to separate Task Scheduler entry at 07:00**; added SAFE/TEAM recipient routing with health-check fallback; added REPORT_FROM_EMAIL override | Buy 1h buffer between data SLA and report SLA; team gets full distribution on healthy days, only data@ on broken days |
| 2026-04-30 | Polished email content (warm + brief tone, "Tourinvest Data Team" sign-off); replaced [DEGRADED] subject tag with softer ⚠ marker + plain-language warning; added REPORT_SMTP_* override for separate mail server | Email goes to business stakeholders (not just ops) — needed friendlier wording. Separate SMTP profile lets From: be data@ without the global relay rejecting it for cross-domain spoofing. |
