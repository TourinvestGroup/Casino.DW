# Workflows — Casino Daily Visits Report

**Last updated:** 2026-04-29

## Daily automated workflow

```
06:00 local (UTC+4)  Windows Task Scheduler fires CasinoDW_DailyRun
        |
        v
   scripts/run_daily_flow.bat
        |
        v
   prefect_flow.py :: daily_casino_dw()
        |
        +-- run_bronze_load           (incremental load, BRONZE_LOOKBACK_DAYS=3)
        +-- run_bronze_egd_load       (CIBatumi -> bronze EGD tables)
        +-- run_gold_refresh          (gold.fact_membership_day, etc.)
        +-- update_refresh_note       (LAST_REFRESH.md)
        +-- send_status_email         (pipeline status -> nino + ninomotskobili)
        +-- run_visits_report  <-- NEW (this initiative)
                |
                v
           export_visit_sessions.py --from-date <yest> --to-date <yest>
                |
                +-- SELECT * FROM gold.fn_visit_sessions(yest, yest)
                +-- write medallion_pg/reports/casino_daily_visits_YYYY-MM-DD.xlsx
                +-- SMTP send -> REPORT_TO_EMAILS (currently: data@tourinvestgroup.com)
```

### Timing assumptions
- Pipeline finishes within ~10–20 min on a normal day → report typically arrives by 06:30 local.
- Catchup mode: if the pipeline backfills multiple days, the report still covers **only yesterday** (the most recent gaming day).

### Failure handling
| Failure point | Effect on pipeline status | Effect on report |
|---|---|---|
| Bronze / Gold load fails | Pipeline marked `failed`, status email goes to team | Report task does not run |
| `gold.fn_visit_sessions` errors | Pipeline still `success`, report task logs warning | No email sent that day |
| SMTP send fails | Pipeline still `success`, report task logs warning | xlsx written locally but not delivered |

By design, the report layer is **decoupled** from pipeline health. Operators monitor the report via Prefect logs, not via the existing pipeline status email.

---

## Manual / on-demand workflow

### Use cases
- Re-send a missed day after an incident
- Validate the script against historical data
- Generate a file without sending email (for team review)

### Steps
```bash
cd medallion_pg/orchestration/python

# Standard: yesterday, with email
python export_visit_sessions.py

# Specific date, with email
python export_visit_sessions.py --from-date 2026-04-22 --to-date 2026-04-22

# Specific date, no email (file only)
python export_visit_sessions.py --from-date 2026-04-22 --to-date 2026-04-22 --no-email

# Multi-day window (rare — produces one file with all days)
python export_visit_sessions.py --from-date 2026-04-20 --to-date 2026-04-22
```

Output lands in `medallion_pg/reports/casino_daily_visits_<TO_DATE>.xlsx`.

---

## Recipient promotion workflow

When ready to switch from testing-only delivery to full team distribution:

1. Confirm at least 3 consecutive days of clean output.
2. Edit `medallion_pg/orchestration/python/.env`:
   ```
   REPORT_TO_EMAILS=data@tourinvestgroup.com,artem.vorotilov@cibatumi.com,nana.khachirova@cibatumi.com,mirian.tavdgiridze@cibatumi.com,tamar.vashadze@cibatumi.com,natela.luashvili@cibatumi.com
   ```
3. Run a manual test:
   ```bash
   python export_visit_sessions.py
   ```
4. Verify all six recipients receive the email.
5. Move the prior `.env` snapshot to `Archive/` with a dated note.

---

## Change log of this workflow

| Date | Change | Why |
|---|---|---|
| 2026-04-29 | Initial automation built | Replaced manual SSMS export the team was running by hand |
