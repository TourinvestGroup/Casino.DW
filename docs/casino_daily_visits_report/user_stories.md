# User Stories — Casino Daily Visits Report

## US-001 — Automate the manual daily visits export
**Date raised:** 2026-04-29
**Status:** DELIVERED 2026-04-29
**Requested by:** Team (forwarded by Nino Motskobili)

### As a
member of the casino analytics team

### I want
to receive yesterday's visit-by-visit gaming activity report automatically by email each morning

### So that
I can review headcount, footfall, and gaming patterns without an analyst running an SSMS query and exporting to Excel by hand

### Acceptance criteria
- [x] Report covers the prior gaming day only (T-1)
- [x] Excel file uses exactly the same 8 columns as the legacy SSMS export: `GamingDay, Membership, VisitNo, CasinoEntryTime, CasinoExitTime, SessionStart, SessionFinish, averBet`
- [x] Empty session columns show literal `"NULL"` (matches legacy format — do not change without team sign-off)
- [x] Date format `mm-dd-yy`; time format `h:mm`; Calibri 11; no header styling
- [x] Excludes `Membership IS NULL` and `Membership = 5` (anonymous/system entry sentinel)
- [x] Pipeline failure does NOT block the report; report failure does NOT fail the pipeline
- [x] Source: PostgreSQL DW (`gold.fn_visit_sessions`), not direct hit on production MSSQL
- [x] Validated against legacy SSMS output (delta explained: 64 extra rows = late-arriving data correctly captured by DW)
- [x] Manual SMTP delivery tested 2026-04-29 — yesterday's report (332 rows) delivered to data@tourinvestgroup.com

### Source query (legacy MSSQL, for reference)
The original query pivots `Person.Visits`, `Manage.PlayerSessions`, and `Casino.PlayersTracking` for `@DaysAgo = 1`, with the same Membership filter. Translated equivalent lives in `gold.fn_visit_sessions`.

### Non-goals (preserved)
- No headcount/footfall summary tab — team wants the raw row-per-(visit × session) only
- No real-time/live data — T-1 is acceptable
- No backfill on demand from this script — use a manual `--from-date / --to-date` invocation if a re-send is needed

---

## US-002 — Promote report to full team distribution
**Date raised:** 2026-04-29
**Status:** DELIVERED 2026-04-29

### As a
team analytics lead

### I want
to fan the report out to the full casino team in addition to data@tourinvestgroup.com

### So that
all stakeholders receive the daily file directly without manual forwarding

### Acceptance criteria
- [x] `REPORT_TO_EMAILS_SAFE` and `REPORT_TO_EMAILS_TEAM` configured in `.env` (split lists)
- [x] Healthy run delivers to all 6 recipients (data@ + 5 team)
- [x] Degraded run (no fresh gold data) drops the team list automatically — only data@ gets `[DEGRADED]`-tagged email
- [x] Recipient resolution verified in dry-run for both modes

### Implementation notes
The "data@ only on break" requirement was implemented via `pipeline_is_healthy()` — a single `SELECT COUNT(*) FROM gold.fact_membership_day WHERE gamingday = ?` decides routing. No manual intervention required when the pipeline breaks.

---

## US-003 — Decouple report timing from data pipeline
**Date raised:** 2026-04-29
**Status:** DELIVERED 2026-04-29

### As a
data ops owner

### I want
the report email to go out at 07:00 (or later), independent of when the pipeline finishes

### So that
- the data has time to settle before the report queries it
- changes to the pipeline schedule don't accidentally shift report-delivery time
- the team's mailbox-arrival expectation is stable

### Acceptance criteria
- [x] Report is no longer triggered by the Prefect flow at 06:00 — runs as separate Windows Task Scheduler entry at 07:00
- [x] `scripts/run_daily_report.bat` and `scripts/register_visits_report_task.ps1` provided
- [x] Documented S4U upgrade path mirrors the daily pipeline's planned migration
- [x] Logs separated to `logs/daily_report.log`

---

## US-004 — Branded From: address for the report
**Date raised:** 2026-04-29
**Status:** DELIVERED 2026-04-29 (pending live SMTP verification)

### As a
team analytics lead

### I want
the daily report email to come **from** `data@tourinvestgroup.com` rather than the generic pipeline service-account address

### So that
recipients see a clear, branded sender and replies route to the data team

### Acceptance criteria
- [x] `REPORT_FROM_EMAIL` env var added; falls back to `SMTP_FROM_EMAIL` if unset
- [x] Pipeline-status email's From: address is unchanged (separate concern)
- [ ] Live verification: tomorrow's 07:00 run delivers with the new From: header without SMTP-server rejection / spam-quarantine
  - If anti-spoofing rejects the new From:, the SMTP relay may need an SPF / DKIM exemption for `data@tourinvestgroup.com`. Operator follow-up required.

---

## Future / not yet planned
- Add a summary tab (daily headcount, total visits, active gamers) — currently a non-goal per US-001
- Migrate scheduled tasks to S4U logon for resilience to user logout — tracked in workflows.md
