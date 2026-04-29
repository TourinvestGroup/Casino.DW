# User Stories — Casino Daily Visits Report

## US-001 — Automate the manual daily visits export
**Date raised:** 2026-04-29
**Requested by:** Team (forwarded by Nino Motskobili)
**Recipient list (testing):** data@tourinvestgroup.com
**Recipient list (future):** artem.vorotilov@cibatumi.com, nana.khachirova@cibatumi.com, mirian.tavdgiridze@cibatumi.com, tamar.vashadze@cibatumi.com, natela.luashvili@cibatumi.com

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
- [ ] Validated against legacy SSMS output for one full gaming day before team rollout
- [ ] Team distribution enabled (currently testing-only with data@tourinvestgroup.com)

### Source query (legacy MSSQL, for reference)
The original query pivots `Person.Visits`, `Manage.PlayerSessions`, and `Casino.PlayersTracking` for `@DaysAgo = 1`, with the same Membership filter. Translated equivalent lives in `gold.fn_visit_sessions`.

### Non-goals
- No headcount/footfall summary tab — team wants the raw row-per-(visit × session) only
- No real-time/live data — T-1 is acceptable
- No backfill on demand from this script — use a manual `--from-date / --to-date` invocation if a re-send is needed

---

## US-002 — Promote report to full team distribution after validation
**Status:** pending team validation of US-001 output

### As a
team analytics lead

### I want
to switch the recipient from `data@tourinvestgroup.com` to the full team distribution

### So that
all stakeholders receive the daily file directly without manual forwarding

### Acceptance criteria
- [ ] Sample report validated against legacy SSMS output for at least 3 consecutive days
- [ ] No NULL-vs-empty-cell complaints from team
- [ ] `REPORT_TO_EMAILS` updated in `.env` to include the five team addresses
- [ ] One manual test run delivers successfully to all five recipients
