# Last Refresh Status

- Last run status: **SUCCESS**
- Last run timestamp (UTC): **2026-02-12 15:51 UTC**
- Target PostgreSQL DB: `Casino.DW`
- Bronze strategy: watermark incremental + sliding reread (`BRONZE_LOOKBACK_DAYS=3`)
- Last Gold refresh window: `2026-02-11` .. `2026-02-11`
- Last error: `None`


## Last known validation snapshot

- `bronze.cashdesk_transactions_raw`: 329,971 rows
- `silver.fact_membership_day` (2026-02-11..2026-02-11): 213 rows
- `gold.fact_membership_day` (2026-02-11..2026-02-11): 213 rows


## Update procedure

This file is auto-updated by `prefect_flow.py` after each flow run.
