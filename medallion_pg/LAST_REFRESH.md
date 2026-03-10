# Last Refresh Status

- Last run status: **SUCCESS**
- Last run timestamp (UTC): **2026-03-10 06:54 UTC**
- Target PostgreSQL DB: `Casino.DW`
- Bronze strategy: watermark incremental + sliding reread (`BRONZE_LOOKBACK_DAYS=3`)
- Last Gold refresh window: `2026-03-05` .. `2026-03-09`
- Last error: `None`


## Last known validation snapshot

- `bronze.cashdesk_transactions_raw`: 1,050,474 rows
- `bronze.drgt_sessions_raw`: 2,530,518 rows
- `bronze.casino_transaction_money_raw`: 1,718,561 rows
- `silver.fact_membership_day` (2026-03-05..2026-03-09): 566 rows
- `gold.fact_membership_day` (2026-03-05..2026-03-09): 566 rows
- `gold.dim_egd_position`: 170 positions (109 active)


## Update procedure

This file is auto-updated by `prefect_flow.py` after each flow run.
