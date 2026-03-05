# Last Refresh Status

- Last run status: **SUCCESS**
- Last run timestamp (UTC): **2026-03-05 (manual)**
- Target PostgreSQL DB: `Casino.DW`
- Bronze strategy: watermark incremental + sliding reread (`BRONZE_LOOKBACK_DAYS=3`)
- Last Gold refresh window: `2021-01-01` .. `2026-03-05`
- Last error: `None`


## Last known validation snapshot

- `bronze.cashdesk_transactions_raw`: 1,048,873 rows
- `bronze.drgt_sessions_raw`: 2,527,834 rows
- `bronze.casino_transaction_money_raw`: 1,716,066 rows
- `bronze.promo_player_bonuses_raw`: 577,646 rows
- `silver.fact_membership_day`: 247,499 rows (full backfill)
- `gold.fact_membership_day`: 247,499 rows (full backfill)
- `gold.fact_player_expenses`: 24,022 rows (full backfill)
- `gold.fact_player_bonuses`: 577,406 rows (full backfill)
- `gold.dim_egd_position`: 164 positions (active + inactive)
- `gold.dim_bonus_indicator`: 9 indicators
- `gold.dim_expense_type`: 8 types


## Update procedure

This file is auto-updated by `prefect_flow.py` after each flow run.
