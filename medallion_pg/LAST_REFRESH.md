# Last Refresh Status

- Last successful end-to-end execution (Bronze + Gold): **2026-02-12 18:52 UTC**
- Source SQL Server: `192.168.77.13\MSSQLSERVER2017` / `DBASE`
- Target PostgreSQL DB: `Casino.DW`
- Bronze strategy: watermark incremental + sliding reread (`BRONZE_LOOKBACK_DAYS=3`)

## Last known validation snapshot

- `bronze.cashdesk_transactions_raw`: 1,034,718 rows
- `silver.fact_membership_day` (2021-01-01..2026-01-31): 318,256 rows
- `gold.fact_membership_day` (2021-01-01..2026-01-31): 318,256 rows

## Update procedure

After each production refresh, update this file with:
1. timestamp (UTC)
2. source/target confirmation
3. key row counts and window range
