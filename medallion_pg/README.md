# PostgreSQL Medallion DW (Scenario 2.5)

![Pipeline](https://img.shields.io/badge/Pipeline-Prefect%20Daily%2009%3A00-6f42c1)
![Bronze](https://img.shields.io/badge/Bronze-Watermark%20%2B%20Sliding%20Window-0a7ea4)
![Last Refresh](https://img.shields.io/badge/Last%20Refresh-2026--02--12%2018%3A52%20UTC-1f883d)

Current runtime status is tracked in [LAST_REFRESH.md](LAST_REFRESH.md).

This project implements Medallion architecture on PostgreSQL localhost:
- Bronze: raw replicated SQL Server tables
- Silver: cleansed, permanent transformation layer
- Gold: business output layer

Important:
- Source SQL Server is read-only from this project (`SELECT` only).
- All writes happen in PostgreSQL (`Casino.DW`).

## 1) SQL setup order

Run scripts in this order:

1. `sql/00_create_schemas.sql`
2. `sql/01_control_tables.sql`
3. `sql/10_bronze_tables.sql`
4. `sql/20_silver_models.sql`
5. `sql/30_gold_models.sql`

Optional:
- `sql/40_full_refresh_example.sql`
- `sql/50_bronze_archive_purge.sql`

## 2) Main output query

```sql
SELECT *
FROM gold.fn_membership_period(
  p_from_date => DATE '2026-01-01',
  p_to_date   => DATE '2026-01-31',
  p_agent_id  => NULL
)
ORDER BY totaldrop_clean DESC;
```

## 3) Daily Bronze feed (Option 2)

Implemented strategy:
- watermark incremental
- plus sliding re-read window for mutable tables

Current sliding window coverage:
- `CashDesk.view_Transactions` by `dateWork`
- `Casino.PlayersTracking` by `dateWork`

Config:
- `BRONZE_LOOKBACK_DAYS` (default `3`)

Run Bronze only:

```bash
cd orchestration/python
py -3.12 -m pip install -r requirements.txt
set BRONZE_LOOKBACK_DAYS=3
py -3.12 load_bronze_incremental.py
```

## 4) Full pipeline commands

Gold refresh only:

```bash
py -3.12 refresh_gold.py --from-date 2026-01-01 --to-date 2026-01-31
```

Bronze + Gold in one command:

```bash
py -3.12 run_pipeline.py --from-date 2026-01-01 --to-date 2026-01-31
```

## 5) Prefect orchestration (daily 09:00)

Files:
- `orchestration/python/prefect_flow.py`
- `orchestration/python/requirements-prefect.txt`

Install:

```bash
cd orchestration/python
py -3.12 -m pip install -r requirements-prefect.txt
```

Required environment variables:
- `MSSQL_CONN_STR`
- `PG_CONN_STR`
- optional `BRONZE_LOOKBACK_DAYS`

Flow behavior:
- Bronze load first
- Gold refresh second
- default Gold window = previous calendar day (weekends included)
- updates `LAST_REFRESH.md` after every run (success or failure)
- sends run status email (success/failure) when SMTP variables are configured

Deploy at 09:00 every day:

```bash
prefect server start
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
prefect deploy prefect_flow.py:daily_casino_dw --name daily-casino-dw --cron "0 9 * * *"
prefect worker start --pool default-agent-pool
```

Timezone example:

```bash
prefect deploy prefect_flow.py:daily_casino_dw --name daily-casino-dw --cron "0 9 * * *" --timezone "Asia/Tbilisi"
```

Optional rolling window mode:

```bash
prefect deploy prefect_flow.py:daily_casino_dw --name daily-casino-dw --cron "0 9 * * *" --param use_previous_day_window=false --param gold_refresh_lookback_days=7
```

Email notification variables:

```bash
set SMTP_HOST=smtp.your-domain.com
set SMTP_PORT=587
set SMTP_USERNAME=your_user
set SMTP_PASSWORD=your_password
set SMTP_USE_TLS=true
set SMTP_USE_SSL=false
set SMTP_FROM_EMAIL=dw-bot@your-domain.com
set SMTP_TO_EMAILS=you@your-domain.com,team@your-domain.com
```

For providers using SSL on port 465 (for example Titan), use:

```bash
set SMTP_PORT=465
set SMTP_USE_TLS=false
set SMTP_USE_SSL=true
```

## 6) Archive / purge old Bronze data

Use `sql/50_bronze_archive_purge.sql`.

Procedure:
- `dw_control.sp_bronze_archive_or_purge(p_cutoff_date, p_mode)`

Modes:
- `archive_delete` (copy old rows to `archive.*` then delete from `bronze.*`)
- `delete_only` (delete from Bronze without archive)

Example:

```sql
SELECT *
FROM dw_control.sp_bronze_archive_or_purge(DATE '2025-01-01', 'archive_delete');
```