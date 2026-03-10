# Casino Data Warehouse - PostgreSQL Medallion Architecture

![Pipeline](https://img.shields.io/badge/Pipeline-Prefect%20Daily%2006%3A00%20UTC%2B4-6f42c1)
![Bronze](https://img.shields.io/badge/Bronze-24%20Sources%20|%20Watermark%20%2B%20Sliding%20Window-0a7ea4)
![Gold](https://img.shields.io/badge/Gold-7%20Facts%20|%203%20Dims-e6a817)
![Last Refresh](https://img.shields.io/badge/Last%20Refresh-see%20LAST__REFRESH.md-1f883d)

Current runtime status is tracked in [LAST_REFRESH.md](LAST_REFRESH.md).

---

## Overview

This project implements a full **Medallion Architecture** (Bronze - Silver - Gold) data warehouse for casino operations, built on PostgreSQL. It extracts operational data from two Microsoft SQL Server sources and transforms it into analytics-ready fact and dimension tables.

### Architecture

```
MSSQL (DBASE)          MSSQL (CIBatumi)
192.168.77.13          192.168.77.15,17420
      |                       |
      v                       v
  +---------+           +-----------+
  | Bronze  | <-------- | Bronze    |
  | 24 tbls |           | 2 EGD tbl |
  +---------+           +-----------+
      |                       |
      v                       v
  +---------+           +--------------------+
  | Silver  |           | Gold (EGD dim)     |
  | 2 facts |           | dim_egd_position   |
  +---------+           | bridge_egd_history |
      |                 +--------------------+
      v
  +----------------------------------+
  | Gold                             |
  | fact_membership_day (daily agg)  |
  | fact_session_day_by_table_game   |
  | fact_session_day_by_agent_group  |
  | fact_player_expenses (txn-level) |
  | fact_player_bonuses  (txn-level) |
  | dim_expense_type                 |
  | dim_bonus_indicator              |
  +----------------------------------+
      |
      v
  [ Power BI / SQL queries ]
```

### Key Principles

- **Source SQL Servers are read-only** from this project (`SELECT` only)
- **All writes happen in PostgreSQL** (`Casino.DW` database, localhost:5432)
- **Idempotent loads**: every extraction uses upsert (`ON CONFLICT ... DO UPDATE`)
- **Date-range refresh**: Gold layer uses delete-insert by `gamingday` range
- **Incremental extraction**: watermark-based with configurable sliding-window re-reads for mutable data

---

## Data Sources

### Source 1: DBASE (192.168.77.13) - Main Casino System

| MSSQL Schema.Table | Bronze Target | Rows | Extraction | Description |
|---------------------|---------------|------|------------|-------------|
| `Person.Visits` | `person_visits_raw` | 162K | Watermark on `Created` | Player visit check-in/out |
| `Person.Players` | `person_players_raw` | 194K | Watermark on `Membership` | Player master data (name, agent, country) |
| `Manage.Agents_Players` | `manage_agents_players_raw` | 7K | Watermark on `dateChange` | Agent-player assignment history (SCD) |
| `Manage.Agents` | `manage_agents_raw` | 44 | Watermark on `idAgent` | Agent reference |
| `Manage.Agent_Groups` | `manage_agent_groups_raw` | 350 | Watermark on `Modified` + sliding window | Agent group assignments with date ranges |
| `Casino.Countries` | `casino_countries_raw` | 256 | Watermark on `idCountry` | Country reference |
| `Casino.Games` | `casino_games_ref_raw` | 9 | Full snapshot | Game reference (AR, BJ, 3CP, etc.) |
| `Casino.TableTypesGames` | `casino_table_types_games_raw` | 9 | Full snapshot | Table type to game mapping |
| `Casino.TableTypes` | `casino_table_types_raw` | 2 | Full snapshot | Table type reference |
| `Casino.Tables` | `casino_tables_ref_raw` | 13 | Full snapshot | Physical table reference |
| `Casino.Currency_ExchRates` | `casino_currency_exch_rates_raw` | 8.7K | Watermark on `row_version` | FX rates (SCD) |
| `Casino.Chips` | `casino_chips_raw` | 17 | Full snapshot | Chip denominations |
| `CashDesk.view_Transactions` | `cashdesk_transactions_raw` | 1.05M | Watermark on `idOper` + sliding window | All cashdesk transactions (buy-in, cash-out, transfers, expenses) |
| `CashDesk.Articles` | `cashdesk_articles_raw` | 14 | Full snapshot | Expense article categories |
| `Casino.Transactions_Calculated` | `casino_transactions_calculated_raw` | 1.03M | Watermark on `idOper` | Deposit amounts per transaction |
| `Casino.Transaction_Money` | `casino_transaction_money_raw` | 1.72M | Watermark on `idOper` | Monetary values for expense/agent-credit transactions |
| `Manage.PlayerSessions` | `manage_player_sessions_raw` | 569K | Watermark on `idPlayersTracking` + sliding window | Live game tracking sessions (drop, chips, avg bet) |
| `Manage.PlayerSession_Details` | `manage_player_session_details_raw` | 2.79M | Watermark on `idPlayerSessionDetail` | Session detail line items |
| `Manage.PlayerSession_Detail_Chips` | `manage_player_session_detail_chips_raw` | 2.98M | Watermark on `idPlayerSessionDetail` | Chip counts per session detail |
| `Manage.view_PlayersTracking` | `casino_players_tracking_raw` | 327K | Watermark on `idPlayersTracking` + sliding window | Links tracking sessions to membership + date |
| `DRGT.Sessions` | `drgt_sessions_raw` | 2.53M | Watermark on `ID_SESSION` + sliding window | Slot machine sessions (bets, wins, bill drop) |
| `Promo.PlayerBonuses` | `promo_player_bonuses_raw` | 578K | Watermark on `idPlayerBonus` + sliding window | Player bonus transactions (earned/reversed) |
| `Promo.BonusIndicators` | `promo_bonus_indicators_raw` | 9 | Full snapshot | Bonus indicator dimension (DropLive, WinLive, etc.) |
| `Promo.BonusIndicators_Games` | `promo_bonus_indicators_games_raw` | 18 | Full snapshot | House edge rates per game (SCD by dateChange) |

### Source 2: CIBatumi (192.168.77.15,17420) - EGD Slot Monitoring

| MSSQL Table | Bronze Target | Rows | Extraction | Description |
|-------------|---------------|------|------------|-------------|
| `dbo.SM_EgdCfg` | `sm_egd_cfg_raw` | 4.9K | Full snapshot | Slot machine config revisions (manufacturer, model, serial) |
| `dbo.SM_MeterDayV6` + `SM_PlayerSessionV7` | `sm_egd_activity_raw` | 162 | Aggregated snapshot | Activity summary per position (first/last seen, session counts) |

---

## Database Schemas

### `dw_control` - ETL Metadata

| Table | Purpose |
|-------|---------|
| `etl_watermark` | Tracks the last-loaded watermark value per source table |
| `etl_run_log` | Audit log of every ETL run (timestamps, row counts, errors) |

### `bronze` - Raw Replicated Data (26 tables)

Raw copies of MSSQL source tables. Every table has:
- `_source_system` - origin identifier (`'mssql'` or `'cibatumi'`)
- `_loaded_at_utc` - load timestamp

All loads use upsert (`ON CONFLICT ... DO UPDATE SET`) so re-runs are safe.

### `silver` - Cleansed Transformation Layer

| Object | Type | Description |
|--------|------|-------------|
| `silver.fn_membership_day()` | Function | Core transformation: produces one row per (gamingday, membership) with 41 columns covering live game financials, slot metrics, expenses, agent credits, discounts, tracking float, and player metadata. Uses CTEs for clean drop calculation, agent history lookup, slot aggregation, expense aggregation, and session metrics. |
| `silver.fact_membership_day` | Table | Materialized output of `fn_membership_day()`, used as source for gold layer |
| `silver.sp_load_fact_membership_day()` | SP | Delete-insert loader for the silver fact table |
| `silver.fact_player_session` | Table | One row per live game tracking session with game, table, agent group, drop, chips, avg bet, and duration |
| `silver.sp_load_reference_and_facts()` | SP | Loads reference dimensions (FX rates, games, tables, agents) and the session fact |
| `silver.dim_fx_rate_daily` | Table | Daily FX rate dimension |
| `silver.dim_game` | Table | Game dimension |
| `silver.dim_table` | Table | Table dimension (with table type and game joins) |
| `silver.dim_agent_group` | Table | Agent group dimension with date ranges |

### `gold` - Business Output Layer

#### Fact Tables

| Table | Grain | Rows | Description |
|-------|-------|------|-------------|
| `gold.fact_membership_day` | gamingday + membership | 248K | Daily player summary: live game drop/cash/sessions, slot metrics, expenses, agent credits, discounts, tracking float, player name + agent + country |
| `gold.fact_session_day_by_table_game` | gamingday + table + game | 29.5K | Daily aggregates per physical table and game: session count, member count, drop, hold, chips, avg bet |
| `gold.fact_session_day_by_agent_group` | gamingday + agent group | 2.3K | Daily aggregates per agent group: session count, member count, drop, hold, chips |
| `gold.fact_player_expenses` | idoper + expense_type_key | 24K | Transaction-level expense detail: amount, article, comment, player + agent context. Covers accounts 641 (operating), 153 (agent credit), 151 (LG discount), 154 (slot discount) |
| `gold.fact_player_bonuses` | idplayerbonus | 578K | Transaction-level bonus detail: earned amount, game, house edge %, hours played, hands/hour, avg bet, ADT (calculated), comment with formula, player + agent context |

#### Dimension Tables

| Table | Rows | Description |
|-------|------|-------------|
| `gold.dim_expense_type` | 8 | Expense category dimension: Air Tickets, Discount+, Hotel, Other (account 641), Agent Out/Void (153), LG Discount (151), Slot Discount (154) |
| `gold.dim_bonus_indicator` | 9 | Bonus indicator dimension: DropLive, WinLive, LossLive, DropSlot, WinSlot, LossSlot, WinTotal, LossTotal, SlotIn |
| `gold.dim_egd_position` | 170 | Slot machine floor position dimension: IP address, manufacturer, model, game, serial, active status, lifecycle dates |
| `gold.bridge_egd_machine_history` | 5.0K | Machine assignment history bridge: which MAC address was at which floor position, with date ranges |

#### Functions

| Function | Description |
|----------|-------------|
| `gold.fn_membership_period(from, to, agent_id)` | Aggregates `fact_membership_day` across a date range, returning one row per membership with period totals for all metrics |
| `gold.sp_load_fact_membership_day(from, to, agent_id)` | Calls silver SP then loads gold fact. Supports optional agent filter |
| `gold.sp_load_fact_player_expenses(from, to)` | Loads expense fact from bronze transactions + transaction_money, classifying by account and comment patterns |
| `gold.sp_load_fact_player_bonuses(from, to)` | Loads bonus fact from bronze promo tables, calculates ADT, resolves game names and agent context |
| `gold.sp_load_session_marts(from, to)` | Loads silver references/sessions, then both session day marts |
| `gold.sp_load_dim_egd_position()` | Full refresh of EGD dimension and machine history bridge from bronze EGD tables |

---

## `gold.fact_membership_day` - Column Reference

This is the primary daily player-level fact table with 42 columns:

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `gamingday` | date | Person.Visits / DRGT.Sessions | Gaming day (PK part 1) |
| `membership` | bigint | Person.Visits / DRGT.Sessions | Player membership ID (PK part 2) |
| `surname` | text | Person.Players | Player last name |
| `forename` | text | Person.Players | Player first name |
| `idagent` | int | Agents_Players (SCD) / Players | Agent ID at time of visit |
| `agentname` | text | Manage.Agents | Agent name |
| `citizenshipcountry` | text | Casino.Countries | Player citizenship country |
| `totaldrop_clean` | numeric | CashDesk transactions | Net clean drop (buy-ins minus recycled funds) |
| `systemdrop_in` | numeric | CashDesk transactions | System-calculated drop (chips bought in) |
| `totalcash_in` | numeric | CashDesk transactions | Total cash exchanged in (cashdesk + table cash) |
| `totalcash_result` | numeric | CashDesk transactions | Net cash result (in minus out) |
| `cashdesk_in` | numeric | CashDesk acct 121 | Cash desk buy-in |
| `cashdesk_out` | numeric | CashDesk acct 121 | Cash desk cash-out |
| `tablecash_in` | numeric | CashDesk acct 703 | Table cash buy-in |
| `agenttransfer_net` | numeric | CashDesk acct 802 | Net agent transfer |
| `junketdeposit_add` | numeric | CashDesk acct 803 | Junket deposit added |
| `junketdeposit_withdraw` | numeric | CashDesk acct 803 | Junket deposit withdrawn |
| `junketdeposit_net` | numeric | CashDesk acct 803 | Net junket deposit |
| `sessionscnt` | bigint | Manage.PlayerSessions | Live game session count |
| `minutes_played` | numeric | Manage.PlayerSessions | Total live game minutes played |
| `slot_totalbet` | numeric | DRGT.Sessions | Total slot bets |
| `slot_cashbet` | numeric | DRGT.Sessions | Cash slot bets (excluding promo) |
| `slot_totalout` | numeric | DRGT.Sessions | Total slot payouts |
| `slot_win` | numeric | DRGT.Sessions | Slot win (house perspective) |
| `slot_nwl` | numeric | DRGT.Sessions | Net win/loss |
| `slot_billdrop` | numeric | DRGT.Sessions | Cash inserted into slot machines |
| `slot_gamesplayed` | numeric | DRGT.Sessions | Number of slot games played |
| `slot_sessions_cnt` | bigint | DRGT.Sessions | Number of slot sessions |
| `tracking_floatin` | numeric | PlayerSession_Detail_Chips | Chip float in (value of chips given to player) |
| `tracking_floatout` | numeric | PlayerSession_Detail_Chips | Chip float out (value of chips returned) |
| `tracking_net` | numeric | PlayerSession_Detail_Chips | Net tracking float (in - out) |
| `expense_total` | numeric | CashDesk acct 641 + Transaction_Money | Total operating expenses (air tickets, hotel, discount, other) |
| `expense_airtickets` | numeric | CashDesk acct 641 (comment match) | Air ticket expenses |
| `expense_discount_plus` | numeric | CashDesk acct 641 (comment match) | Discount+ expenses |
| `expense_hotel` | numeric | CashDesk acct 641 (comment match) | Hotel/accommodation expenses |
| `expense_other` | numeric | CashDesk acct 641 (remainder) | Other operating expenses |
| `discount_lg` | numeric | CashDesk acct 151 + Transaction_Money | Live game discount |
| `discount_slot` | numeric | CashDesk acct 154 + Transaction_Money | Slot discount |
| `agent_credit_out` | numeric | CashDesk acct 153 dir=1 + Transaction_Money | Agent credit given |
| `agent_credit_void` | numeric | CashDesk acct 153 dir=-1 + Transaction_Money | Agent credit returned |
| `agent_credit_net` | numeric | CashDesk acct 153 + Transaction_Money | Net agent credit |

---

## `gold.fact_player_bonuses` - Column Reference

Transaction-level bonus data with ADT calculation:

| Column | Type | Source | Description |
|--------|------|--------|-------------|
| `idplayerbonus` | int | Promo.PlayerBonuses | Bonus transaction PK |
| `gamingday` | date | PlayerBonuses.dateWork | Gaming day |
| `timeoper` | timestamp | PlayerBonuses.timeOper | Transaction timestamp |
| `membership` | bigint | PlayerBonuses.Membership | Player membership ID |
| `idagent` | int | Agents_Players (SCD) / Players | Agent ID at time of bonus |
| `agentname` | text | Manage.Agents | Agent name |
| `citizenshipcountry` | text | Casino.Countries | Player citizenship |
| `typeoper` | int | PlayerBonuses.typeOper | 1 = earned, -1 = reversed |
| `idbonusindicator` | int | PlayerBonuses.idBonusIndicator | FK to dim_bonus_indicator |
| `idgame` | int | PlayerBonuses.idGame | Game ID |
| `gamename` | text | Casino.Games | Game name (BlackJack, American Roulette, etc.) |
| `sumbonuses` | numeric | PlayerBonuses.sumBonuses | Earned bonus amount |
| `costbonuses` | numeric | PlayerBonuses.costBonuses | Cost of bonuses |
| `multiplierloyalty` | numeric | PlayerBonuses.multiplierLoyalty | House edge % (e.g., 0.0266 for AR, 0.0100 for BJ) |
| `hours` | numeric | PlayerBonuses.hours | Hours played |
| `handsperhour` | int | PlayerBonuses.handsPerHour | Hands per hour (e.g., 30 for AR, 60 for BJ) |
| `averbet` | numeric | PlayerBonuses.averBet | Average bet |
| `percentadt` | numeric | PlayerBonuses.percentADT | ADT percent (typically 0.20 = 20%) |
| `adt` | numeric | Calculated | Average Daily Theoretical = `multiplierloyalty * hours * handsperhour * averbet` |
| `comment` | text | PlayerBonuses.Comment | Formula comment (e.g., `AR 0.0266 * 0.62 * 30 * 87.9 * 0.20`) |
| `isdeleted` | boolean | PlayerBonuses.isDeleted | Soft delete flag |

**ADT Formula**: `ADT = HouseEdge% x Hours x HandsPerHour x AvgBet`

**Earned Bonus Formula**: `Earned = ADT x percentADT(20%)`

---

## `gold.fact_player_expenses` - Column Reference

Transaction-level expense detail:

| Column | Type | Description |
|--------|------|-------------|
| `idoper` | bigint | CashDesk transaction PK |
| `expense_type_key` | int | FK to dim_expense_type |
| `gamingday` | date | Gaming day |
| `membership` | bigint | Player membership ID |
| `idagent` | int | Agent ID at time of transaction |
| `agentname` | text | Agent name |
| `citizenshipcountry` | text | Player citizenship |
| `amount` | numeric | Expense amount (from Casino.Transaction_Money.sumMoney) |
| `comment` | text | Transaction comment |
| `idarticle` | int | Article ID |
| `article_name` | text | Article name from CashDesk.Articles |

**Expense classification** (account 641) uses comment pattern matching to categorize into Air Tickets, Discount+, Hotel, or Other.

---

## SQL Setup Order

Run scripts in this order on PostgreSQL `Casino.DW`:

| # | Script | Description |
|---|--------|-------------|
| 1 | `sql/00_create_schemas.sql` | Creates `dw_control`, `bronze`, `silver`, `gold` schemas |
| 2 | `sql/01_control_tables.sql` | Creates `etl_watermark` and `etl_run_log` tables, seeds initial watermarks |
| 3 | `sql/10_bronze_tables.sql` | Creates all 24 bronze tables from DBASE with indexes |
| 4 | `sql/11_bronze_egd_tables.sql` | Creates 2 bronze EGD tables from CIBatumi |
| 5 | `sql/20_silver_models.sql` | Creates silver functions, SPs, and fact/dimension tables |
| 6 | `sql/30_gold_models.sql` | Creates all gold facts, dimensions, SPs, and the period aggregation function |
| 7 | `sql/31_gold_egd_dimension.sql` | Creates EGD dimension, machine history bridge, and refresh SP |

Optional:
- `sql/40_full_refresh_example.sql` - Example full historical refresh
- `sql/50_bronze_archive_purge.sql` - Archive/purge old bronze data procedure

---

## Python Scripts

All scripts are in `orchestration/python/`. Use the venv at `.venv/` or install dependencies from `requirements.txt`.

| Script | Description |
|--------|-------------|
| `load_bronze_incremental.py` | Extracts 24 source tables from DBASE (MSSQL) into bronze (PostgreSQL) using watermark-based incremental loading with optional sliding window re-reads |
| `load_egd_dimension.py` | Extracts EGD machine data from CIBatumi (MSSQL) and refreshes `dim_egd_position` + `bridge_egd_machine_history` |
| `refresh_gold.py` | Calls all gold stored procedures in sequence: `sp_load_fact_membership_day`, `sp_load_fact_player_expenses`, `sp_load_fact_player_bonuses`, `sp_load_session_marts`, `sp_load_dim_egd_position` |
| `run_pipeline.py` | Runs bronze load (DBASE) + bronze EGD load (CIBatumi) + gold refresh in one command |
| `prefect_flow.py` | Prefect orchestration flow with daily scheduling, auto-catchup for missed days, LAST_REFRESH.md updates, and email notifications |

---

## Running the Pipeline

### Prerequisites

```bash
cd orchestration/python
pip install -r requirements.txt
```

Required environment variables (in `.env` file):

```bash
# SQL Server ODBC connection (DBASE - main casino system)
MSSQL_CONN_STR=DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.77.13;DATABASE=DBASE;UID=...;PWD=...;TrustServerCertificate=yes;

# SQL Server ODBC connection (CIBatumi - EGD slot monitoring)
MSSQL_CIBATUMI_CONN_STR=DRIVER={ODBC Driver 17 for SQL Server};SERVER=192.168.77.15,17420;DATABASE=CIBatumi;UID=...;PWD=...;TrustServerCertificate=yes;

# PostgreSQL target
PG_CONN_STR=host=localhost port=5432 dbname=Casino.DW user=postgres password=...

# Bronze sliding reread window (days)
BRONZE_LOOKBACK_DAYS=3
```

### Bronze load only

```bash
python load_bronze_incremental.py
```

### EGD dimension load only

```bash
python load_egd_dimension.py
```

### Gold refresh only

```bash
python refresh_gold.py --from-date 2026-01-01 --to-date 2026-01-31
python refresh_gold.py --from-date 2026-01-01 --to-date 2026-01-31 --agent-id 123
python refresh_gold.py --from-date 2026-01-01 --to-date 2026-01-31 --skip-egd
```

### Full pipeline (Bronze + Gold)

```bash
python run_pipeline.py --from-date 2026-01-01 --to-date 2026-01-31
```

---

## Main Output Queries

### Player summary for a period

```sql
SELECT *
FROM gold.fn_membership_period(
  p_from_date => DATE '2026-01-01',
  p_to_date   => DATE '2026-01-31',
  p_agent_id  => NULL
)
ORDER BY totaldrop_clean DESC;
```

### Daily player detail

```sql
SELECT *
FROM gold.fact_membership_day
WHERE gamingday BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
ORDER BY gamingday, membership;
```

### Session day by table and game

```sql
SELECT *
FROM gold.fact_session_day_by_table_game
WHERE gamingday BETWEEN DATE '2026-01-01' AND DATE '2026-01-31';
```

### Session day by agent group

```sql
SELECT *
FROM gold.fact_session_day_by_agent_group
WHERE gamingday BETWEEN DATE '2026-01-01' AND DATE '2026-01-31';
```

### Player expenses detail

```sql
SELECT e.*, d.expense_group, d.expense_type
FROM gold.fact_player_expenses e
JOIN gold.dim_expense_type d ON d.expense_type_key = e.expense_type_key
WHERE e.gamingday BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
ORDER BY e.gamingday, e.membership;
```

### Player bonuses with ADT

```sql
SELECT
    gamingday, membership, gamename, typeoper,
    sumbonuses AS earned,
    multiplierloyalty AS house_edge_pct,
    hours, handsperhour, averbet,
    adt AS average_daily_theoretical,
    percentadt, comment
FROM gold.fact_player_bonuses
WHERE gamingday BETWEEN DATE '2026-01-01' AND DATE '2026-01-31'
  AND typeoper = 1  -- earned only
ORDER BY gamingday, membership;
```

### Bonus summary by player and month

```sql
SELECT
    membership,
    DATE_TRUNC('month', gamingday)::date AS month,
    COUNT(*) AS bonus_entries,
    SUM(CASE WHEN typeoper = 1 THEN sumbonuses ELSE 0 END) AS total_earned,
    SUM(CASE WHEN typeoper = -1 THEN sumbonuses ELSE 0 END) AS total_reversed,
    SUM(sumbonuses) AS net_bonus,
    AVG(CASE WHEN typeoper = 1 AND adt IS NOT NULL THEN adt END) AS avg_adt
FROM gold.fact_player_bonuses
WHERE gamingday >= DATE '2026-01-01'
GROUP BY membership, DATE_TRUNC('month', gamingday)
ORDER BY month, total_earned DESC;
```

### EGD slot machine positions

```sql
SELECT *
FROM gold.dim_egd_position
WHERE is_active = true
ORDER BY floor_zone, position_in_zone;
```

---

## Prefect Orchestration (Daily Scheduling)

### Install

```bash
pip install -r requirements-prefect.txt
```

### Deploy

```bash
prefect server start
prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api

# Deploy at 06:00 daily (Georgia time)
prefect deploy prefect_flow.py:daily_casino_dw --name daily-casino-dw \
  --cron "0 6 * * *" --timezone "Asia/Tbilisi"

prefect worker start --pool default-agent-pool
```

Or run directly with auto-scheduling:

```bash
python prefect_flow.py --serve --cron "0 6 * * *"
```

### Flow behavior

1. **Bronze load** - Extracts all 24 DBASE sources with watermark + sliding window
2. **Bronze EGD load** - Extracts EGD data from CIBatumi (if `MSSQL_CIBATUMI_CONN_STR` is set)
3. **Gold refresh** - Calls all gold SPs for the target date range
4. **Status update** - Updates `LAST_REFRESH.md` with run result
5. **Email notification** - Sends success/failure email (if SMTP is configured)

### Auto-catchup

The flow automatically detects missed days by comparing the latest `gamingday` in `gold.fact_membership_day` with yesterday. If gaps exist, it backfills from the last known day through yesterday.

### Email notifications

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

For SSL on port 465 (e.g., Titan): set `SMTP_PORT=465`, `SMTP_USE_TLS=false`, `SMTP_USE_SSL=true`.

---

## Archive / Purge Old Bronze Data

```sql
-- Archive old bronze rows to archive.* schema, then delete from bronze
SELECT * FROM dw_control.sp_bronze_archive_or_purge(DATE '2025-01-01', 'archive_delete');

-- Delete only (no archive copy)
SELECT * FROM dw_control.sp_bronze_archive_or_purge(DATE '2025-01-01', 'delete_only');
```

---

## Current Data Volume (as of 2026-03-10)

| Layer | Table | Rows |
|-------|-------|------|
| Bronze | `cashdesk_transactions_raw` | 1,050,474 |
| Bronze | `casino_transaction_money_raw` | 1,718,561 |
| Bronze | `drgt_sessions_raw` | 2,530,518 |
| Bronze | `manage_player_sessions_raw` | 570,282 |
| Bronze | `manage_player_session_details_raw` | 2,795,724 |
| Bronze | `manage_player_session_detail_chips_raw` | 2,988,924 |
| Bronze | `promo_player_bonuses_raw` | 578,647 |
| Bronze | `person_players_raw` | 194,233 |
| Bronze | `person_visits_raw` | 165,170 |
| Bronze | `casino_players_tracking_raw` | 327,475 |
| Silver | `fact_membership_day` | 248,033 |
| Silver | `fact_player_session` | 481,596 |
| Gold | `fact_membership_day` | 248,033 |
| Gold | `fact_player_bonuses` | 578,403 |
| Gold | `fact_session_day_by_table_game` | 29,538 |
| Gold | `fact_player_expenses` | 24,087 |
| Gold | `fact_session_day_by_agent_group` | 2,285 |
| Gold | `dim_egd_position` | 170 |
| Gold | `bridge_egd_machine_history` | 4,976 |
| Gold | `dim_bonus_indicator` | 9 |
| Gold | `dim_expense_type` | 8 |
| **Total Bronze** | **26 tables** | **~14.6M rows** |
| **Total Gold** | **7 facts + 3 dims** | **~888K rows** |
