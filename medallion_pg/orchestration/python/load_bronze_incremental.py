import os
from datetime import datetime, timedelta, timezone
from typing import Any

import pyodbc
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

MSSQL_CONN_STR = os.getenv("MSSQL_CONN_STR")
PG_CONN_STR = os.getenv("PG_CONN_STR")
DEFAULT_LOOKBACK_DAYS = int(os.getenv("BRONZE_LOOKBACK_DAYS", "3"))

SOURCE_TABLES = [
    {
        "source_name": "Person.Visits",
        "watermark_column": "Created",
        "watermark_type": "datetime",
        "select_sql": """
            SELECT dateWork, Membership, time_In, time_Out, Created
            FROM Person.Visits
            WHERE (? IS NULL OR Created > ?)
              AND Membership IS NOT NULL
        """,
        "target_sql": """
            INSERT INTO bronze.person_visits_raw
                (datework, membership, time_in, time_out, created)
            VALUES (%s, %s, %s, %s, %s)
        """,
        "watermark_getter": lambda row: row[4],
    },
    {
        "source_name": "Manage.Agents_Players",
        "watermark_column": "dateChange",
        "watermark_type": "datetime",
        "select_sql": """
            SELECT Membership, idAgent, dateChange, Created
            FROM Manage.Agents_Players
            WHERE (? IS NULL OR dateChange > ?)
              AND Membership IS NOT NULL
        """,
        "target_sql": """
            INSERT INTO bronze.manage_agents_players_raw
                (membership, idagent, datechange, created)
            VALUES (%s, %s, %s, %s)
        """,
        "watermark_getter": lambda row: row[2],
    },
    {
        "source_name": "Person.Players",
        "watermark_column": "Membership",
        "watermark_type": "int",
        "select_sql": """
            SELECT Membership, idAgent, idCountry
            FROM Person.Players
            WHERE (? IS NULL OR Membership > ?)
        """,
        "target_sql": """
            INSERT INTO bronze.person_players_raw
                (membership, idagent, idcountry)
            VALUES (%s, %s, %s)
            ON CONFLICT (membership) DO UPDATE SET
                idagent = EXCLUDED.idagent,
                idcountry = EXCLUDED.idcountry,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
    },
    {
        "source_name": "Manage.Agents",
        "watermark_column": "idAgent",
        "watermark_type": "int",
        "select_sql": """
            SELECT idAgent, nameAgent
            FROM Manage.Agents
            WHERE (? IS NULL OR idAgent > ?)
        """,
        "target_sql": """
            INSERT INTO bronze.manage_agents_raw
                (idagent, nameagent)
            VALUES (%s, %s)
            ON CONFLICT (idagent) DO UPDATE SET
                nameagent = EXCLUDED.nameagent,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
    },
    {
        "source_name": "Casino.Countries",
        "watermark_column": "idCountry",
        "watermark_type": "int",
        "select_sql": """
            SELECT idCountry, nameCountry
            FROM Casino.Countries
            WHERE (? IS NULL OR idCountry > ?)
        """,
        "target_sql": """
            INSERT INTO bronze.casino_countries_raw
                (idcountry, namecountry)
            VALUES (%s, %s)
            ON CONFLICT (idcountry) DO UPDATE SET
                namecountry = EXCLUDED.namecountry,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
    },
    {
        "source_name": "CashDesk.view_Transactions",
        "watermark_column": "idOper",
        "watermark_type": "int",
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
        "select_sql": """
            SELECT idOper, dateWork, timeOper, Membership, idAccount, directionOper,
                   TotalMoneyUE, ChipsUE, isDeleted, isCalculatedInDrop
            FROM CashDesk.view_Transactions
            WHERE ((? IS NULL OR idOper > ?) OR dateWork >= ?)
              AND Membership IS NOT NULL
        """,
        "target_sql": """
            INSERT INTO bronze.cashdesk_transactions_raw
                (idoper, datework, timeoper, membership, idaccount, directionoper,
                 totalmoneyue, chipsue, isdeleted, iscalculatedindrop)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idoper) DO UPDATE SET
                datework = EXCLUDED.datework,
                timeoper = EXCLUDED.timeoper,
                membership = EXCLUDED.membership,
                idaccount = EXCLUDED.idaccount,
                directionoper = EXCLUDED.directionoper,
                totalmoneyue = EXCLUDED.totalmoneyue,
                chipsue = EXCLUDED.chipsue,
                isdeleted = EXCLUDED.isdeleted,
                iscalculatedindrop = EXCLUDED.iscalculatedindrop,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
        "row_mapper": lambda row: (
            row[0],
            row[1],
            row[2],
            row[3],
            row[4],
            row[5],
            row[6],
            row[7],
            None if row[8] is None else bool(row[8]),
            None if row[9] is None else bool(row[9]),
        ),
    },
    {
        "source_name": "Casino.Transactions_Calculated",
        "watermark_column": "idOper",
        "watermark_type": "int",
        "select_sql": """
            SELECT idOper, Deposit
            FROM Casino.Transactions_Calculated
            WHERE (? IS NULL OR idOper > ?)
        """,
        "target_sql": """
            INSERT INTO bronze.casino_transactions_calculated_raw
                (idoper, deposit)
            VALUES (%s, %s)
            ON CONFLICT (idoper) DO UPDATE SET
                deposit = EXCLUDED.deposit,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
    },
    {
        "source_name": "Manage.PlayerSessions",
        "watermark_column": "idPlayersTracking",
        "watermark_type": "int",
        "select_sql": """
            SELECT idPlayersTracking, timeStart, RealDrop, HandHold, CashOut, averBet
            FROM Manage.PlayerSessions
            WHERE (? IS NULL OR idPlayersTracking > ?)
        """,
        "target_sql": """
            INSERT INTO bronze.manage_player_sessions_raw
                (idplayerstracking, timestart, realdrop, handhold, cashout, averbet)
            VALUES (%s, %s, %s, %s, %s, %s)
        """,
        "watermark_getter": lambda row: row[0],
    },
    {
        "source_name": "Casino.PlayersTracking",
        "watermark_column": "idPlayersTracking",
        "watermark_type": "int",
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
        "select_sql": """
            SELECT idPlayersTracking, dateWork, Membership
            FROM Casino.PlayersTracking
            WHERE ((? IS NULL OR idPlayersTracking > ?) OR dateWork >= ?)
              AND Membership IS NOT NULL
        """,
        "target_sql": """
            INSERT INTO bronze.casino_players_tracking_raw
                (idplayerstracking, datework, membership)
            VALUES (%s, %s, %s)
            ON CONFLICT (idplayerstracking) DO UPDATE SET
                datework = EXCLUDED.datework,
                membership = EXCLUDED.membership,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
    },
]


def get_pg_connection():
    return psycopg2.connect(PG_CONN_STR)


def get_mssql_connection():
    return pyodbc.connect(MSSQL_CONN_STR)


def parse_watermark(raw_value: str | None, watermark_type: str) -> Any:
    if raw_value is None:
        return None
    if watermark_type == "int":
        return int(raw_value)
    if watermark_type == "datetime":
        return datetime.fromisoformat(raw_value)
    return raw_value


def get_watermark(pg_conn, source_name):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT watermark_value
            FROM dw_control.etl_watermark
            WHERE source_table = %s
            """,
            (source_name,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def set_watermark(pg_conn, source_name, watermark_column, watermark_value):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw_control.etl_watermark (source_table, watermark_column, watermark_value, updated_at_utc)
            VALUES (%s, %s, %s, now())
            ON CONFLICT (source_table)
            DO UPDATE SET
                watermark_column = EXCLUDED.watermark_column,
                watermark_value = EXCLUDED.watermark_value,
                updated_at_utc = now()
            """,
            (source_name, watermark_column, str(watermark_value) if watermark_value is not None else None),
        )


def log_run(pg_conn, pipeline_name, source_table, status, rows_read=0, rows_written=0, error_message=None):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw_control.etl_run_log
                (pipeline_name, source_table, status, started_at_utc, finished_at_utc, rows_read, rows_written, error_message)
            VALUES (%s, %s, %s, now(), now(), %s, %s, %s)
            """,
            (pipeline_name, source_table, status, rows_read, rows_written, error_message),
        )


def build_select_params(current_watermark, table_cfg):
    lookback_days = table_cfg.get("lookback_days")
    if lookback_days is None:
        return (current_watermark, current_watermark)

    lookback_start = (datetime.now(timezone.utc) - timedelta(days=lookback_days)).date()
    return (current_watermark, current_watermark, lookback_start)


def load_table(mssql_conn, pg_conn, table_cfg):
    source_name = table_cfg["source_name"]
    watermark_column = table_cfg["watermark_column"]
    watermark_type = table_cfg.get("watermark_type", "text")
    select_sql = table_cfg["select_sql"]
    target_sql = table_cfg["target_sql"]

    current_watermark_raw = get_watermark(pg_conn, source_name)
    current_watermark = parse_watermark(current_watermark_raw, watermark_type)
    select_params = build_select_params(current_watermark, table_cfg)

    with mssql_conn.cursor() as src_cur:
        src_cur.execute(select_sql, *select_params)
        rows = src_cur.fetchall()

    if not rows:
        log_run(pg_conn, "bronze_incremental", source_name, "success", 0, 0, None)
        pg_conn.commit()
        return

    max_wm = current_watermark
    rows_to_write = []

    for row in rows:
        row_mapper = table_cfg.get("row_mapper")
        rows_to_write.append(row_mapper(row) if row_mapper else tuple(row))
        candidate = table_cfg["watermark_getter"](row)
        if candidate is not None and (max_wm is None or candidate > max_wm):
            max_wm = candidate

    with pg_conn.cursor() as tgt_cur:
        execute_batch(tgt_cur, target_sql, rows_to_write, page_size=1000)

    set_watermark(pg_conn, source_name, watermark_column, max_wm)
    log_run(pg_conn, "bronze_incremental", source_name, "success", len(rows), len(rows_to_write), None)
    pg_conn.commit()


def main():
    if not MSSQL_CONN_STR or not PG_CONN_STR:
        raise RuntimeError("Set MSSQL_CONN_STR and PG_CONN_STR in environment or .env file")

    mssql_conn = get_mssql_connection()
    pg_conn = get_pg_connection()

    try:
        for table_cfg in SOURCE_TABLES:
            try:
                load_table(mssql_conn, pg_conn, table_cfg)
                print(f"Loaded {table_cfg['source_name']}")
            except Exception as table_ex:
                pg_conn.rollback()
                log_run(pg_conn, "bronze_incremental", table_cfg["source_name"], "failed", 0, 0, str(table_ex))
                pg_conn.commit()
                print(f"Failed {table_cfg['source_name']}: {table_ex}")

    finally:
        mssql_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    print(f"Start bronze load: {datetime.now(timezone.utc).isoformat()}")
    main()
    print(f"End bronze load: {datetime.now(timezone.utc).isoformat()}")