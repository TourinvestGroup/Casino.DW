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
        "source_name": "Manage.Agent_Groups",
        "watermark_column": "Modified",
        "watermark_type": "datetime",
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
        "select_sql": """
            SELECT idAgentGroup, idAgent, nameAgentGroup, dateBegin, dateEnd,
                   memoAgentGroup, Created, CreatedBy, Modified, ModifiedBy, row_Version
            FROM Manage.Agent_Groups
            WHERE ((? IS NULL OR Modified > ?) OR dateBegin >= ?)
        """,
        "target_sql": """
            INSERT INTO bronze.manage_agent_groups_raw
                (idagentgroup, idagent, nameagentgroup, datebegin, dateend,
                 memoagentgroup, created, createdby, modified, modifiedby, row_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idagentgroup) DO UPDATE SET
                idagent = EXCLUDED.idagent,
                nameagentgroup = EXCLUDED.nameagentgroup,
                datebegin = EXCLUDED.datebegin,
                dateend = EXCLUDED.dateend,
                memoagentgroup = EXCLUDED.memoagentgroup,
                created = EXCLUDED.created,
                createdby = EXCLUDED.createdby,
                modified = EXCLUDED.modified,
                modifiedby = EXCLUDED.modifiedby,
                row_version = EXCLUDED.row_version,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[8],
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
        "source_name": "Casino.Games",
        "watermark_column": "__full_snapshot__",
        "full_snapshot": True,
        "select_sql": """
            SELECT idGame, codeGame, nameGame, listOrder_Game, idBonusSystem, nmbBoxes
            FROM Casino.Games
        """,
        "target_sql": """
            INSERT INTO bronze.casino_games_ref_raw
                (idgame, codegame, namegame, listorder_game, idbonussystem, nmbboxes)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (idgame) DO UPDATE SET
                codegame = EXCLUDED.codegame,
                namegame = EXCLUDED.namegame,
                listorder_game = EXCLUDED.listorder_game,
                idbonussystem = EXCLUDED.idbonussystem,
                nmbboxes = EXCLUDED.nmbboxes,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: None,
    },
    {
        "source_name": "Casino.TableTypesGames",
        "watermark_column": "__full_snapshot__",
        "full_snapshot": True,
        "select_sql": """
            SELECT idTableType, idGame, listOrder
            FROM Casino.TableTypesGames
        """,
        "target_sql": """
            INSERT INTO bronze.casino_table_types_games_raw
                (idtabletype, idgame, listorder)
            VALUES (%s, %s, %s)
            ON CONFLICT (idtabletype, idgame) DO UPDATE SET
                listorder = EXCLUDED.listorder,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: None,
    },
    {
        "source_name": "Casino.TableTypes",
        "watermark_column": "__full_snapshot__",
        "full_snapshot": True,
        "select_sql": """
            SELECT idTableType, codeTableType, nameTableType, listOrder_TableType, idGameType
            FROM Casino.TableTypes
        """,
        "target_sql": """
            INSERT INTO bronze.casino_table_types_raw
                (idtabletype, codetabletype, nametabletype, listorder_tabletype, idgametype)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (idtabletype) DO UPDATE SET
                codetabletype = EXCLUDED.codetabletype,
                nametabletype = EXCLUDED.nametabletype,
                listorder_tabletype = EXCLUDED.listorder_tabletype,
                idgametype = EXCLUDED.idgametype,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: None,
    },
    {
        "source_name": "Casino.Tables",
        "watermark_column": "__full_snapshot__",
        "full_snapshot": True,
        "select_sql": """
            SELECT idTable, idTableType, nameTable, snmbTable, codeTable,
                   isVirtual, listOrder_Table, idBonusGame, idBonusSystem, isMarketing,
                   idCurrency, MysteryGuarantee, rateMystery, lowerLimitMystery,
                   upperLimitMystery, minPlayingBet
            FROM Casino.Tables
        """,
        "target_sql": """
            INSERT INTO bronze.casino_tables_ref_raw
                (idtable, idtabletype, nametable, snmbtable, codetable,
                 isvirtual, listorder_table, idbonusgame, idbonussystem, ismarketing,
                 idcurrency, mysteryguarantee, ratemystery, lowerlimitmystery,
                 upperlimitmystery, minplayingbet)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idtable) DO UPDATE SET
                idtabletype = EXCLUDED.idtabletype,
                nametable = EXCLUDED.nametable,
                snmbtable = EXCLUDED.snmbtable,
                codetable = EXCLUDED.codetable,
                isvirtual = EXCLUDED.isvirtual,
                listorder_table = EXCLUDED.listorder_table,
                idbonusgame = EXCLUDED.idbonusgame,
                idbonussystem = EXCLUDED.idbonussystem,
                ismarketing = EXCLUDED.ismarketing,
                idcurrency = EXCLUDED.idcurrency,
                mysteryguarantee = EXCLUDED.mysteryguarantee,
                ratemystery = EXCLUDED.ratemystery,
                lowerlimitmystery = EXCLUDED.lowerlimitmystery,
                upperlimitmystery = EXCLUDED.upperlimitmystery,
                minplayingbet = EXCLUDED.minplayingbet,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: None,
        "row_mapper": lambda row: (
            row[0], row[1], row[2], row[3], row[4],
            None if row[5] is None else bool(row[5]),
            row[6], row[7], row[8],
            None if row[9] is None else bool(row[9]),
            row[10], row[11], row[12], row[13], row[14], row[15],
        ),
    },
    {
        "source_name": "Casino.Currency_ExchRates",
        "watermark_column": "row_version",
        "watermark_type": "int",
        "select_sql": """
            SELECT idCasino, dateChange, idCurrency, idCurrencyExchRate, ExchRate,
                   Created, CreatedBy, CreatedHostName, Modified, ModifiedBy,
                   ModifiedHostName, row_version
            FROM Casino.Currency_ExchRates
            WHERE (? IS NULL OR row_version > ?)
        """,
        "target_sql": """
            INSERT INTO bronze.casino_currency_exch_rates_raw
                (idcasino, datechange, idcurrency, idcurrencyexchrate, exchrate,
                 created, createdby, createdhostname, modified, modifiedby,
                 modifiedhostname, row_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (idcasino, datechange, idcurrency) DO UPDATE SET
                idcurrencyexchrate = EXCLUDED.idcurrencyexchrate,
                exchrate = EXCLUDED.exchrate,
                created = EXCLUDED.created,
                createdby = EXCLUDED.createdby,
                createdhostname = EXCLUDED.createdhostname,
                modified = EXCLUDED.modified,
                modifiedby = EXCLUDED.modifiedby,
                modifiedhostname = EXCLUDED.modifiedhostname,
                row_version = EXCLUDED.row_version,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[11],
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
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
        "select_sql": """
            SELECT idPlayersTracking, timeStart, RealDrop, HandHold, CashOut, averBet
            FROM Manage.PlayerSessions
            WHERE ((? IS NULL OR idPlayersTracking > ?) OR CAST(timeStart AS date) >= ?)
        """,
        "target_sql": """
            INSERT INTO bronze.manage_player_sessions_raw
                (idplayerstracking, timestart, realdrop, handhold, cashout, averbet)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (idplayerstracking, timestart) DO UPDATE SET
                realdrop = EXCLUDED.realdrop,
                handhold = EXCLUDED.handhold,
                cashout = EXCLUDED.cashout,
                averbet = EXCLUDED.averbet,
                _loaded_at_utc = now()
        """,
        "watermark_getter": lambda row: row[0],
    },
    {
        "source_name": "Manage.view_PlayersTracking",
        "watermark_column": "idPlayersTracking",
        "watermark_type": "int",
        "lookback_days": DEFAULT_LOOKBACK_DAYS,
        "select_sql": """
            SELECT idPlayersTracking, dateWork, Membership
            FROM Manage.view_PlayersTracking
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
    if table_cfg.get("full_snapshot", False):
        return tuple()

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
    is_full_snapshot = table_cfg.get("full_snapshot", False)

    with mssql_conn.cursor() as src_cur:
        if is_full_snapshot:
            src_cur.execute(select_sql)
        else:
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

    if not is_full_snapshot:
        set_watermark(pg_conn, source_name, watermark_column, max_wm)
    else:
        set_watermark(pg_conn, source_name, watermark_column, datetime.now(timezone.utc).isoformat())
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