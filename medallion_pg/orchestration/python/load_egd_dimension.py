"""
EGD Dimension Loader
====================
Extracts EGD machine data from CIBatumi (192.168.77.15,17420)
and materializes dim_egd_position + bridge_egd_machine_history
in PostgreSQL Casino.DW.

Source tables: dbo.SM_EgdCfg, dbo.SM_MeterDayV6, dbo.SM_PlayerSessionV7
Target tables: bronze.sm_egd_cfg_raw, bronze.sm_egd_activity_raw
               gold.dim_egd_position, gold.bridge_egd_machine_history

Strategy: full snapshot (dimension is small ~5K cfg + ~164 activity rows)
"""

import os
from datetime import datetime, timezone

import pyodbc
import psycopg2
from psycopg2.extras import execute_batch
from dotenv import load_dotenv

load_dotenv()

MSSQL_CIBATUMI_CONN_STR = os.getenv("MSSQL_CIBATUMI_CONN_STR")
PG_CONN_STR = os.getenv("PG_CONN_STR")


# ── MSSQL queries (run on CIBatumi) ──────────────────────────────────────

SQL_EGD_CFG = """
    SELECT
        Generated, GamingDay, CasinoId, SiteId,
        IpAddr, MacAddr, SmibIp,
        InventoryNr, Manufacturer, Model, Game,
        User1, User2, CoinDenom, Currency,
        LicenseNumber, SerialNumber, User3, User4,
        GameType, InUse, Rev,
        LastChanged, LastChangedBy
    FROM dbo.SM_EgdCfg
"""

SQL_EGD_ACTIVITY = """
    WITH meter AS (
        SELECT
            IpAddr,
            MIN(CASE WHEN GamingDay > '2000-01-01' THEN GamingDay END) AS first_seen_meter,
            MAX(CASE WHEN GamingDay < '2100-01-01' THEN GamingDay END) AS last_seen_meter,
            COUNT(DISTINCT CASE WHEN GamingDay BETWEEN '2000-01-01' AND '2100-01-01'
                                THEN GamingDay END)                    AS total_meter_days
        FROM dbo.SM_MeterDayV6
        GROUP BY IpAddr
    ),
    sess AS (
        SELECT
            IpAddr,
            MIN(GamingDay) AS first_seen_session,
            MAX(GamingDay) AS last_seen_session,
            COUNT(*)       AS total_sessions
        FROM dbo.SM_PlayerSessionV7
        GROUP BY IpAddr
    ),
    latest_mac AS (
        SELECT IpAddr, MacAddr AS latest_macaddr
        FROM (
            SELECT IpAddr, MacAddr,
                   ROW_NUMBER() OVER (
                       PARTITION BY IpAddr
                       ORDER BY GamingDay DESC, Generated DESC
                   ) AS rn
            FROM dbo.SM_MeterDayV6
            WHERE GamingDay BETWEEN '2000-01-01' AND '2100-01-01'
        ) t
        WHERE rn = 1
    )
    SELECT
        COALESCE(m.IpAddr, s.IpAddr) AS IpAddr,
        m.first_seen_meter,
        m.last_seen_meter,
        COALESCE(m.total_meter_days, 0),
        s.first_seen_session,
        s.last_seen_session,
        COALESCE(s.total_sessions, 0),
        lm.latest_macaddr
    FROM meter m
    FULL OUTER JOIN sess s ON m.IpAddr = s.IpAddr
    LEFT JOIN latest_mac lm ON COALESCE(m.IpAddr, s.IpAddr) = lm.IpAddr
"""


# ── PostgreSQL target queries ────────────────────────────────────────────

PG_UPSERT_CFG = """
    INSERT INTO bronze.sm_egd_cfg_raw (
        generated, gamingday, casinoid, siteid,
        ipaddr, macaddr, smibip,
        inventorynr, manufacturer, model, game,
        user1, user2, coindenom, currency,
        licensenumber, serialnumber, user3, user4,
        gametype, inuse, rev,
        lastchanged, lastchangedby
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (casinoid, siteid, macaddr, smibip, rev) DO UPDATE SET
        generated     = EXCLUDED.generated,
        gamingday     = EXCLUDED.gamingday,
        ipaddr        = EXCLUDED.ipaddr,
        inventorynr   = EXCLUDED.inventorynr,
        manufacturer  = EXCLUDED.manufacturer,
        model         = EXCLUDED.model,
        game          = EXCLUDED.game,
        user1         = EXCLUDED.user1,
        user2         = EXCLUDED.user2,
        coindenom     = EXCLUDED.coindenom,
        currency      = EXCLUDED.currency,
        licensenumber = EXCLUDED.licensenumber,
        serialnumber  = EXCLUDED.serialnumber,
        user3         = EXCLUDED.user3,
        user4         = EXCLUDED.user4,
        gametype      = EXCLUDED.gametype,
        inuse         = EXCLUDED.inuse,
        lastchanged   = EXCLUDED.lastchanged,
        lastchangedby = EXCLUDED.lastchangedby,
        _loaded_at_utc = now()
"""

PG_UPSERT_ACTIVITY = """
    INSERT INTO bronze.sm_egd_activity_raw (
        ipaddr, first_seen_meter, last_seen_meter, total_meter_days,
        first_seen_session, last_seen_session, total_sessions, latest_macaddr
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (ipaddr) DO UPDATE SET
        first_seen_meter   = EXCLUDED.first_seen_meter,
        last_seen_meter    = EXCLUDED.last_seen_meter,
        total_meter_days   = EXCLUDED.total_meter_days,
        first_seen_session = EXCLUDED.first_seen_session,
        last_seen_session  = EXCLUDED.last_seen_session,
        total_sessions     = EXCLUDED.total_sessions,
        latest_macaddr     = EXCLUDED.latest_macaddr,
        _loaded_at_utc     = now()
"""


def log_run(pg_conn, source_table, status, rows_read=0, rows_written=0, error_message=None):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dw_control.etl_run_log
                (pipeline_name, source_table, status, started_at_utc, finished_at_utc,
                 rows_read, rows_written, error_message)
            VALUES ('egd_dimension', %s, %s, now(), now(), %s, %s, %s)
            """,
            (source_table, status, rows_read, rows_written, error_message),
        )


def extract_and_load(mssql_conn, pg_conn, source_sql, target_sql, label):
    """Generic extract from MSSQL -> load into PG bronze."""
    print(f"  Extracting {label} from CIBatumi ...")
    with mssql_conn.cursor() as src_cur:
        src_cur.execute(source_sql)
        rows = src_cur.fetchall()

    if not rows:
        print(f"  {label}: 0 rows")
        log_run(pg_conn, label, "success", 0, 0)
        pg_conn.commit()
        return 0

    data = [tuple(r) for r in rows]

    print(f"  Loading {len(data)} rows into PG ...")
    with pg_conn.cursor() as tgt_cur:
        execute_batch(tgt_cur, target_sql, data, page_size=1000)

    log_run(pg_conn, label, "success", len(rows), len(data))
    pg_conn.commit()
    print(f"  {label}: {len(data)} rows loaded")
    return len(data)


def build_gold(pg_conn):
    """Refresh gold.dim_egd_position and gold.bridge_egd_machine_history."""
    print("  Building gold.dim_egd_position + bridge_egd_machine_history ...")
    with pg_conn.cursor() as cur:
        cur.execute("SELECT gold.sp_load_dim_egd_position()")
    pg_conn.commit()

    # Report counts
    with pg_conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM gold.dim_egd_position")
        dim_cnt = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM gold.bridge_egd_machine_history")
        bridge_cnt = cur.fetchone()[0]

    log_run(pg_conn, "gold.dim_egd_position", "success", dim_cnt, dim_cnt)
    log_run(pg_conn, "gold.bridge_egd_machine_history", "success", bridge_cnt, bridge_cnt)
    pg_conn.commit()

    print(f"  dim_egd_position:           {dim_cnt} rows")
    print(f"  bridge_egd_machine_history: {bridge_cnt} rows")


def validate(pg_conn):
    """Quick validation queries."""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT position_status, COUNT(*) AS cnt
            FROM gold.dim_egd_position
            GROUP BY position_status
            ORDER BY cnt DESC
        """)
        print("\n  Position status breakdown:")
        for row in cur.fetchall():
            print(f"    {row[0]:25s}: {row[1]}")

        cur.execute("""
            SELECT COUNT(*) FROM gold.dim_egd_position WHERE has_egd_cfg = false
        """)
        orphans = cur.fetchone()[0]
        if orphans > 0:
            print(f"\n  WARNING: {orphans} positions have NO SM_EgdCfg config (orphans)")

        cur.execute("""
            SELECT COUNT(*) FROM gold.dim_egd_position WHERE is_active = true
        """)
        active = cur.fetchone()[0]
        print(f"  Active positions: {active}")


def main():
    if not MSSQL_CIBATUMI_CONN_STR:
        raise RuntimeError("Set MSSQL_CIBATUMI_CONN_STR in .env (CIBatumi connection)")
    if not PG_CONN_STR:
        raise RuntimeError("Set PG_CONN_STR in .env (PostgreSQL Casino.DW connection)")

    print(f"EGD Dimension load started: {datetime.now(timezone.utc).isoformat()}")

    mssql_conn = pyodbc.connect(MSSQL_CIBATUMI_CONN_STR)
    pg_conn = psycopg2.connect(PG_CONN_STR)

    try:
        # Step 1: Bronze - Extract SM_EgdCfg
        extract_and_load(mssql_conn, pg_conn, SQL_EGD_CFG, PG_UPSERT_CFG, "SM_EgdCfg")

        # Step 2: Bronze - Extract activity summary
        extract_and_load(mssql_conn, pg_conn, SQL_EGD_ACTIVITY, PG_UPSERT_ACTIVITY, "SM_EgdActivity")

        # Step 3: Gold - Build dimension + bridge
        build_gold(pg_conn)

        # Step 4: Validate
        validate(pg_conn)

    except Exception as ex:
        pg_conn.rollback()
        log_run(pg_conn, "egd_dimension", "failed", 0, 0, str(ex))
        pg_conn.commit()
        print(f"FAILED: {ex}")
        raise
    finally:
        mssql_conn.close()
        pg_conn.close()

    print(f"\nEGD Dimension load complete: {datetime.now(timezone.utc).isoformat()}")


if __name__ == "__main__":
    main()
