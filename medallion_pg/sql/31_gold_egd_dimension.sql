/*
================================================================================
 Gold layer: EGD Position Dimension + Machine History Bridge
 Source: bronze.sm_egd_cfg_raw, bronze.sm_egd_activity_raw
================================================================================
*/

-- --------------------------------------------------------------------------
-- 1. dim_egd_position — one row per floor position (IpAddr)
--    PK = ipaddr, joins directly to EGD fact tables.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_egd_position (
    ipaddr              int         PRIMARY KEY,
    ipaddr_readable     text        NOT NULL,
    floor_zone          int,
    position_in_zone    int,

    -- Current machine at this position (from actual fact data)
    current_macaddr     bigint,

    -- Machine attributes (from SM_EgdCfg latest config)
    inventorynr         text,
    manufacturer        text,
    model               text,
    game                text,
    serialnumber        text,
    coindenom           int,
    currency            smallint,
    gametype            text,
    licensenumber       text,

    -- Status (derived from actual activity, NOT from SM_EgdCfg.InUse)
    is_active           boolean     NOT NULL DEFAULT false,
    position_status     text        NOT NULL DEFAULT 'Unknown',

    -- Lifecycle dates
    first_seen          date,
    last_seen           date,
    total_meter_days    int         NOT NULL DEFAULT 0,
    total_sessions      bigint      NOT NULL DEFAULT 0,

    -- Config metadata
    has_egd_cfg         boolean     NOT NULL DEFAULT false,
    cfg_revisions       int         NOT NULL DEFAULT 0,
    machine_turnover    int         NOT NULL DEFAULT 0,

    loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);


-- --------------------------------------------------------------------------
-- 2. bridge_egd_machine_history — which MacAddr was at which IpAddr when
--    Use for "machines with periods of being active and status"
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.bridge_egd_machine_history (
    ipaddr              int         NOT NULL,
    ipaddr_readable     text        NOT NULL,
    macaddr             bigint      NOT NULL,
    casinoid            char(4),
    siteid              char(4),
    effective_from      date        NOT NULL,
    effective_to        date        NOT NULL DEFAULT '9999-12-31',
    is_current          boolean     NOT NULL DEFAULT false,

    -- Machine attributes at this point in time
    inventorynr         text,
    manufacturer        text,
    model               text,
    game                text,
    serialnumber        text,
    coindenom           int,

    assignment_status   text        NOT NULL DEFAULT 'Historical',
    rev                 int         NOT NULL,

    loaded_at_utc       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (ipaddr, macaddr, rev)
);
CREATE INDEX IF NOT EXISTS idx_bridge_egd_history_macaddr
    ON gold.bridge_egd_machine_history(macaddr);
CREATE INDEX IF NOT EXISTS idx_bridge_egd_history_dates
    ON gold.bridge_egd_machine_history(effective_from, effective_to);


-- --------------------------------------------------------------------------
-- 3. sp_load_dim_egd_position — full refresh procedure
-- --------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION gold.sp_load_dim_egd_position()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- ====== DIM_EGD_POSITION ======

    TRUNCATE gold.dim_egd_position;

    INSERT INTO gold.dim_egd_position (
        ipaddr, ipaddr_readable, floor_zone, position_in_zone,
        current_macaddr,
        inventorynr, manufacturer, model, game, serialnumber,
        coindenom, currency, gametype, licensenumber,
        is_active, position_status,
        first_seen, last_seen, total_meter_days, total_sessions,
        has_egd_cfg, cfg_revisions, machine_turnover
    )
    WITH

    -- All unique IpAddrs from both bronze tables
    all_ips AS (
        SELECT DISTINCT ipaddr FROM bronze.sm_egd_activity_raw
        UNION
        SELECT DISTINCT ipaddr FROM bronze.sm_egd_cfg_raw WHERE ipaddr IS NOT NULL
    ),

    -- Latest config per IpAddr from SM_EgdCfg
    latest_cfg AS (
        SELECT DISTINCT ON (ipaddr)
            ipaddr,
            inventorynr, manufacturer, model, game, serialnumber,
            coindenom, currency, gametype, licensenumber
        FROM bronze.sm_egd_cfg_raw
        WHERE ipaddr IS NOT NULL
        ORDER BY ipaddr, rev DESC
    ),

    -- Config counts per IpAddr
    cfg_counts AS (
        SELECT
            ipaddr,
            COUNT(*)                AS cfg_revisions,
            COUNT(DISTINCT macaddr) AS machine_turnover
        FROM bronze.sm_egd_cfg_raw
        WHERE ipaddr IS NOT NULL
        GROUP BY ipaddr
    )

    SELECT
        a.ipaddr,

        -- Human-readable IP
        (a.ipaddr / 16777216)::text || '.' ||
        ((a.ipaddr / 65536) % 256)::text || '.' ||
        ((a.ipaddr / 256) % 256)::text || '.' ||
        (a.ipaddr % 256)::text,

        (a.ipaddr / 256) % 256,           -- floor_zone
        a.ipaddr % 256,                    -- position_in_zone

        act.latest_macaddr,

        c.inventorynr, c.manufacturer, c.model, c.game, c.serialnumber,
        c.coindenom, c.currency, c.gametype, c.licensenumber,

        -- is_active: had activity in last 7 days
        COALESCE(
            COALESCE(act.last_seen_meter, act.last_seen_session) >= (CURRENT_DATE - INTERVAL '7 days'),
            false
        ),

        -- position_status
        CASE
            WHEN COALESCE(act.last_seen_meter, act.last_seen_session) >= (CURRENT_DATE - INTERVAL '7 days')
                THEN 'Active'
            WHEN COALESCE(act.last_seen_meter, act.last_seen_session) >= (CURRENT_DATE - INTERVAL '90 days')
                THEN 'Recently Inactive'
            WHEN COALESCE(act.last_seen_meter, act.last_seen_session) IS NOT NULL
                THEN 'Decommissioned'
            ELSE 'Config Only'
        END,

        -- Lifecycle dates
        LEAST(act.first_seen_meter, act.first_seen_session),
        GREATEST(act.last_seen_meter, act.last_seen_session),
        COALESCE(act.total_meter_days, 0),
        COALESCE(act.total_sessions, 0),

        -- Config metadata
        c.ipaddr IS NOT NULL,
        COALESCE(cc.cfg_revisions, 0),
        COALESCE(cc.machine_turnover, 0)

    FROM all_ips a
    LEFT JOIN bronze.sm_egd_activity_raw act ON a.ipaddr = act.ipaddr
    LEFT JOIN latest_cfg                  c  ON a.ipaddr = c.ipaddr
    LEFT JOIN cfg_counts                  cc ON a.ipaddr = cc.ipaddr;


    -- ====== BRIDGE_EGD_MACHINE_HISTORY ======

    TRUNCATE gold.bridge_egd_machine_history;

    INSERT INTO gold.bridge_egd_machine_history (
        ipaddr, ipaddr_readable, macaddr, casinoid, siteid,
        effective_from, effective_to, is_current,
        inventorynr, manufacturer, model, game, serialnumber, coindenom,
        assignment_status, rev
    )
    WITH cfg_with_next AS (
        SELECT
            ipaddr,
            macaddr,
            casinoid,
            siteid,
            gamingday                                                          AS effective_from,
            LEAD(gamingday) OVER (PARTITION BY macaddr ORDER BY rev)           AS effective_to_raw,
            ROW_NUMBER() OVER (PARTITION BY macaddr ORDER BY rev DESC) = 1     AS is_latest,
            inventorynr, manufacturer, model, game, serialnumber, coindenom,
            rev
        FROM bronze.sm_egd_cfg_raw
        WHERE ipaddr IS NOT NULL
    )
    SELECT
        ipaddr,
        (ipaddr / 16777216)::text || '.' ||
        ((ipaddr / 65536) % 256)::text || '.' ||
        ((ipaddr / 256) % 256)::text || '.' ||
        (ipaddr % 256)::text,
        macaddr,
        casinoid,
        siteid,
        effective_from,
        COALESCE(effective_to_raw, '9999-12-31'::date),
        is_latest,
        inventorynr, manufacturer, model, game, serialnumber, coindenom,
        CASE WHEN is_latest AND effective_to_raw IS NULL THEN 'Current' ELSE 'Historical' END,
        rev
    FROM cfg_with_next;

END;
$$;
