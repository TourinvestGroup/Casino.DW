/*
================================================================================
 Bronze layer: EGD tables from CIBatumi (192.168.77.15,17420)
 Source: SM_EgdCfg (machine config revisions)
         SM_MeterDayV6 / SM_PlayerSessionV7 (activity summaries)
================================================================================
*/

-- --------------------------------------------------------------------------
-- 1. Full snapshot of SM_EgdCfg (versioned config log, ~5K rows)
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.sm_egd_cfg_raw (
    generated           timestamp,
    gamingday           date,
    casinoid            char(4),
    siteid              char(4),
    ipaddr              int,
    macaddr             bigint      NOT NULL,
    smibip              int         NOT NULL,
    inventorynr         text,
    manufacturer        text,
    model               text,
    game                text,
    user1               text,
    user2               text,
    coindenom           int,
    currency            smallint,
    licensenumber       text,
    serialnumber        text,
    user3               text,
    user4               text,
    gametype            text,
    inuse               int,
    rev                 int         NOT NULL,
    lastchanged         timestamp,
    lastchangedby       text,
    _source_system      text        DEFAULT 'cibatumi',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (casinoid, siteid, macaddr, smibip, rev)
);
CREATE INDEX IF NOT EXISTS idx_sm_egd_cfg_ipaddr ON bronze.sm_egd_cfg_raw(ipaddr);
CREATE INDEX IF NOT EXISTS idx_sm_egd_cfg_macaddr ON bronze.sm_egd_cfg_raw(macaddr);


-- --------------------------------------------------------------------------
-- 2. Pre-aggregated activity per IpAddr (derived on MSSQL side, ~164 rows)
--    Avoids pulling millions of fact rows just for the dimension.
-- --------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.sm_egd_activity_raw (
    ipaddr              int         PRIMARY KEY,
    first_seen_meter    date,
    last_seen_meter     date,
    total_meter_days    int,
    first_seen_session  date,
    last_seen_session   date,
    total_sessions      bigint,
    latest_macaddr      bigint,
    _source_system      text        DEFAULT 'cibatumi',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
