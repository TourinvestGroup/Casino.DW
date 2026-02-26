/*
================================================================================
 dim_egd_position - EGD Floor Position Dimension
 ================================================================================
 PURPOSE:  Master list of ALL slot positions (IpAddr) ever seen across
           SM_EgdCfg, SM_PlayerSessionV7, SM_MeterDayV6.
           Keyed by IpAddr so it joins directly to both fact tables.

 RUN ON:   CIBatumi (192.168.77.15,17420)
 ================================================================================
*/

-- ============================================================================
-- STEP 1: VIEW - Always-fresh dimension from source tables
-- ============================================================================

IF OBJECT_ID('dbo.vw_dim_egd_position', 'V') IS NOT NULL
    DROP VIEW dbo.vw_dim_egd_position;
GO

CREATE VIEW dbo.vw_dim_egd_position
AS
WITH

-- All unique IpAddrs across every table
all_ips AS (
    SELECT DISTINCT IpAddr FROM dbo.SM_PlayerSessionV7
    UNION
    SELECT DISTINCT IpAddr FROM dbo.SM_MeterDayV6
    UNION
    SELECT DISTINCT IpAddr FROM dbo.SM_EgdCfg WHERE IpAddr IS NOT NULL
),

-- Activity from MeterDayV6 (daily aggregated = cleaner than sessions)
meter_activity AS (
    SELECT
        IpAddr,
        MIN(CASE WHEN GamingDay > '2000-01-01' THEN GamingDay END) AS FirstSeenMeter,
        MAX(CASE WHEN GamingDay < '2100-01-01' THEN GamingDay END) AS LastSeenMeter,
        COUNT(DISTINCT CASE WHEN GamingDay BETWEEN '2000-01-01' AND '2100-01-01'
                             THEN GamingDay END)                    AS TotalMeterDays
    FROM dbo.SM_MeterDayV6
    GROUP BY IpAddr
),

-- Activity from PlayerSessionV7
session_activity AS (
    SELECT
        IpAddr,
        MIN(GamingDay) AS FirstSeenSession,
        MAX(GamingDay) AS LastSeenSession,
        COUNT(*)       AS TotalSessions
    FROM dbo.SM_PlayerSessionV7
    GROUP BY IpAddr
),

-- Latest MacAddr from MeterDayV6 (most reliable for "which machine is here now")
latest_mac_meter AS (
    SELECT IpAddr, MacAddr AS CurrentMacAddr
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
),

-- Latest config from SM_EgdCfg per IpAddr (most recent revision where this IP was used)
latest_cfg AS (
    SELECT
        IpAddr,
        MacAddr         AS CfgMacAddr,
        InventoryNr,
        Manufacturer,
        Model,
        Game,
        SerialNumber,
        CoinDenom,
        Currency,
        GameType,
        LicenseNumber,
        InUse           AS CfgInUse,
        GamingDay       AS CfgDate,
        Rev             AS CfgRev
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY IpAddr
                   ORDER BY Rev DESC
               ) AS rn
        FROM dbo.SM_EgdCfg
        WHERE IpAddr IS NOT NULL
    ) t
    WHERE rn = 1
),

-- Count config revisions per IpAddr
cfg_counts AS (
    SELECT IpAddr, COUNT(*) AS CfgRevisions, COUNT(DISTINCT MacAddr) AS MachineTurnover
    FROM dbo.SM_EgdCfg
    WHERE IpAddr IS NOT NULL
    GROUP BY IpAddr
)

SELECT
    a.IpAddr,

    -- Human-readable IP address
    CAST(a.IpAddr / 16777216 AS VARCHAR) + '.' +
    CAST((a.IpAddr / 65536) % 256 AS VARCHAR) + '.' +
    CAST((a.IpAddr / 256) % 256 AS VARCHAR) + '.' +
    CAST(a.IpAddr % 256 AS VARCHAR)                     AS IpAddrReadable,

    -- Floor zone (derived from 3rd octet of IP = 10.1.X.y)
    CAST((a.IpAddr / 256) % 256 AS INT)                 AS FloorZone,

    -- Position within zone
    CAST(a.IpAddr % 256 AS INT)                         AS PositionInZone,

    -- Current machine (from fact data, most reliable)
    lm.CurrentMacAddr,

    -- Config attributes (from SM_EgdCfg, may be stale)
    c.InventoryNr,
    c.Manufacturer,
    c.Model,
    c.Game,
    c.SerialNumber,
    c.CoinDenom,
    c.Currency,
    c.GameType,
    c.LicenseNumber,

    -- Activity status (derived from ACTUAL data, not InUse flag)
    CASE
        WHEN COALESCE(m.LastSeenMeter, s.LastSeenSession) >= DATEADD(DAY, -7, GETDATE())
            THEN 1 ELSE 0
    END                                                  AS IsActive,

    CASE
        WHEN COALESCE(m.LastSeenMeter, s.LastSeenSession) >= DATEADD(DAY, -7, GETDATE())
            THEN 'Active'
        WHEN COALESCE(m.LastSeenMeter, s.LastSeenSession) >= DATEADD(DAY, -90, GETDATE())
            THEN 'Recently Inactive'
        WHEN COALESCE(m.LastSeenMeter, s.LastSeenSession) IS NOT NULL
            THEN 'Decommissioned'
        ELSE 'Config Only'
    END                                                  AS PositionStatus,

    -- Dates
    COALESCE(
        CASE WHEN m.FirstSeenMeter < s.FirstSeenSession THEN m.FirstSeenMeter ELSE s.FirstSeenSession END,
        m.FirstSeenMeter, s.FirstSeenSession
    )                                                    AS FirstSeen,

    COALESCE(
        CASE WHEN m.LastSeenMeter > s.LastSeenSession THEN m.LastSeenMeter ELSE s.LastSeenSession END,
        m.LastSeenMeter, s.LastSeenSession
    )                                                    AS LastSeen,

    -- Volume metrics
    COALESCE(m.TotalMeterDays, 0)                        AS TotalMeterDays,
    COALESCE(s.TotalSessions, 0)                         AS TotalSessions,

    -- Config metadata
    CASE WHEN c.IpAddr IS NOT NULL THEN 1 ELSE 0 END    AS HasEgdCfg,
    COALESCE(cc.CfgRevisions, 0)                         AS CfgRevisions,
    COALESCE(cc.MachineTurnover, 0)                       AS MachineTurnover,

    -- SM_EgdCfg InUse flag (for reference only - unreliable!)
    c.CfgInUse                                            AS CfgInUseFlag_UNRELIABLE

FROM all_ips a
LEFT JOIN meter_activity      m  ON a.IpAddr = m.IpAddr
LEFT JOIN session_activity    s  ON a.IpAddr = s.IpAddr
LEFT JOIN latest_mac_meter    lm ON a.IpAddr = lm.IpAddr
LEFT JOIN latest_cfg          c  ON a.IpAddr = c.IpAddr
LEFT JOIN cfg_counts          cc ON a.IpAddr = cc.IpAddr;
GO


-- ============================================================================
-- STEP 2: Quick validation queries
-- ============================================================================

-- Show full dimension
SELECT * FROM dbo.vw_dim_egd_position ORDER BY IpAddr;

-- Status breakdown
SELECT PositionStatus, COUNT(*) AS Positions
FROM dbo.vw_dim_egd_position
GROUP BY PositionStatus
ORDER BY Positions DESC;

-- Orphan positions (no config)
SELECT *
FROM dbo.vw_dim_egd_position
WHERE HasEgdCfg = 0
ORDER BY IpAddr;

-- Active positions
SELECT IpAddrReadable, CurrentMacAddr, Manufacturer, Model, Game,
       FirstSeen, LastSeen, TotalMeterDays, TotalSessions
FROM dbo.vw_dim_egd_position
WHERE IsActive = 1
ORDER BY FloorZone, PositionInZone;
