/*
================================================================================
 bridge_egd_position_machine_history
 ================================================================================
 PURPOSE:  Which physical machine (MacAddr) was at which floor position (IpAddr)
           during which period. This is your "machines with periods of being
           active and status" table.

 LOGIC:    Derives assignment periods from SM_EgdCfg revisions.
           Each row = one continuous assignment of a MacAddr to an IpAddr.

 RUN ON:   CIBatumi (192.168.77.15,17420)
 ================================================================================
*/

IF OBJECT_ID('dbo.vw_bridge_egd_position_machine', 'V') IS NOT NULL
    DROP VIEW dbo.vw_bridge_egd_position_machine;
GO

CREATE VIEW dbo.vw_bridge_egd_position_machine
AS
WITH

-- Each config revision with its effective date range
cfg_with_dates AS (
    SELECT
        IpAddr,
        MacAddr,
        CasinoId,
        SiteId,
        Rev,
        GamingDay                                                    AS EffectiveFrom,
        LEAD(GamingDay) OVER (
            PARTITION BY MacAddr ORDER BY Rev
        )                                                            AS EffectiveToRaw,
        InventoryNr,
        Manufacturer,
        Model,
        Game,
        SerialNumber,
        CoinDenom,
        InUse,
        ROW_NUMBER() OVER (PARTITION BY MacAddr ORDER BY Rev DESC)   AS IsLatest
    FROM dbo.SM_EgdCfg
    WHERE IpAddr IS NOT NULL
)

SELECT
    IpAddr,

    -- Human-readable IP
    CAST(IpAddr / 16777216 AS VARCHAR) + '.' +
    CAST((IpAddr / 65536) % 256 AS VARCHAR) + '.' +
    CAST((IpAddr / 256) % 256 AS VARCHAR) + '.' +
    CAST(IpAddr % 256 AS VARCHAR)                        AS IpAddrReadable,

    MacAddr,
    CasinoId,
    SiteId,

    EffectiveFrom,
    COALESCE(EffectiveToRaw, '9999-12-31')               AS EffectiveTo,

    CASE WHEN IsLatest = 1 THEN 1 ELSE 0 END             AS IsCurrentAssignment,

    -- Machine attributes at this point in time
    InventoryNr,
    Manufacturer,
    Model,
    Game,
    SerialNumber,
    CoinDenom,

    -- Assignment status
    CASE
        WHEN IsLatest = 1 AND EffectiveToRaw IS NULL
            THEN 'Current'
        ELSE 'Historical'
    END                                                   AS AssignmentStatus,

    Rev

FROM cfg_with_dates;
GO


-- ============================================================================
-- Validation queries
-- ============================================================================

-- Full machine rotation history for a specific position
SELECT *
FROM dbo.vw_bridge_egd_position_machine
WHERE IpAddrReadable = '10.1.3.3'
ORDER BY EffectiveFrom;

-- All positions a specific machine has been at
-- (replace MacAddr value with one you want to trace)
SELECT *
FROM dbo.vw_bridge_egd_position_machine
WHERE MacAddr = 62496249155
ORDER BY EffectiveFrom;

-- Current assignments only
SELECT *
FROM dbo.vw_bridge_egd_position_machine
WHERE IsCurrentAssignment = 1
ORDER BY IpAddr;
