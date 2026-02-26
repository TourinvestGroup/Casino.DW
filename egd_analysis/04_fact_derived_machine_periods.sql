/*
================================================================================
 Fact-derived Machine Assignment Periods
 ================================================================================
 PURPOSE:  Since SM_EgdCfg misses 9 IPs entirely, this query derives
           machine-at-position periods from actual SM_MeterDayV6 fact data.
           More complete and reliable than SM_EgdCfg alone.

 LOGIC:    Groups consecutive days where the same MacAddr is at the same IpAddr.
           A gap of >1 day or a MacAddr change starts a new period.

 RUN ON:   CIBatumi (192.168.77.15,17420)
 ================================================================================
*/

IF OBJECT_ID('dbo.vw_fact_machine_periods', 'V') IS NOT NULL
    DROP VIEW dbo.vw_fact_machine_periods;
GO

CREATE VIEW dbo.vw_fact_machine_periods
AS
WITH

daily_assignment AS (
    SELECT
        IpAddr,
        MacAddr,
        GamingDay,
        -- Detect when MacAddr changes at an IpAddr
        LAG(MacAddr) OVER (PARTITION BY IpAddr ORDER BY GamingDay)  AS PrevMac,
        LAG(GamingDay) OVER (PARTITION BY IpAddr ORDER BY GamingDay) AS PrevDay
    FROM (
        -- Take one row per IpAddr+GamingDay (latest generated)
        SELECT IpAddr, MacAddr, GamingDay,
               ROW_NUMBER() OVER (
                   PARTITION BY IpAddr, GamingDay
                   ORDER BY Generated DESC
               ) AS rn
        FROM dbo.SM_MeterDayV6
        WHERE GamingDay BETWEEN '2000-01-01' AND '2100-01-01'
    ) t
    WHERE rn = 1
),

-- Mark group boundaries (new machine or gap > 3 days)
boundaries AS (
    SELECT *,
        CASE
            WHEN PrevMac IS NULL
              OR MacAddr <> PrevMac
              OR DATEDIFF(DAY, PrevDay, GamingDay) > 3
            THEN 1 ELSE 0
        END AS IsNewPeriod
    FROM daily_assignment
),

-- Assign group IDs
groups AS (
    SELECT *,
        SUM(IsNewPeriod) OVER (PARTITION BY IpAddr ORDER BY GamingDay) AS PeriodGroup
    FROM boundaries
)

SELECT
    IpAddr,

    CAST(IpAddr / 16777216 AS VARCHAR) + '.' +
    CAST((IpAddr / 65536) % 256 AS VARCHAR) + '.' +
    CAST((IpAddr / 256) % 256 AS VARCHAR) + '.' +
    CAST(IpAddr % 256 AS VARCHAR)                   AS IpAddrReadable,

    MacAddr,
    MIN(GamingDay)                                   AS PeriodStart,
    MAX(GamingDay)                                   AS PeriodEnd,
    COUNT(*)                                         AS DaysActive,

    CASE
        WHEN MAX(GamingDay) >= DATEADD(DAY, -7, GETDATE())
            THEN 'Active'
        WHEN MAX(GamingDay) >= DATEADD(DAY, -90, GETDATE())
            THEN 'Recently Inactive'
        ELSE 'Inactive'
    END                                              AS PeriodStatus

FROM groups
GROUP BY IpAddr, MacAddr, PeriodGroup;
GO


-- ============================================================================
-- Validation
-- ============================================================================

-- Full timeline for all positions
SELECT *
FROM dbo.vw_fact_machine_periods
ORDER BY IpAddr, PeriodStart;

-- Positions with multiple machines over time
SELECT IpAddrReadable, COUNT(*) AS Periods, COUNT(DISTINCT MacAddr) AS UniqueMachines
FROM dbo.vw_fact_machine_periods
GROUP BY IpAddrReadable
HAVING COUNT(DISTINCT MacAddr) > 1
ORDER BY UniqueMachines DESC;

-- Active periods only
SELECT *
FROM dbo.vw_fact_machine_periods
WHERE PeriodStatus = 'Active'
ORDER BY IpAddr;

-- Check orphan IPs (the 9 missing from SM_EgdCfg) - should show data here
SELECT *
FROM dbo.vw_fact_machine_periods
WHERE IpAddr IN (167838470, 167839242, 167841283, 167841284, 167841286,
                 167841288, 167841289, 167841290, 167841539)
ORDER BY IpAddr, PeriodStart;
