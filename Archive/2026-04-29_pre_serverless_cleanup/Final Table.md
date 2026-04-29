
## Фильтр по одному Агенту. Добавить в начале ко всем DECLARE
```sql
DECLARE @AgentId int = 123;  -- нужный агент ID 
```
###  В конце добавить после FROM day_rows
```sql
WHERE (@AgentId IS NULL OR idAgent = @AgentId)
```



```sql

DECLARE @Year  int = 2026;
DECLARE @Month int = 1;

DECLARE @gdFrom date = DATEFROMPARTS(@Year, @Month, 1);
DECLARE @gdTo   date = EOMONTH(@gdFrom);


;WITH visits_base AS (
    /* one row per (GamingDay, Membership) as "true visit" */
    SELECT
        GamingDay  = v.dateWork,
        v.Membership,
        VisitTime  = MIN(COALESCE(v.time_In, v.Created)),   -- anchor time for agent history
        time_In    = MIN(v.time_In),
        time_Out   = MAX(v.time_Out)
    FROM Person.Visits v
    WHERE
        v.dateWork >= @gdFrom
        AND v.dateWork <= @gdTo
        AND v.Membership IS NOT NULL
        AND v.Membership <> 5
    GROUP BY
        v.dateWork, v.Membership
),

agent_at_visit AS (
    SELECT
        vb.GamingDay,
        vb.Membership,

        idAgent = COALESCE(
          ap_hist.idAgent,
          NULLIF(p0.idAgent, 0)
        )


    FROM visits_base vb

    OUTER APPLY (
        SELECT TOP (1) ap2.idAgent
        FROM Manage.Agents_Players ap2
        WHERE ap2.Membership = vb.Membership
          AND ap2.dateChange <= vb.VisitTime
        ORDER BY ap2.dateChange DESC, ap2.Created DESC
    ) ap_hist

    LEFT JOIN Person.Players p0
        ON p0.Membership = vb.Membership

),


/* === REPLACE your money_tx + drop_events with this === */

money_tx AS (
    SELECT
        GamingDay = vt.dateWork,          -- ВАЖНО: считаем по dateWork системы
        vt.timeOper,
        vt.Membership,
        vt.idOper,
        vt.idAccount,

        -- Универсальные суммы вместо TotalMoneyUE_In/Out:
        InAmt     = CASE WHEN vt.directionOper =  1 THEN ABS(ISNULL(vt.TotalMoneyUE,0.0)) ELSE 0.0 END,
        OutAmtAbs = CASE WHEN vt.directionOper = -1 THEN ABS(ISNULL(vt.TotalMoneyUE,0.0)) ELSE 0.0 END,

        vt.TotalMoneyUE,
        tc.Deposit
    FROM CashDesk.view_Transactions vt
    LEFT JOIN Casino.Transactions_Calculated tc
           ON tc.idOper = vt.idOper
    WHERE
        vt.dateWork >= @gdFrom AND vt.dateWork <= @gdTo
        AND vt.Membership IS NOT NULL
        AND vt.Membership <> 5
        AND vt.idAccount IN (121,703,802,803)
        AND ISNULL(vt.isDeleted,0) = 0
        AND ISNULL(vt.isCalculatedInDrop,0) = 1   -- именно то, что “система считает в DROP”
),


drop_events AS (
    SELECT
        GamingDay, Membership, timeOper, idOper,
        BuyAmt = CAST(CASE WHEN idAccount IN (121,703) THEN InAmt     ELSE 0.0 END AS decimal(19,4)),
        OutAmt = CAST(CASE WHEN idAccount = 121       THEN OutAmtAbs ELSE 0.0 END AS decimal(19,4))
    FROM money_tx
    WHERE idAccount IN (121,703)
),


drop_calc AS (
    SELECT
        de.*,
        S = SUM(de.OutAmt - de.BuyAmt) OVER (
                PARTITION BY de.GamingDay, de.Membership
                ORDER BY de.timeOper, de.idOper
                ROWS UNBOUNDED PRECEDING
            )
    FROM drop_events de
),

drop_pool1 AS (
    SELECT
        x.*,
        PoolAfterRaw = CAST(
            x.S - CASE WHEN x.MinS > 0 THEN 0 ELSE x.MinS END
        AS decimal(19,4))
    FROM (
        SELECT
            dc.*,
            MinS = MIN(dc.S) OVER (
                PARTITION BY dc.GamingDay, dc.Membership
                ORDER BY dc.timeOper, dc.idOper
                ROWS UNBOUNDED PRECEDING
            )
        FROM drop_calc dc
    ) x
),


drop_pool AS (
    SELECT
        p1.*,
        PoolAfter  = p1.PoolAfterRaw,
        PoolBefore = CAST(
            LAG(p1.PoolAfterRaw, 1, 0.0) OVER (
                PARTITION BY p1.GamingDay, p1.Membership
                ORDER BY p1.timeOper, p1.idOper
            ) AS decimal(19,4)
        )
    FROM drop_pool1 p1
),


drop_clean AS (
    SELECT
        GamingDay,
        Membership,
        TotalDrop_Clean = SUM(
            CASE
                WHEN BuyAmt > 0 THEN
                    BuyAmt - (
                        CASE
                            WHEN PoolBefore > 0
                                THEN CASE WHEN BuyAmt >= PoolBefore THEN PoolBefore ELSE BuyAmt END
                            ELSE 0.0
                        END
                    )
                ELSE 0.0
            END
        )
    FROM drop_pool
    GROUP BY GamingDay, Membership
),



money_day AS (
    SELECT
        GamingDay,
        Membership,

        CashDesk_In  = SUM(CASE WHEN idAccount = 121 THEN InAmt     ELSE 0.0 END),
        CashDesk_Out = SUM(CASE WHEN idAccount = 121 THEN OutAmtAbs ELSE 0.0 END),
        TableCash_In = SUM(CASE WHEN idAccount = 703 THEN InAmt     ELSE 0.0 END),

        TotalExchange_In  = SUM(CASE WHEN idAccount IN (121,703) THEN InAmt ELSE 0.0 END),
        TotalExchange_Net =
            SUM(CASE WHEN idAccount IN (121,703) THEN InAmt ELSE 0.0 END)
          - SUM(CASE WHEN idAccount = 121       THEN OutAmtAbs ELSE 0.0 END),

        AgentTransfer_Net =
            SUM(CASE WHEN idAccount = 802 THEN ISNULL(TotalMoneyUE,0.0) ELSE 0.0 END),

        -- ДЕПОЗИТЫ (803) — возвращаем как было:
        PrivateDeposit_Net =
            SUM(CASE WHEN idAccount = 803 THEN ISNULL(Deposit, 0.0) ELSE 0.0 END),
        PrivateDeposit_Add =
            SUM(CASE WHEN idAccount = 803 AND ISNULL(Deposit,0.0) > 0 THEN Deposit ELSE 0.0 END),
        PrivateDeposit_Withdraw =
            SUM(CASE WHEN idAccount = 803 AND ISNULL(Deposit,0.0) < 0 THEN Deposit ELSE 0.0 END)

    FROM money_tx
    GROUP BY GamingDay, Membership
),


sess AS (
    SELECT
        GamingDay = CAST(pt.dateWork AS date),   -- WorkDay системы
        ps.timeStart,
        pt.Membership,
        ps.RealDrop,
        ps.HandHold,
        ps.CashOut,
        ps.averBet
    FROM Manage.PlayerSessions ps
    JOIN Casino.PlayersTracking pt
         ON pt.idPlayersTracking = ps.idPlayersTracking
    WHERE
        pt.dateWork >= @gdFrom
        AND pt.dateWork <= @gdTo
        AND pt.Membership IS NOT NULL
        AND pt.Membership <> 5
),
sess_day AS (
    SELECT
        GamingDay,
        Membership,
        SessionsCnt = COUNT(*)
    FROM sess
    GROUP BY GamingDay, Membership
),


system_drop AS (
    SELECT
        GamingDay   = vt.dateWork,
        Membership  = vt.Membership,
        SystemDrop_In = SUM(
            CASE
                WHEN vt.directionOper = 1
                 AND vt.isCalculatedInDrop = 1
                 AND vt.idAccount IN (121,703)
                THEN ABS(ISNULL(vt.ChipsUE, 0.0))
                ELSE 0.0
            END
        )
    FROM CashDesk.view_Transactions vt
    WHERE
        vt.dateWork >= @gdFrom AND vt.dateWork <= @gdTo
        AND vt.Membership IS NOT NULL
        AND vt.Membership <> 5
        AND ISNULL(vt.isDeleted,0) = 0
    GROUP BY vt.dateWork, vt.Membership
),

/* ============================================================
   FINAL OUTPUT: UNIQUE MEMBERSHIP FOR PERIOD (keep ACTUAL CTEs)
   ============================================================ */

/* --- тут остаются ВСЕ CTE из ACTUAL без изменений --- */
/* visits_base, agent_at_visit, money_tx, drop_events, drop_clean,
   money_day, sess_day, system_drop, ... */

/* 1) Собираем дневные строки (как ACTUAL day-by-day результат) */
day_rows AS (
    SELECT
        vb.GamingDay,
        vb.Membership,

        aa.idAgent,
        AgentName =
            CASE
                WHEN aa.idAgent IS NULL THEN 'NO AGENT'
                WHEN ISNULL(a.nameAgent,'') = '' THEN 'NO AGENT'
                WHEN a.nameAgent = 'NO AGENT' THEN 'NO AGENT'
                ELSE a.nameAgent
            END,

        CitizenshipCountry = cc.nameCountry,

        /* money */
        ISNULL(dc.TotalDrop_Clean,0.0) AS TotalDrop_Clean,
        ISNULL(sd.SystemDrop_In, 0.0)  AS SystemDrop_In,

        ISNULL(m.TotalExchange_In, 0.0)  AS TotalCash_In,
        ISNULL(m.TotalExchange_Net, 0.0) AS TotalCash_Result,

        ISNULL(m.CashDesk_In, 0.0)    AS CashDesk_In,
        ISNULL(m.CashDesk_Out, 0.0)   AS CashDesk_Out,
        ISNULL(m.TableCash_In, 0.0)   AS TableCash_In,

        ISNULL(m.AgentTransfer_Net, 0.0)          AS AgentTransfer_Net,
        -ISNULL(m.PrivateDeposit_Add, 0.0)        AS JunketDeposit_Add,
        -ISNULL(m.PrivateDeposit_Withdraw, 0.0)   AS JunketDeposit_Withdraw,
        -ISNULL(m.PrivateDeposit_Net, 0.0)        AS JunketDeposit_Net,

        /* sessions */
        ISNULL(s.SessionsCnt, 0) AS SessionsCnt

    FROM visits_base vb
    LEFT JOIN agent_at_visit aa
        ON aa.GamingDay = vb.GamingDay
       AND aa.Membership = vb.Membership
    LEFT JOIN Manage.Agents a
        ON a.idAgent = aa.idAgent
    LEFT JOIN Person.Players pp
        ON pp.Membership = vb.Membership
    LEFT JOIN Casino.Countries cc
        ON cc.idCountry = pp.idCountry
    LEFT JOIN money_day m
        ON m.GamingDay = vb.GamingDay
       AND m.Membership = vb.Membership
    LEFT JOIN sess_day s
        ON s.GamingDay = vb.GamingDay
       AND s.Membership = vb.Membership
    LEFT JOIN drop_clean dc
        ON dc.GamingDay = vb.GamingDay
       AND dc.Membership = vb.Membership
    LEFT JOIN system_drop sd
        ON sd.GamingDay = vb.GamingDay
       AND sd.Membership = vb.Membership
)

/* 2) Финальная агрегация: 1 строка на Membership за период */
SELECT
    Membership,

    idAgent              = MAX(idAgent),
    AgentName            = MAX(AgentName),
    CitizenshipCountry   = MAX(CitizenshipCountry),

    VisitsDays           = COUNT(*),

    TotalDrop_Clean      = SUM(TotalDrop_Clean),
    SystemDrop_In        = SUM(SystemDrop_In),

    TotalCash_In         = SUM(TotalCash_In),
    TotalCash_Result     = SUM(TotalCash_Result),

    CashDesk_In          = SUM(CashDesk_In),
    CashDesk_Out         = SUM(CashDesk_Out),
    TableCash_In         = SUM(TableCash_In),

    AgentTransfer_Net       = SUM(AgentTransfer_Net),
    JunketDeposit_Add       = SUM(JunketDeposit_Add),
    JunketDeposit_Withdraw  = SUM(JunketDeposit_Withdraw),
    JunketDeposit_Net       = SUM(JunketDeposit_Net),

    SessionsCnt          = SUM(SessionsCnt)

FROM day_rows
GROUP BY Membership
ORDER BY TotalDrop_Clean DESC;

```