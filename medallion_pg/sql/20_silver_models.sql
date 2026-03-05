DROP FUNCTION IF EXISTS silver.fn_membership_day(date, date, int);
CREATE OR REPLACE FUNCTION silver.fn_membership_day(
    p_from_date date,
    p_to_date   date,
    p_agent_id  int DEFAULT NULL
)
RETURNS TABLE (
    gamingday               date,
    membership              bigint,
    surname                 text,
    forename                text,
    idagent                 int,
    agentname               text,
    citizenshipcountry      text,
    totaldrop_clean         numeric(19,4),
    systemdrop_in           numeric(19,4),
    totalcash_in            numeric(19,4),
    totalcash_result        numeric(19,4),
    cashdesk_in             numeric(19,4),
    cashdesk_out            numeric(19,4),
    tablecash_in            numeric(19,4),
    agenttransfer_net       numeric(19,4),
    junketdeposit_add       numeric(19,4),
    junketdeposit_withdraw  numeric(19,4),
    junketdeposit_net       numeric(19,4),
    sessionscnt             bigint,
    minutes_played          numeric(19,4),
    slot_totalbet           numeric(19,4),
    slot_cashbet            numeric(19,4),
    slot_totalout           numeric(19,4),
    slot_win                numeric(19,4),
    slot_nwl                numeric(19,4),
    slot_billdrop           numeric(19,4),
    slot_gamesplayed        numeric(19,4),
    slot_sessions_cnt       bigint,
    tracking_floatin        numeric(19,4),
    tracking_floatout       numeric(19,4),
    tracking_net            numeric(19,4),
    expense_total           numeric(19,4),
    expense_airtickets      numeric(19,4),
    expense_discount_plus   numeric(19,4),
    expense_hotel           numeric(19,4),
    expense_other           numeric(19,4),
    discount_lg             numeric(19,4),
    discount_slot           numeric(19,4),
    agent_credit_out        numeric(19,4),
    agent_credit_void       numeric(19,4),
    agent_credit_net        numeric(19,4)
)
LANGUAGE sql
AS $$
WITH visits_base AS (
    SELECT gamingday, membership, visittime, time_in, time_out
    FROM (
        SELECT
            v.datework AS gamingday,
            v.membership,
            MIN(COALESCE(v.time_in, v.created)) AS visittime,
            MIN(v.time_in) AS time_in,
            MAX(v.time_out) AS time_out
        FROM bronze.person_visits_raw v
        WHERE
            v.datework >= p_from_date
            AND v.datework <= p_to_date
            AND v.membership IS NOT NULL
            AND v.membership <> 5
        GROUP BY
            v.datework, v.membership

        UNION ALL

        SELECT
            ds.gamingday,
            ds.playerid AS membership,
            MIN(ds.starttimelocal) AS visittime,
            MIN(ds.starttimelocal) AS time_in,
            MAX(ds.endtimelocal) AS time_out
        FROM bronze.drgt_sessions_raw ds
        WHERE
            ds.gamingday >= p_from_date
            AND ds.gamingday <= p_to_date
            AND ds.playerid IS NOT NULL
            AND ds.playerid <> 5
            AND NOT EXISTS (
                SELECT 1 FROM bronze.person_visits_raw v2
                WHERE v2.datework = ds.gamingday
                  AND v2.membership = ds.playerid
                  AND v2.datework >= p_from_date
                  AND v2.datework <= p_to_date
            )
        GROUP BY ds.gamingday, ds.playerid
    ) combined
),
agent_at_visit AS (
    SELECT
        vb.gamingday,
        vb.membership,
        COALESCE(
            (
                SELECT ap2.idagent
                FROM bronze.manage_agents_players_raw ap2
                WHERE ap2.membership = vb.membership
                  AND ap2.datechange <= vb.visittime
                ORDER BY ap2.datechange DESC, ap2.created DESC
                LIMIT 1
            ),
            NULLIF(p0.idagent, 0)
        ) AS idagent
    FROM visits_base vb
    LEFT JOIN bronze.person_players_raw p0
        ON p0.membership = vb.membership
),
money_tx AS (
    SELECT
        vt.datework AS gamingday,
        vt.timeoper,
        vt.membership,
        vt.idoper,
        vt.idaccount,
        CASE WHEN vt.directionoper = 1 THEN ABS(COALESCE(vt.totalmoneyue, 0.0)) ELSE 0.0 END::numeric(19,4) AS inamt,
        CASE WHEN vt.directionoper = -1 THEN ABS(COALESCE(vt.totalmoneyue, 0.0)) ELSE 0.0 END::numeric(19,4) AS outamtabs,
        COALESCE(vt.totalmoneyue, 0.0)::numeric(19,4) AS totalmoneyue,
        COALESCE(tc.deposit, 0.0)::numeric(19,4) AS deposit
    FROM bronze.cashdesk_transactions_raw vt
    LEFT JOIN bronze.casino_transactions_calculated_raw tc
           ON tc.idoper = vt.idoper
    WHERE
        vt.datework >= p_from_date AND vt.datework <= p_to_date
        AND vt.membership IS NOT NULL
        AND vt.membership <> 5
        AND vt.idaccount IN (121,703,802,803)
        AND COALESCE(vt.isdeleted, false) = false
        AND COALESCE(vt.iscalculatedindrop, false) = true
),
drop_events AS (
    SELECT
        gamingday,
        membership,
        timeoper,
        idoper,
        CASE WHEN idaccount IN (121,703) THEN inamt ELSE 0.0 END::numeric(19,4) AS buyamt,
        CASE WHEN idaccount = 121 THEN outamtabs ELSE 0.0 END::numeric(19,4) AS outamt
    FROM money_tx
    WHERE idaccount IN (121,703)
),
drop_calc AS (
    SELECT
        de.*,
        SUM(de.outamt - de.buyamt) OVER (
            PARTITION BY de.gamingday, de.membership
            ORDER BY de.timeoper, de.idoper
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS s
    FROM drop_events de
),
drop_pool1 AS (
    SELECT
        x.*,
        (
            x.s - CASE WHEN x.mins > 0 THEN 0 ELSE x.mins END
        )::numeric(19,4) AS poolafterraw
    FROM (
        SELECT
            dc.*,
            MIN(dc.s) OVER (
                PARTITION BY dc.gamingday, dc.membership
                ORDER BY dc.timeoper, dc.idoper
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS mins
        FROM drop_calc dc
    ) x
),
drop_pool AS (
    SELECT
        p1.*,
        p1.poolafterraw AS poolafter,
        LAG(p1.poolafterraw, 1, 0.0) OVER (
            PARTITION BY p1.gamingday, p1.membership
            ORDER BY p1.timeoper, p1.idoper
        )::numeric(19,4) AS poolbefore
    FROM drop_pool1 p1
),
drop_clean AS (
    SELECT
        gamingday,
        membership,
        SUM(
            CASE
                WHEN buyamt > 0 THEN
                    buyamt - (
                        CASE
                            WHEN poolbefore > 0
                                THEN CASE WHEN buyamt >= poolbefore THEN poolbefore ELSE buyamt END
                            ELSE 0.0
                        END
                    )
                ELSE 0.0
            END
        )::numeric(19,4) AS totaldrop_clean
    FROM drop_pool
    GROUP BY gamingday, membership
),
money_day AS (
    SELECT
        gamingday,
        membership,
        SUM(CASE WHEN idaccount = 121 THEN inamt ELSE 0.0 END)::numeric(19,4) AS cashdesk_in,
        SUM(CASE WHEN idaccount = 121 THEN outamtabs ELSE 0.0 END)::numeric(19,4) AS cashdesk_out,
        SUM(CASE WHEN idaccount = 703 THEN inamt ELSE 0.0 END)::numeric(19,4) AS tablecash_in,
        SUM(CASE WHEN idaccount IN (121,703) THEN inamt ELSE 0.0 END)::numeric(19,4) AS totalexchange_in,
        (
            SUM(CASE WHEN idaccount IN (121,703) THEN inamt ELSE 0.0 END)
            - SUM(CASE WHEN idaccount = 121 THEN outamtabs ELSE 0.0 END)
        )::numeric(19,4) AS totalexchange_net,
        SUM(CASE WHEN idaccount = 802 THEN totalmoneyue ELSE 0.0 END)::numeric(19,4) AS agenttransfer_net,
        SUM(CASE WHEN idaccount = 803 THEN deposit ELSE 0.0 END)::numeric(19,4) AS privatedeposit_net,
        SUM(CASE WHEN idaccount = 803 AND deposit > 0 THEN deposit ELSE 0.0 END)::numeric(19,4) AS privatedeposit_add,
        SUM(CASE WHEN idaccount = 803 AND deposit < 0 THEN deposit ELSE 0.0 END)::numeric(19,4) AS privatedeposit_withdraw
    FROM money_tx
    GROUP BY gamingday, membership
),
sess AS (
    SELECT
        pt.datework::date AS gamingday,
        ps.timestart,
        pt.membership,
        ps.realdrop,
        ps.handhold,
        ps.cashout,
        ps.averbet
    FROM bronze.manage_player_sessions_raw ps
    JOIN bronze.casino_players_tracking_raw pt
      ON pt.idplayerstracking = ps.idplayerstracking
    WHERE
        pt.datework >= p_from_date
        AND pt.datework <= p_to_date
        AND pt.membership IS NOT NULL
        AND pt.membership <> 5
),
sess_day AS (
    SELECT
        gamingday,
        membership,
        COUNT(*)::bigint AS sessionscnt
    FROM sess
    GROUP BY gamingday, membership
),
system_drop AS (
    SELECT
        vt.datework AS gamingday,
        vt.membership,
        SUM(
            CASE
                WHEN vt.directionoper = 1
                 AND COALESCE(vt.iscalculatedindrop, false) = true
                 AND vt.idaccount IN (121,703)
                THEN ABS(COALESCE(vt.chipsue, 0.0))
                ELSE 0.0
            END
        )::numeric(19,4) AS systemdrop_in
    FROM bronze.cashdesk_transactions_raw vt
    WHERE
        vt.datework >= p_from_date AND vt.datework <= p_to_date
        AND vt.membership IS NOT NULL
        AND vt.membership <> 5
        AND COALESCE(vt.isdeleted, false) = false
    GROUP BY vt.datework, vt.membership
),
slot_day AS (
    SELECT
        ds.gamingday,
        ds.playerid AS membership,
        (SUM(COALESCE(ds.totalbet, 0.0)) / 100.0)::numeric(19,4)  AS slot_totalbet,
        (SUM(COALESCE(ds.cashbet, 0.0))  / 100.0)::numeric(19,4) AS slot_cashbet,
        (SUM(COALESCE(ds.totalout, 0.0)) / 100.0)::numeric(19,4) AS slot_totalout,
        (SUM(COALESCE(ds.win, 0.0))      / 100.0)::numeric(19,4) AS slot_win,
        (SUM(COALESCE(ds.nwl, 0.0))      / 100.0)::numeric(19,4) AS slot_nwl,
        (SUM(COALESCE(ds.billdrop, 0.0)) / 100.0)::numeric(19,4) AS slot_billdrop,
        SUM(COALESCE(ds.gamesplayed, 0.0))::numeric(19,4) AS slot_gamesplayed,
        COUNT(*)::bigint AS slot_sessions_cnt
    FROM bronze.drgt_sessions_raw ds
    WHERE ds.gamingday >= p_from_date AND ds.gamingday <= p_to_date
    GROUP BY ds.gamingday, ds.playerid
),
sess_time_day AS (
    SELECT
        pt.datework::date AS gamingday,
        pt.membership,
        SUM(
            CASE
                WHEN ps.timestart IS NULL OR ps.timefinish IS NULL THEN 0
                WHEN ps.timefinish < ps.timestart THEN 0
                ELSE EXTRACT(EPOCH FROM (ps.timefinish - ps.timestart)) / 60.0
            END
        )::numeric(19,4) AS minutes_played
    FROM bronze.manage_player_sessions_raw ps
    JOIN bronze.casino_players_tracking_raw pt
      ON pt.idplayerstracking = ps.idplayerstracking
    WHERE pt.datework >= p_from_date AND pt.datework <= p_to_date
      AND pt.membership IS NOT NULL AND pt.membership <> 5
      AND COALESCE(ps.isdeleted, false) = false
    GROUP BY pt.datework::date, pt.membership
),
tracking_sess AS (
    SELECT
        pt.datework::date AS gamingday,
        pt.membership,
        ps.idplayersession,
        SUM(CASE WHEN pdc.qntchips > 0 THEN pdc.qntchips * ch.valuechip ELSE 0 END)::numeric(19,4) AS floatin_ue,
        SUM(CASE WHEN pdc.qntchips < 0 THEN -pdc.qntchips * ch.valuechip ELSE 0 END)::numeric(19,4) AS floatout_ue,
        SUM(pdc.qntchips * ch.valuechip)::numeric(19,4) AS net_ue
    FROM bronze.manage_player_sessions_raw ps
    JOIN bronze.casino_players_tracking_raw pt
      ON pt.idplayerstracking = ps.idplayerstracking
    JOIN bronze.manage_player_session_details_raw psd
      ON psd.idplayersession = ps.idplayersession
    JOIN bronze.manage_player_session_detail_chips_raw pdc
      ON pdc.idplayersessiondetail = psd.idplayersessiondetail
    JOIN bronze.casino_chips_raw ch
      ON ch.idchip = pdc.idchip
    WHERE pt.datework >= p_from_date AND pt.datework <= p_to_date
      AND pt.membership IS NOT NULL AND pt.membership <> 5
      AND COALESCE(ps.isdeleted, false) = false
    GROUP BY pt.datework::date, pt.membership, ps.idplayersession
),
tracking_day AS (
    SELECT
        gamingday,
        membership,
        SUM(floatin_ue)::numeric(19,4)  AS tracking_floatin,
        SUM(floatout_ue)::numeric(19,4) AS tracking_floatout,
        SUM(net_ue)::numeric(19,4)      AS tracking_net
    FROM tracking_sess
    GROUP BY gamingday, membership
),
expense_tx AS (
    SELECT
        vt.datework AS gamingday,
        vt.membership,
        vt.idaccount,
        vt.directionoper,
        COALESCE(tm.summoney, 0.0)::numeric(19,4) AS summoney,
        COALESCE(vt.comment, '') AS comment
    FROM bronze.cashdesk_transactions_raw vt
    LEFT JOIN (
        SELECT idoper, SUM(summoney) AS summoney
        FROM bronze.casino_transaction_money_raw
        GROUP BY idoper
    ) tm ON tm.idoper = vt.idoper
    WHERE vt.datework >= p_from_date AND vt.datework <= p_to_date
      AND vt.membership IS NOT NULL AND vt.membership <> 5
      AND vt.idaccount IN (153, 151, 154, 641)
      AND COALESCE(vt.isdeleted, false) = false
),
expense_day AS (
    SELECT
        gamingday,
        membership,
        SUM(CASE WHEN idaccount = 641 THEN summoney ELSE 0.0 END)::numeric(19,4) AS expense_total,
        SUM(CASE WHEN idaccount = 641 AND (comment ILIKE '%Air Ticket%' OR comment ILIKE '%Flight%'
             OR comment ILIKE '%Fly Ticket%' OR comment ILIKE '%Air Tiket%'
             OR comment ILIKE '%Air Tichets%' OR comment ILIKE '%Air Tickat%'
             OR comment ILIKE '%Air Tickent%' OR comment ILIKE '%Ticket Cost%'
             OR comment ILIKE '%Tickets Cost%' OR comment ILIKE '%Ticket Payment%')
            THEN summoney ELSE 0.0 END)::numeric(19,4) AS expense_airtickets,
        SUM(CASE WHEN idaccount = 641 AND (comment ILIKE '%Discount%' OR comment ILIKE '%Discaunt%'
             OR comment ILIKE '%Discont%' OR comment ILIKE '%Discout%' OR comment ILIKE '%disccount%'
             OR comment ILIKE '%Dsicount%' OR comment ILIKE '%Difference%')
            THEN summoney ELSE 0.0 END)::numeric(19,4) AS expense_discount_plus,
        SUM(CASE WHEN idaccount = 641 AND (comment ILIKE '%Hotel%' OR comment ILIKE '%Room Payment%'
             OR comment ILIKE '%Acommodation%' OR comment ILIKE '%Accommodation%')
            THEN summoney ELSE 0.0 END)::numeric(19,4) AS expense_hotel,
        SUM(CASE WHEN idaccount = 641
             AND NOT (comment ILIKE '%Air Ticket%' OR comment ILIKE '%Flight%'
                      OR comment ILIKE '%Fly Ticket%' OR comment ILIKE '%Air Tiket%'
                      OR comment ILIKE '%Air Tichets%' OR comment ILIKE '%Air Tickat%'
                      OR comment ILIKE '%Air Tickent%' OR comment ILIKE '%Ticket Cost%'
                      OR comment ILIKE '%Tickets Cost%' OR comment ILIKE '%Ticket Payment%')
             AND NOT (comment ILIKE '%Discount%' OR comment ILIKE '%Discaunt%'
                      OR comment ILIKE '%Discont%' OR comment ILIKE '%Discout%'
                      OR comment ILIKE '%disccount%' OR comment ILIKE '%Dsicount%'
                      OR comment ILIKE '%Difference%')
             AND NOT (comment ILIKE '%Hotel%' OR comment ILIKE '%Room Payment%'
                      OR comment ILIKE '%Acommodation%' OR comment ILIKE '%Accommodation%')
            THEN summoney ELSE 0.0 END)::numeric(19,4) AS expense_other,
        SUM(CASE WHEN idaccount = 151 THEN summoney ELSE 0.0 END)::numeric(19,4) AS discount_lg,
        SUM(CASE WHEN idaccount = 154 THEN summoney ELSE 0.0 END)::numeric(19,4) AS discount_slot,
        SUM(CASE WHEN idaccount = 153 AND directionoper = 1  THEN summoney ELSE 0.0 END)::numeric(19,4) AS agent_credit_out,
        SUM(CASE WHEN idaccount = 153 AND directionoper = -1 THEN summoney ELSE 0.0 END)::numeric(19,4) AS agent_credit_void,
        SUM(CASE WHEN idaccount = 153 THEN summoney ELSE 0.0 END)::numeric(19,4) AS agent_credit_net
    FROM expense_tx
    GROUP BY gamingday, membership
),
day_rows AS (
    SELECT
        vb.gamingday,
        vb.membership,
        NULLIF(TRIM(pp.surname), '') AS surname,
        NULLIF(TRIM(pp.forename), '') AS forename,
        aa.idagent,
        CASE
            WHEN aa.idagent IS NULL THEN 'NO AGENT'
            WHEN COALESCE(a.nameagent, '') = '' THEN 'NO AGENT'
            WHEN a.nameagent = 'NO AGENT' THEN 'NO AGENT'
            ELSE a.nameagent
        END AS agentname,
        cc.namecountry AS citizenshipcountry,
        COALESCE(dc.totaldrop_clean, 0.0)::numeric(19,4) AS totaldrop_clean,
        COALESCE(sd.systemdrop_in, 0.0)::numeric(19,4) AS systemdrop_in,
        COALESCE(m.totalexchange_in, 0.0)::numeric(19,4) AS totalcash_in,
        COALESCE(m.totalexchange_net, 0.0)::numeric(19,4) AS totalcash_result,
        COALESCE(m.cashdesk_in, 0.0)::numeric(19,4) AS cashdesk_in,
        COALESCE(m.cashdesk_out, 0.0)::numeric(19,4) AS cashdesk_out,
        COALESCE(m.tablecash_in, 0.0)::numeric(19,4) AS tablecash_in,
        COALESCE(m.agenttransfer_net, 0.0)::numeric(19,4) AS agenttransfer_net,
        (-COALESCE(m.privatedeposit_add, 0.0))::numeric(19,4) AS junketdeposit_add,
        (-COALESCE(m.privatedeposit_withdraw, 0.0))::numeric(19,4) AS junketdeposit_withdraw,
        (-COALESCE(m.privatedeposit_net, 0.0))::numeric(19,4) AS junketdeposit_net,
        COALESCE(s.sessionscnt, 0)::bigint AS sessionscnt,
        COALESCE(st.minutes_played, 0.0)::numeric(19,4) AS minutes_played,
        COALESCE(sl.slot_totalbet, 0.0)::numeric(19,4)  AS slot_totalbet,
        COALESCE(sl.slot_cashbet, 0.0)::numeric(19,4)   AS slot_cashbet,
        COALESCE(sl.slot_totalout, 0.0)::numeric(19,4)  AS slot_totalout,
        COALESCE(sl.slot_win, 0.0)::numeric(19,4)       AS slot_win,
        COALESCE(sl.slot_nwl, 0.0)::numeric(19,4)       AS slot_nwl,
        COALESCE(sl.slot_billdrop, 0.0)::numeric(19,4)  AS slot_billdrop,
        COALESCE(sl.slot_gamesplayed, 0.0)::numeric(19,4) AS slot_gamesplayed,
        COALESCE(sl.slot_sessions_cnt, 0)::bigint        AS slot_sessions_cnt,
        COALESCE(td.tracking_floatin, 0.0)::numeric(19,4)  AS tracking_floatin,
        COALESCE(td.tracking_floatout, 0.0)::numeric(19,4) AS tracking_floatout,
        COALESCE(td.tracking_net, 0.0)::numeric(19,4)      AS tracking_net,
        COALESCE(ed.expense_total, 0.0)::numeric(19,4)      AS expense_total,
        COALESCE(ed.expense_airtickets, 0.0)::numeric(19,4) AS expense_airtickets,
        COALESCE(ed.expense_discount_plus, 0.0)::numeric(19,4) AS expense_discount_plus,
        COALESCE(ed.expense_hotel, 0.0)::numeric(19,4)      AS expense_hotel,
        COALESCE(ed.expense_other, 0.0)::numeric(19,4)      AS expense_other,
        COALESCE(ed.discount_lg, 0.0)::numeric(19,4)        AS discount_lg,
        COALESCE(ed.discount_slot, 0.0)::numeric(19,4)      AS discount_slot,
        COALESCE(ed.agent_credit_out, 0.0)::numeric(19,4)   AS agent_credit_out,
        COALESCE(ed.agent_credit_void, 0.0)::numeric(19,4)  AS agent_credit_void,
        COALESCE(ed.agent_credit_net, 0.0)::numeric(19,4)   AS agent_credit_net
    FROM visits_base vb
    LEFT JOIN agent_at_visit aa
        ON aa.gamingday = vb.gamingday
       AND aa.membership = vb.membership
    LEFT JOIN bronze.manage_agents_raw a
        ON a.idagent = aa.idagent
    LEFT JOIN bronze.person_players_raw pp
        ON pp.membership = vb.membership
    LEFT JOIN bronze.casino_countries_raw cc
        ON cc.idcountry = pp.idcountry
    LEFT JOIN money_day m
        ON m.gamingday = vb.gamingday
       AND m.membership = vb.membership
    LEFT JOIN sess_day s
        ON s.gamingday = vb.gamingday
       AND s.membership = vb.membership
    LEFT JOIN sess_time_day st
        ON st.gamingday = vb.gamingday
       AND st.membership = vb.membership
    LEFT JOIN drop_clean dc
        ON dc.gamingday = vb.gamingday
       AND dc.membership = vb.membership
    LEFT JOIN system_drop sd
        ON sd.gamingday = vb.gamingday
       AND sd.membership = vb.membership
    LEFT JOIN slot_day sl
        ON sl.gamingday = vb.gamingday
       AND sl.membership = vb.membership
    LEFT JOIN tracking_day td
        ON td.gamingday = vb.gamingday
       AND td.membership = vb.membership
    LEFT JOIN expense_day ed
        ON ed.gamingday = vb.gamingday
       AND ed.membership = vb.membership
)
SELECT *
FROM day_rows
WHERE p_agent_id IS NULL OR idagent = p_agent_id;
$$;

CREATE TABLE IF NOT EXISTS silver.fact_membership_day (
    gamingday               date NOT NULL,
    membership              bigint NOT NULL,
    surname                 text,
    forename                text,
    idagent                 int,
    agentname               text,
    citizenshipcountry      text,
    totaldrop_clean         numeric(19,4) NOT NULL,
    systemdrop_in           numeric(19,4) NOT NULL,
    totalcash_in            numeric(19,4) NOT NULL,
    totalcash_result        numeric(19,4) NOT NULL,
    cashdesk_in             numeric(19,4) NOT NULL,
    cashdesk_out            numeric(19,4) NOT NULL,
    tablecash_in            numeric(19,4) NOT NULL,
    agenttransfer_net       numeric(19,4) NOT NULL,
    junketdeposit_add       numeric(19,4) NOT NULL,
    junketdeposit_withdraw  numeric(19,4) NOT NULL,
    junketdeposit_net       numeric(19,4) NOT NULL,
    sessionscnt             bigint NOT NULL,
    minutes_played          numeric(19,4) NOT NULL DEFAULT 0,
    slot_totalbet           numeric(19,4) NOT NULL DEFAULT 0,
    slot_cashbet            numeric(19,4) NOT NULL DEFAULT 0,
    slot_totalout           numeric(19,4) NOT NULL DEFAULT 0,
    slot_win                numeric(19,4) NOT NULL DEFAULT 0,
    slot_nwl                numeric(19,4) NOT NULL DEFAULT 0,
    slot_billdrop           numeric(19,4) NOT NULL DEFAULT 0,
    slot_gamesplayed        numeric(19,4) NOT NULL DEFAULT 0,
    slot_sessions_cnt       bigint NOT NULL DEFAULT 0,
    tracking_floatin        numeric(19,4) NOT NULL DEFAULT 0,
    tracking_floatout       numeric(19,4) NOT NULL DEFAULT 0,
    tracking_net            numeric(19,4) NOT NULL DEFAULT 0,
    expense_total           numeric(19,4) NOT NULL DEFAULT 0,
    expense_airtickets      numeric(19,4) NOT NULL DEFAULT 0,
    expense_discount_plus   numeric(19,4) NOT NULL DEFAULT 0,
    expense_hotel           numeric(19,4) NOT NULL DEFAULT 0,
    expense_other           numeric(19,4) NOT NULL DEFAULT 0,
    discount_lg             numeric(19,4) NOT NULL DEFAULT 0,
    discount_slot           numeric(19,4) NOT NULL DEFAULT 0,
    agent_credit_out        numeric(19,4) NOT NULL DEFAULT 0,
    agent_credit_void       numeric(19,4) NOT NULL DEFAULT 0,
    agent_credit_net        numeric(19,4) NOT NULL DEFAULT 0,
    loaded_at_utc           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gamingday, membership)
);

ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS surname text;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS forename text;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS minutes_played numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS tracking_floatin numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS tracking_floatout numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS tracking_net numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_totalbet numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_cashbet numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_totalout numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_win numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_nwl numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_billdrop numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_gamesplayed numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS slot_sessions_cnt bigint NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS expense_total numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS expense_airtickets numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS expense_discount_plus numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS expense_hotel numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS expense_other numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS discount_lg numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS discount_slot numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS agent_credit_out numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS agent_credit_void numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE silver.fact_membership_day ADD COLUMN IF NOT EXISTS agent_credit_net numeric(19,4) NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_silver_fact_membership_day_mem
    ON silver.fact_membership_day(membership);

CREATE OR REPLACE FUNCTION silver.sp_load_fact_membership_day(
    p_from_date date,
    p_to_date   date,
    p_agent_id  int DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM silver.fact_membership_day
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date
      AND (p_agent_id IS NULL OR idagent = p_agent_id);

    INSERT INTO silver.fact_membership_day (
        gamingday,
        membership,
        surname,
        forename,
        idagent,
        agentname,
        citizenshipcountry,
        totaldrop_clean,
        systemdrop_in,
        totalcash_in,
        totalcash_result,
        cashdesk_in,
        cashdesk_out,
        tablecash_in,
        agenttransfer_net,
        junketdeposit_add,
        junketdeposit_withdraw,
        junketdeposit_net,
        sessionscnt,
        minutes_played,
        slot_totalbet,
        slot_cashbet,
        slot_totalout,
        slot_win,
        slot_nwl,
        slot_billdrop,
        slot_gamesplayed,
        slot_sessions_cnt,
        tracking_floatin,
        tracking_floatout,
        tracking_net,
        expense_total,
        expense_airtickets,
        expense_discount_plus,
        expense_hotel,
        expense_other,
        discount_lg,
        discount_slot,
        agent_credit_out,
        agent_credit_void,
        agent_credit_net
    )
    SELECT
        d.gamingday,
        d.membership,
        d.surname,
        d.forename,
        d.idagent,
        d.agentname,
        d.citizenshipcountry,
        d.totaldrop_clean,
        d.systemdrop_in,
        d.totalcash_in,
        d.totalcash_result,
        d.cashdesk_in,
        d.cashdesk_out,
        d.tablecash_in,
        d.agenttransfer_net,
        d.junketdeposit_add,
        d.junketdeposit_withdraw,
        d.junketdeposit_net,
        d.sessionscnt,
        d.minutes_played,
        d.slot_totalbet,
        d.slot_cashbet,
        d.slot_totalout,
        d.slot_win,
        d.slot_nwl,
        d.slot_billdrop,
        d.slot_gamesplayed,
        d.slot_sessions_cnt,
        d.tracking_floatin,
        d.tracking_floatout,
        d.tracking_net,
        d.expense_total,
        d.expense_airtickets,
        d.expense_discount_plus,
        d.expense_hotel,
        d.expense_other,
        d.discount_lg,
        d.discount_slot,
        d.agent_credit_out,
        d.agent_credit_void,
        d.agent_credit_net
    FROM silver.fn_membership_day(p_from_date, p_to_date, p_agent_id) d;
END;
$$;

CREATE TABLE IF NOT EXISTS silver.dim_game (
    idgame               int PRIMARY KEY,
    codegame             text,
    namegame             text,
    listorder_game       int,
    idbonussystem        int,
    nmbboxes             int,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.dim_table_type (
    idtabletype          int PRIMARY KEY,
    codetabletype        text,
    nametabletype        text,
    listorder_tabletype  int,
    idgametype           int,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.bridge_table_type_game (
    idtabletype          int NOT NULL,
    idgame               int NOT NULL,
    listorder            int,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idtabletype, idgame)
);

CREATE TABLE IF NOT EXISTS silver.dim_table (
    idtable              int PRIMARY KEY,
    idtabletype          int,
    nametable            text,
    snmbtable            text,
    codetable            text,
    isvirtual            boolean,
    listorder_table      int,
    idbonusgame          int,
    idbonussystem        int,
    ismarketing          boolean,
    idcurrency           int,
    mysteryguarantee     numeric(19,4),
    ratemystery          numeric(19,4),
    lowerlimitmystery    numeric(19,4),
    upperlimitmystery    numeric(19,4),
    minplayingbet        numeric(19,4),
    loaded_at_utc        timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS silver.dim_agent_group_scd (
    idagentgroup         int PRIMARY KEY,
    idagent              int,
    nameagent            text,
    nameagentgroup       text,
    datebegin            date,
    dateend              date,
    memoagentgroup       text,
    row_version          int,
    modified             timestamp,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_silver_agent_group_scd_agent_date
    ON silver.dim_agent_group_scd(idagent, datebegin, dateend);

CREATE TABLE IF NOT EXISTS silver.fact_fx_rate_daily (
    idcasino             int NOT NULL,
    datechange           date NOT NULL,
    idcurrency           int NOT NULL,
    idcurrencyexchrate   int,
    exchrate             numeric(19,8),
    row_version          int,
    created              timestamp,
    modified             timestamp,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idcasino, datechange, idcurrency)
);

CREATE TABLE IF NOT EXISTS silver.fact_player_session (
    idplayerstracking    bigint NOT NULL,
    timestart            timestamp NOT NULL,
    gamingday            date NOT NULL,
    membership           bigint,
    idplayersession      bigint,
    timefinish           timestamp,
    idtable              int,
    idgame               int,
    idslot               int,
    idagent              int,
    idagentgroup         int,
    realdrop             numeric(19,4),
    chipsin              numeric(19,4),
    chipsout             numeric(19,4),
    handhold             numeric(19,4),
    cashout              numeric(19,4),
    averbet              numeric(19,4),
    loaded_at_utc        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idplayerstracking, timestart)
);
CREATE INDEX IF NOT EXISTS idx_silver_fact_player_session_day
    ON silver.fact_player_session(gamingday);
CREATE INDEX IF NOT EXISTS idx_silver_fact_player_session_table_game
    ON silver.fact_player_session(gamingday, idtable, idgame);
CREATE INDEX IF NOT EXISTS idx_silver_fact_player_session_agent_group
    ON silver.fact_player_session(gamingday, idagentgroup);

CREATE OR REPLACE FUNCTION silver.sp_load_reference_dimensions()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO silver.dim_game (
        idgame, codegame, namegame, listorder_game, idbonussystem, nmbboxes
    )
    SELECT
        g.idgame,
        g.codegame,
        g.namegame,
        g.listorder_game,
        g.idbonussystem,
        g.nmbboxes
    FROM bronze.casino_games_ref_raw g
    ON CONFLICT (idgame) DO UPDATE SET
        codegame = EXCLUDED.codegame,
        namegame = EXCLUDED.namegame,
        listorder_game = EXCLUDED.listorder_game,
        idbonussystem = EXCLUDED.idbonussystem,
        nmbboxes = EXCLUDED.nmbboxes,
        loaded_at_utc = now();

    INSERT INTO silver.dim_table_type (
        idtabletype, codetabletype, nametabletype, listorder_tabletype, idgametype
    )
    SELECT
        tt.idtabletype,
        tt.codetabletype,
        tt.nametabletype,
        tt.listorder_tabletype,
        tt.idgametype
    FROM bronze.casino_table_types_raw tt
    ON CONFLICT (idtabletype) DO UPDATE SET
        codetabletype = EXCLUDED.codetabletype,
        nametabletype = EXCLUDED.nametabletype,
        listorder_tabletype = EXCLUDED.listorder_tabletype,
        idgametype = EXCLUDED.idgametype,
        loaded_at_utc = now();

    INSERT INTO silver.bridge_table_type_game (
        idtabletype, idgame, listorder
    )
    SELECT
        btg.idtabletype,
        btg.idgame,
        btg.listorder
    FROM bronze.casino_table_types_games_raw btg
    ON CONFLICT (idtabletype, idgame) DO UPDATE SET
        listorder = EXCLUDED.listorder,
        loaded_at_utc = now();

    INSERT INTO silver.dim_table (
        idtable, idtabletype, nametable, snmbtable, codetable,
        isvirtual, listorder_table, idbonusgame, idbonussystem, ismarketing,
        idcurrency, mysteryguarantee, ratemystery, lowerlimitmystery,
        upperlimitmystery, minplayingbet
    )
    SELECT
        t.idtable,
        t.idtabletype,
        t.nametable,
        t.snmbtable,
        t.codetable,
        t.isvirtual,
        t.listorder_table,
        t.idbonusgame,
        t.idbonussystem,
        t.ismarketing,
        t.idcurrency,
        t.mysteryguarantee,
        t.ratemystery,
        t.lowerlimitmystery,
        t.upperlimitmystery,
        t.minplayingbet
    FROM bronze.casino_tables_ref_raw t
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
        loaded_at_utc = now();

    INSERT INTO silver.dim_agent_group_scd (
        idagentgroup, idagent, nameagent, nameagentgroup,
        datebegin, dateend, memoagentgroup, row_version, modified
    )
    SELECT
        ag.idagentgroup,
        ag.idagent,
        a.nameagent,
        ag.nameagentgroup,
        ag.datebegin,
        ag.dateend,
        ag.memoagentgroup,
        ag.row_version,
        ag.modified
    FROM bronze.manage_agent_groups_raw ag
    LEFT JOIN bronze.manage_agents_raw a
      ON a.idagent = ag.idagent
    ON CONFLICT (idagentgroup) DO UPDATE SET
        idagent = EXCLUDED.idagent,
        nameagent = EXCLUDED.nameagent,
        nameagentgroup = EXCLUDED.nameagentgroup,
        datebegin = EXCLUDED.datebegin,
        dateend = EXCLUDED.dateend,
        memoagentgroup = EXCLUDED.memoagentgroup,
        row_version = EXCLUDED.row_version,
        modified = EXCLUDED.modified,
        loaded_at_utc = now();
END;
$$;

CREATE OR REPLACE FUNCTION silver.fn_player_session(
    p_from_date date,
    p_to_date   date
)
RETURNS TABLE (
    idplayerstracking    bigint,
    timestart            timestamp,
    gamingday            date,
    membership           bigint,
    idplayersession      bigint,
    timefinish           timestamp,
    idtable              int,
    idgame               int,
    idslot               int,
    idagent              int,
    idagentgroup         int,
    realdrop             numeric(19,4),
    chipsin              numeric(19,4),
    chipsout             numeric(19,4),
    handhold             numeric(19,4),
    cashout              numeric(19,4),
    averbet              numeric(19,4)
)
LANGUAGE sql
AS $$
SELECT
    ps.idplayerstracking,
    ps.timestart,
    COALESCE(pt.datework, ps.timestart::date) AS gamingday,
    pt.membership,
    ps.idplayersession,
    ps.timefinish,
    ps.idtable,
    ps.idgame,
    ps.idslot,
    pp.idagent,
    ag_match.idagentgroup,
    COALESCE(ps.realdrop, 0.0)::numeric(19,4) AS realdrop,
    COALESCE(ps.chipsin, 0.0)::numeric(19,4) AS chipsin,
    COALESCE(ps.chipsout, 0.0)::numeric(19,4) AS chipsout,
    COALESCE(ps.handhold, 0.0)::numeric(19,4) AS handhold,
    COALESCE(ps.cashout, 0.0)::numeric(19,4) AS cashout,
    COALESCE(ps.averbet, 0.0)::numeric(19,4) AS averbet
FROM bronze.manage_player_sessions_raw ps
JOIN bronze.casino_players_tracking_raw pt
  ON pt.idplayerstracking = ps.idplayerstracking
LEFT JOIN bronze.person_players_raw pp
  ON pp.membership = pt.membership
LEFT JOIN LATERAL (
    SELECT ag.idagentgroup
    FROM bronze.manage_agent_groups_raw ag
    WHERE ag.idagent = pp.idagent
      AND COALESCE(pt.datework, ps.timestart::date) >= ag.datebegin
      AND COALESCE(pt.datework, ps.timestart::date) <= COALESCE(ag.dateend, DATE '2999-12-31')
    ORDER BY ag.datebegin DESC, ag.modified DESC
    LIMIT 1
) ag_match ON true
WHERE COALESCE(pt.datework, ps.timestart::date) >= p_from_date
  AND COALESCE(pt.datework, ps.timestart::date) <= p_to_date
  AND pt.membership IS NOT NULL
  AND pt.membership <> 5
  AND COALESCE(ps.isdeleted, false) = false;
$$;

CREATE OR REPLACE FUNCTION silver.sp_load_fact_fx_rate_daily(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM silver.fact_fx_rate_daily
    WHERE datechange >= p_from_date
      AND datechange <= p_to_date;

    INSERT INTO silver.fact_fx_rate_daily (
        idcasino, datechange, idcurrency, idcurrencyexchrate,
        exchrate, row_version, created, modified
    )
    SELECT
        r.idcasino,
        r.datechange,
        r.idcurrency,
        r.idcurrencyexchrate,
        r.exchrate,
        r.row_version,
        r.created,
        r.modified
    FROM bronze.casino_currency_exch_rates_raw r
    WHERE r.datechange >= p_from_date
      AND r.datechange <= p_to_date;
END;
$$;

CREATE OR REPLACE FUNCTION silver.sp_load_fact_player_session(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM silver.fact_player_session
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date;

    INSERT INTO silver.fact_player_session (
        idplayerstracking,
        timestart,
        gamingday,
        membership,
        idplayersession,
        timefinish,
        idtable,
        idgame,
        idslot,
        idagent,
        idagentgroup,
        realdrop,
        chipsin,
        chipsout,
        handhold,
        cashout,
        averbet
    )
    SELECT
        s.idplayerstracking,
        s.timestart,
        s.gamingday,
        s.membership,
        s.idplayersession,
        s.timefinish,
        s.idtable,
        s.idgame,
        s.idslot,
        s.idagent,
        s.idagentgroup,
        s.realdrop,
        s.chipsin,
        s.chipsout,
        s.handhold,
        s.cashout,
        s.averbet
    FROM silver.fn_player_session(p_from_date, p_to_date) s;
END;
$$;

CREATE OR REPLACE FUNCTION silver.sp_load_reference_and_facts(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM silver.sp_load_reference_dimensions();
    PERFORM silver.sp_load_fact_fx_rate_daily(p_from_date, p_to_date);
    PERFORM silver.sp_load_fact_player_session(p_from_date, p_to_date);
END;
$$;