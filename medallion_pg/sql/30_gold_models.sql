CREATE TABLE IF NOT EXISTS gold.fact_membership_day (
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

ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS surname text;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS forename text;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS minutes_played numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS tracking_floatin numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS tracking_floatout numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS tracking_net numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_totalbet numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_cashbet numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_totalout numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_win numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_nwl numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_billdrop numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_gamesplayed numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS slot_sessions_cnt bigint NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS expense_total numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS expense_airtickets numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS expense_discount_plus numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS expense_hotel numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS expense_other numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS discount_lg numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS discount_slot numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS agent_credit_out numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS agent_credit_void numeric(19,4) NOT NULL DEFAULT 0;
ALTER TABLE gold.fact_membership_day ADD COLUMN IF NOT EXISTS agent_credit_net numeric(19,4) NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION gold.sp_load_fact_membership_day(
    p_from_date date,
    p_to_date   date,
    p_agent_id  int DEFAULT NULL
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM silver.sp_load_fact_membership_day(p_from_date, p_to_date, p_agent_id);

    DELETE FROM gold.fact_membership_day
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date
      AND (p_agent_id IS NULL OR idagent = p_agent_id);

    INSERT INTO gold.fact_membership_day (
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
        FROM silver.fact_membership_day d
        WHERE d.gamingday >= p_from_date
            AND d.gamingday <= p_to_date
            AND (p_agent_id IS NULL OR d.idagent = p_agent_id);
END;
$$;

DROP FUNCTION IF EXISTS gold.fn_membership_period(date, date, int);
CREATE OR REPLACE FUNCTION gold.fn_membership_period(
    p_from_date date,
    p_to_date   date,
    p_agent_id  int DEFAULT NULL
)
RETURNS TABLE (
    membership              bigint,
    surname                 text,
    forename                text,
    idagent                 int,
    agentname               text,
    citizenshipcountry      text,
    visitsdays              bigint,
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
SELECT
    d.membership,
    MAX(d.surname) AS surname,
    MAX(d.forename) AS forename,
    MAX(d.idagent) AS idagent,
    MAX(d.agentname) AS agentname,
    MAX(d.citizenshipcountry) AS citizenshipcountry,
    COUNT(*)::bigint AS visitsdays,
    SUM(d.totaldrop_clean)::numeric(19,4) AS totaldrop_clean,
    SUM(d.systemdrop_in)::numeric(19,4) AS systemdrop_in,
    SUM(d.totalcash_in)::numeric(19,4) AS totalcash_in,
    SUM(d.totalcash_result)::numeric(19,4) AS totalcash_result,
    SUM(d.cashdesk_in)::numeric(19,4) AS cashdesk_in,
    SUM(d.cashdesk_out)::numeric(19,4) AS cashdesk_out,
    SUM(d.tablecash_in)::numeric(19,4) AS tablecash_in,
    SUM(d.agenttransfer_net)::numeric(19,4) AS agenttransfer_net,
    SUM(d.junketdeposit_add)::numeric(19,4) AS junketdeposit_add,
    SUM(d.junketdeposit_withdraw)::numeric(19,4) AS junketdeposit_withdraw,
    SUM(d.junketdeposit_net)::numeric(19,4) AS junketdeposit_net,
    SUM(d.sessionscnt)::bigint AS sessionscnt,
    SUM(d.minutes_played)::numeric(19,4) AS minutes_played,
    SUM(d.slot_totalbet)::numeric(19,4) AS slot_totalbet,
    SUM(d.slot_cashbet)::numeric(19,4) AS slot_cashbet,
    SUM(d.slot_totalout)::numeric(19,4) AS slot_totalout,
    SUM(d.slot_win)::numeric(19,4) AS slot_win,
    SUM(d.slot_nwl)::numeric(19,4) AS slot_nwl,
    SUM(d.slot_billdrop)::numeric(19,4) AS slot_billdrop,
    SUM(d.slot_gamesplayed)::numeric(19,4) AS slot_gamesplayed,
    SUM(d.slot_sessions_cnt)::bigint AS slot_sessions_cnt,
    SUM(d.tracking_floatin)::numeric(19,4) AS tracking_floatin,
    SUM(d.tracking_floatout)::numeric(19,4) AS tracking_floatout,
    SUM(d.tracking_net)::numeric(19,4) AS tracking_net,
    SUM(d.expense_total)::numeric(19,4) AS expense_total,
    SUM(d.expense_airtickets)::numeric(19,4) AS expense_airtickets,
    SUM(d.expense_discount_plus)::numeric(19,4) AS expense_discount_plus,
    SUM(d.expense_hotel)::numeric(19,4) AS expense_hotel,
    SUM(d.expense_other)::numeric(19,4) AS expense_other,
    SUM(d.discount_lg)::numeric(19,4) AS discount_lg,
    SUM(d.discount_slot)::numeric(19,4) AS discount_slot,
    SUM(d.agent_credit_out)::numeric(19,4) AS agent_credit_out,
    SUM(d.agent_credit_void)::numeric(19,4) AS agent_credit_void,
    SUM(d.agent_credit_net)::numeric(19,4) AS agent_credit_net
FROM gold.fact_membership_day d
WHERE d.gamingday >= p_from_date
    AND d.gamingday <= p_to_date
    AND (p_agent_id IS NULL OR d.idagent = p_agent_id)
GROUP BY d.membership;
$$;

CREATE TABLE IF NOT EXISTS gold.fact_session_day_by_table_game (
    gamingday            date NOT NULL,
    idtable              int NOT NULL,
    idgame               int NOT NULL,
    sessionscnt          bigint NOT NULL,
    memberscnt           bigint NOT NULL,
    total_realdrop       numeric(19,4) NOT NULL,
    total_handhold       numeric(19,4) NOT NULL,
    total_cashout        numeric(19,4) NOT NULL,
    total_chipsin        numeric(19,4) NOT NULL,
    total_chipsout       numeric(19,4) NOT NULL,
    avg_averbet          numeric(19,4) NOT NULL,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gamingday, idtable, idgame)
);

CREATE TABLE IF NOT EXISTS gold.fact_session_day_by_agent_group (
    gamingday            date NOT NULL,
    idagentgroup         int NOT NULL,
    idagent              int,
    sessionscnt          bigint NOT NULL,
    memberscnt           bigint NOT NULL,
    total_realdrop       numeric(19,4) NOT NULL,
    total_handhold       numeric(19,4) NOT NULL,
    total_cashout        numeric(19,4) NOT NULL,
    total_chipsin        numeric(19,4) NOT NULL,
    total_chipsout       numeric(19,4) NOT NULL,
    avg_averbet          numeric(19,4) NOT NULL,
    loaded_at_utc        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gamingday, idagentgroup)
);

CREATE OR REPLACE FUNCTION gold.sp_load_fact_session_day_by_table_game(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM gold.fact_session_day_by_table_game
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date;

    INSERT INTO gold.fact_session_day_by_table_game (
        gamingday,
        idtable,
        idgame,
        sessionscnt,
        memberscnt,
        total_realdrop,
        total_handhold,
        total_cashout,
        total_chipsin,
        total_chipsout,
        avg_averbet
    )
    SELECT
        s.gamingday,
        COALESCE(s.idtable, -1) AS idtable,
        COALESCE(s.idgame, -1) AS idgame,
        COUNT(*)::bigint AS sessionscnt,
        COUNT(DISTINCT s.membership)::bigint AS memberscnt,
        SUM(COALESCE(s.realdrop, 0.0))::numeric(19,4) AS total_realdrop,
        SUM(COALESCE(s.handhold, 0.0))::numeric(19,4) AS total_handhold,
        SUM(COALESCE(s.cashout, 0.0))::numeric(19,4) AS total_cashout,
        SUM(COALESCE(s.chipsin, 0.0))::numeric(19,4) AS total_chipsin,
        SUM(COALESCE(s.chipsout, 0.0))::numeric(19,4) AS total_chipsout,
        COALESCE(AVG(COALESCE(s.averbet, 0.0)), 0.0)::numeric(19,4) AS avg_averbet
    FROM silver.fact_player_session s
    WHERE s.gamingday >= p_from_date
      AND s.gamingday <= p_to_date
    GROUP BY s.gamingday, COALESCE(s.idtable, -1), COALESCE(s.idgame, -1);
END;
$$;

CREATE OR REPLACE FUNCTION gold.sp_load_fact_session_day_by_agent_group(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM gold.fact_session_day_by_agent_group
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date;

    INSERT INTO gold.fact_session_day_by_agent_group (
        gamingday,
        idagentgroup,
        idagent,
        sessionscnt,
        memberscnt,
        total_realdrop,
        total_handhold,
        total_cashout,
        total_chipsin,
        total_chipsout,
        avg_averbet
    )
    SELECT
        s.gamingday,
        COALESCE(s.idagentgroup, -1) AS idagentgroup,
        MAX(s.idagent) AS idagent,
        COUNT(*)::bigint AS sessionscnt,
        COUNT(DISTINCT s.membership)::bigint AS memberscnt,
        SUM(COALESCE(s.realdrop, 0.0))::numeric(19,4) AS total_realdrop,
        SUM(COALESCE(s.handhold, 0.0))::numeric(19,4) AS total_handhold,
        SUM(COALESCE(s.cashout, 0.0))::numeric(19,4) AS total_cashout,
        SUM(COALESCE(s.chipsin, 0.0))::numeric(19,4) AS total_chipsin,
        SUM(COALESCE(s.chipsout, 0.0))::numeric(19,4) AS total_chipsout,
        COALESCE(AVG(COALESCE(s.averbet, 0.0)), 0.0)::numeric(19,4) AS avg_averbet
    FROM silver.fact_player_session s
    WHERE s.gamingday >= p_from_date
      AND s.gamingday <= p_to_date
    GROUP BY s.gamingday, COALESCE(s.idagentgroup, -1);
END;
$$;

-- ============================================================
-- Expense star schema for Power BI
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_expense_type (
    expense_type_key    int PRIMARY KEY,
    account_id          int NOT NULL,
    direction           int,                 -- 1=In, -1=Out, NULL=any
    expense_group       text NOT NULL,       -- Operating Expense, Agent Credit, Discount
    expense_type        text NOT NULL,       -- Air Tickets, Discount+, Hotel, Other, Agent Out, Agent Void, LG Discount, Slot Discount
    sort_order          int NOT NULL DEFAULT 0
);

-- Seed the dimension (idempotent)
INSERT INTO gold.dim_expense_type (expense_type_key, account_id, direction, expense_group, expense_type, sort_order)
VALUES
    (1, 641, NULL, 'Operating Expense', 'Air Tickets',   1),
    (2, 641, NULL, 'Operating Expense', 'Discount+',     2),
    (3, 641, NULL, 'Operating Expense', 'Hotel',          3),
    (4, 641, NULL, 'Operating Expense', 'Other',          4),
    (5, 153,    1, 'Agent Credit',      'Agent Out',      5),
    (6, 153,   -1, 'Agent Credit',      'Agent Void',     6),
    (7, 151, NULL, 'Discount',          'LG Discount',    7),
    (8, 154, NULL, 'Discount',          'Slot Discount',  8)
ON CONFLICT (expense_type_key) DO UPDATE SET
    account_id    = EXCLUDED.account_id,
    direction     = EXCLUDED.direction,
    expense_group = EXCLUDED.expense_group,
    expense_type  = EXCLUDED.expense_type,
    sort_order    = EXCLUDED.sort_order;

CREATE TABLE IF NOT EXISTS gold.fact_player_expenses (
    idoper              bigint NOT NULL,
    expense_type_key    int NOT NULL REFERENCES gold.dim_expense_type(expense_type_key),
    gamingday           date NOT NULL,
    membership          bigint NOT NULL,
    idagent             int,
    agentname           text,
    citizenshipcountry  text,
    amount              numeric(19,4) NOT NULL,
    comment             text,
    idarticle           int,
    article_name        text,
    loaded_at_utc       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idoper, expense_type_key)
);
CREATE INDEX IF NOT EXISTS idx_fact_player_expenses_day_mem
    ON gold.fact_player_expenses(gamingday, membership);
CREATE INDEX IF NOT EXISTS idx_fact_player_expenses_type
    ON gold.fact_player_expenses(expense_type_key);

CREATE OR REPLACE FUNCTION gold.sp_load_fact_player_expenses(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM gold.fact_player_expenses
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date;

    INSERT INTO gold.fact_player_expenses (
        idoper, expense_type_key, gamingday, membership,
        idagent, agentname, citizenshipcountry,
        amount, comment, idarticle, article_name
    )
    SELECT
        vt.idoper,
        CASE
            -- Account 641: Operating Expenses — categorise by comment
            WHEN vt.idaccount = 641 AND (
                 vt.comment ILIKE '%Air Ticket%' OR vt.comment ILIKE '%Flight%'
                 OR vt.comment ILIKE '%Fly Ticket%' OR vt.comment ILIKE '%Air Tiket%'
                 OR vt.comment ILIKE '%Air Tichets%' OR vt.comment ILIKE '%Air Tickat%'
                 OR vt.comment ILIKE '%Air Tickent%' OR vt.comment ILIKE '%Ticket Cost%'
                 OR vt.comment ILIKE '%Tickets Cost%' OR vt.comment ILIKE '%Ticket Payment%')
                THEN 1   -- Air Tickets
            WHEN vt.idaccount = 641 AND (
                 vt.comment ILIKE '%Discount%' OR vt.comment ILIKE '%Discaunt%'
                 OR vt.comment ILIKE '%Discont%' OR vt.comment ILIKE '%Discout%'
                 OR vt.comment ILIKE '%disccount%' OR vt.comment ILIKE '%Dsicount%'
                 OR vt.comment ILIKE '%Difference%')
                THEN 2   -- Discount+
            WHEN vt.idaccount = 641 AND (
                 vt.comment ILIKE '%Hotel%' OR vt.comment ILIKE '%Room Payment%'
                 OR vt.comment ILIKE '%Acommodation%' OR vt.comment ILIKE '%Accommodation%')
                THEN 3   -- Hotel
            WHEN vt.idaccount = 641
                THEN 4   -- Other
            -- Account 153: Agent Credits
            WHEN vt.idaccount = 153 AND vt.directionoper = 1
                THEN 5   -- Agent Out
            WHEN vt.idaccount = 153 AND vt.directionoper = -1
                THEN 6   -- Agent Void
            -- Discounts
            WHEN vt.idaccount = 151 THEN 7   -- LG Discount
            WHEN vt.idaccount = 154 THEN 8   -- Slot Discount
        END AS expense_type_key,
        vt.datework AS gamingday,
        vt.membership,
        COALESCE(
            (SELECT ap.idagent
             FROM bronze.manage_agents_players_raw ap
             WHERE ap.membership = vt.membership
               AND ap.datechange <= vt.timeoper
             ORDER BY ap.datechange DESC, ap.created DESC
             LIMIT 1),
            NULLIF(pp.idagent, 0)
        ) AS idagent,
        CASE
            WHEN COALESCE(
                (SELECT ap.idagent
                 FROM bronze.manage_agents_players_raw ap
                 WHERE ap.membership = vt.membership
                   AND ap.datechange <= vt.timeoper
                 ORDER BY ap.datechange DESC, ap.created DESC
                 LIMIT 1),
                NULLIF(pp.idagent, 0)
            ) IS NOT NULL
            THEN COALESCE(ag.nameagent, 'NO AGENT')
            ELSE 'NO AGENT'
        END AS agentname,
        cc.namecountry AS citizenshipcountry,
        COALESCE(tm.summoney, 0.0)::numeric(19,4) AS amount,
        vt.comment,
        vt.idarticle,
        ar.namearticle AS article_name
    FROM bronze.cashdesk_transactions_raw vt
    LEFT JOIN (
        SELECT idoper, SUM(summoney) AS summoney
        FROM bronze.casino_transaction_money_raw
        GROUP BY idoper
    ) tm ON tm.idoper = vt.idoper
    LEFT JOIN bronze.person_players_raw pp
        ON pp.membership = vt.membership
    LEFT JOIN bronze.casino_countries_raw cc
        ON cc.idcountry = pp.idcountry
    LEFT JOIN bronze.manage_agents_raw ag
        ON ag.idagent = COALESCE(
            (SELECT ap2.idagent
             FROM bronze.manage_agents_players_raw ap2
             WHERE ap2.membership = vt.membership
               AND ap2.datechange <= vt.timeoper
             ORDER BY ap2.datechange DESC, ap2.created DESC
             LIMIT 1),
            NULLIF(pp.idagent, 0)
        )
    LEFT JOIN bronze.cashdesk_articles_raw ar
        ON ar.idarticle = vt.idarticle
    WHERE vt.datework >= p_from_date
      AND vt.datework <= p_to_date
      AND vt.membership IS NOT NULL
      AND vt.membership <> 5
      AND vt.idaccount IN (641, 153, 151, 154)
      AND COALESCE(vt.isdeleted, false) = false;
END;
$$;

-- ============================================================
-- Bonus star schema for Power BI
-- ============================================================

CREATE TABLE IF NOT EXISTS gold.dim_bonus_indicator (
    idbonusindicator    int PRIMARY KEY,
    codebonusindicator  text NOT NULL,
    namebonusindicator  text NOT NULL
);

-- Seed from bronze (idempotent)
INSERT INTO gold.dim_bonus_indicator (idbonusindicator, codebonusindicator, namebonusindicator)
SELECT bi.idbonusindicator, bi.codebonusindicator, bi.namebonusindicator
FROM bronze.promo_bonus_indicators_raw bi
ON CONFLICT (idbonusindicator) DO UPDATE SET
    codebonusindicator = EXCLUDED.codebonusindicator,
    namebonusindicator = EXCLUDED.namebonusindicator;

CREATE TABLE IF NOT EXISTS gold.fact_player_bonuses (
    idplayerbonus       int PRIMARY KEY,
    gamingday           date NOT NULL,
    timeoper            timestamp,
    membership          bigint NOT NULL,
    idagent             int,
    agentname           text,
    citizenshipcountry  text,
    typeoper            int NOT NULL,          -- 1=earned, -1=reversed
    idbonusindicator    int,
    idgame              int,
    gamename            text,
    sumbonuses          numeric(19,4) NOT NULL DEFAULT 0,
    costbonuses         numeric(19,4) NOT NULL DEFAULT 0,
    multiplierloyalty   numeric(19,4),          -- house edge %
    hours               numeric(19,4),
    handsperhour        int,
    averbet             numeric(19,4),
    percentadt          numeric(19,4),          -- ADT percent (typically 0.20)
    adt                 numeric(19,4),          -- calculated: multiplier * hours * handsPerHour * avgBet
    comment             text,
    isdeleted           boolean NOT NULL DEFAULT false,
    loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fact_player_bonuses_day_mem
    ON gold.fact_player_bonuses(gamingday, membership);
CREATE INDEX IF NOT EXISTS idx_fact_player_bonuses_indicator
    ON gold.fact_player_bonuses(idbonusindicator);
CREATE INDEX IF NOT EXISTS idx_fact_player_bonuses_game
    ON gold.fact_player_bonuses(idgame);

CREATE OR REPLACE FUNCTION gold.sp_load_fact_player_bonuses(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- Refresh dim_bonus_indicator from bronze
    INSERT INTO gold.dim_bonus_indicator (idbonusindicator, codebonusindicator, namebonusindicator)
    SELECT bi.idbonusindicator, bi.codebonusindicator, bi.namebonusindicator
    FROM bronze.promo_bonus_indicators_raw bi
    ON CONFLICT (idbonusindicator) DO UPDATE SET
        codebonusindicator = EXCLUDED.codebonusindicator,
        namebonusindicator = EXCLUDED.namebonusindicator;

    -- Delete-insert for the date range
    DELETE FROM gold.fact_player_bonuses
    WHERE gamingday >= p_from_date
      AND gamingday <= p_to_date;

    INSERT INTO gold.fact_player_bonuses (
        idplayerbonus, gamingday, timeoper, membership,
        idagent, agentname, citizenshipcountry,
        typeoper, idbonusindicator, idgame, gamename,
        sumbonuses, costbonuses,
        multiplierloyalty, hours, handsperhour, averbet, percentadt, adt,
        comment, isdeleted
    )
    SELECT
        pb.idplayerbonus,
        pb.datework AS gamingday,
        pb.timeoper,
        pb.membership,
        COALESCE(
            (SELECT ap.idagent
             FROM bronze.manage_agents_players_raw ap
             WHERE ap.membership = pb.membership
               AND ap.datechange <= pb.timeoper
             ORDER BY ap.datechange DESC, ap.created DESC
             LIMIT 1),
            NULLIF(pp.idagent, 0)
        ) AS idagent,
        CASE
            WHEN COALESCE(
                (SELECT ap.idagent
                 FROM bronze.manage_agents_players_raw ap
                 WHERE ap.membership = pb.membership
                   AND ap.datechange <= pb.timeoper
                 ORDER BY ap.datechange DESC, ap.created DESC
                 LIMIT 1),
                NULLIF(pp.idagent, 0)
            ) IS NOT NULL
            THEN COALESCE(ag.nameagent, 'NO AGENT')
            ELSE 'NO AGENT'
        END AS agentname,
        cc.namecountry AS citizenshipcountry,
        pb.typeoper,
        pb.idbonusindicator,
        pb.idgame,
        g.namegame AS gamename,
        COALESCE(pb.sumbonuses, 0)::numeric(19,4),
        COALESCE(pb.costbonuses, 0)::numeric(19,4),
        pb.multiplierloyalty,
        pb.hours,
        pb.handsperhour,
        pb.averbet,
        pb.percentadt,
        -- ADT = multiplier × hours × handsPerHour × avgBet
        CASE
            WHEN pb.multiplierloyalty IS NOT NULL
             AND pb.hours IS NOT NULL
             AND pb.handsperhour IS NOT NULL
             AND pb.averbet IS NOT NULL
            THEN (pb.multiplierloyalty * pb.hours * pb.handsperhour * pb.averbet)::numeric(19,4)
            ELSE NULL
        END AS adt,
        pb.comment,
        COALESCE(pb.isdeleted, false)
    FROM bronze.promo_player_bonuses_raw pb
    LEFT JOIN bronze.person_players_raw pp
        ON pp.membership = pb.membership
    LEFT JOIN bronze.casino_countries_raw cc
        ON cc.idcountry = pp.idcountry
    LEFT JOIN bronze.manage_agents_raw ag
        ON ag.idagent = COALESCE(
            (SELECT ap2.idagent
             FROM bronze.manage_agents_players_raw ap2
             WHERE ap2.membership = pb.membership
               AND ap2.datechange <= pb.timeoper
             ORDER BY ap2.datechange DESC, ap2.created DESC
             LIMIT 1),
            NULLIF(pp.idagent, 0)
        )
    LEFT JOIN bronze.casino_games_ref_raw g
        ON g.idgame = pb.idgame
    WHERE pb.datework >= p_from_date
      AND pb.datework <= p_to_date
      AND pb.membership IS NOT NULL
      AND pb.membership <> 5;
END;
$$;

CREATE OR REPLACE FUNCTION gold.sp_load_session_marts(
    p_from_date date,
    p_to_date   date
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    PERFORM silver.sp_load_reference_and_facts(p_from_date, p_to_date);
    PERFORM gold.sp_load_fact_session_day_by_table_game(p_from_date, p_to_date);
    PERFORM gold.sp_load_fact_session_day_by_agent_group(p_from_date, p_to_date);
END;
$$;