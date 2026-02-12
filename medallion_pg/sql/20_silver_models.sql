CREATE OR REPLACE FUNCTION silver.fn_membership_day(
    p_from_date date,
    p_to_date   date,
    p_agent_id  int DEFAULT NULL
)
RETURNS TABLE (
    gamingday               date,
    membership              bigint,
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
    sessionscnt             bigint
)
LANGUAGE sql
AS $$
WITH visits_base AS (
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
day_rows AS (
    SELECT
        vb.gamingday,
        vb.membership,
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
        COALESCE(s.sessionscnt, 0)::bigint AS sessionscnt
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
    LEFT JOIN drop_clean dc
        ON dc.gamingday = vb.gamingday
       AND dc.membership = vb.membership
    LEFT JOIN system_drop sd
        ON sd.gamingday = vb.gamingday
       AND sd.membership = vb.membership
)
SELECT *
FROM day_rows
WHERE p_agent_id IS NULL OR idagent = p_agent_id;
$$;

CREATE TABLE IF NOT EXISTS silver.fact_membership_day (
    gamingday               date NOT NULL,
    membership              bigint NOT NULL,
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
    loaded_at_utc           timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (gamingday, membership)
);

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
        sessionscnt
    )
    SELECT
        d.gamingday,
        d.membership,
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
        d.sessionscnt
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