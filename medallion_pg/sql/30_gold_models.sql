CREATE TABLE IF NOT EXISTS gold.fact_membership_day (
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
        FROM silver.fact_membership_day d
        WHERE d.gamingday >= p_from_date
            AND d.gamingday <= p_to_date
            AND (p_agent_id IS NULL OR d.idagent = p_agent_id);
END;
$$;

CREATE OR REPLACE FUNCTION gold.fn_membership_period(
    p_from_date date,
    p_to_date   date,
    p_agent_id  int DEFAULT NULL
)
RETURNS TABLE (
    membership              bigint,
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
    sessionscnt             bigint
)
LANGUAGE sql
AS $$
SELECT
    d.membership,
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
    SUM(d.sessionscnt)::bigint AS sessionscnt
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