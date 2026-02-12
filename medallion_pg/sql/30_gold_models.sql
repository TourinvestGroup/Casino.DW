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