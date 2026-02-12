-- Full refresh example for a fixed period.
-- Edit dates/agent as needed, then execute in pgAdmin.

SELECT silver.sp_load_fact_membership_day(
    p_from_date => DATE '2026-01-01',
    p_to_date   => DATE '2026-01-31',
    p_agent_id  => NULL
);

SELECT gold.sp_load_fact_membership_day(
    p_from_date => DATE '2026-01-01',
    p_to_date   => DATE '2026-01-31',
    p_agent_id  => NULL
);

SELECT *
FROM gold.fn_membership_period(
    p_from_date => DATE '2026-01-01',
    p_to_date   => DATE '2026-01-31',
    p_agent_id  => NULL
)
ORDER BY totaldrop_clean DESC;