-- gold.fn_visit_sessions(p_from_date, p_to_date)
-- Returns one row per (visit x overlapping game session) for the requested gaming-day range.
-- Mirrors the legacy MSSQL "Casino Daily Analysis" report: visits from Person.Visits joined to
-- player gaming sessions from Manage.PlayerSessions via Casino.PlayersTracking.
-- Mapping in PG bronze:
--   Person.Visits            -> bronze.person_visits_raw
--   Manage.PlayerSessions    -> bronze.manage_player_sessions_raw
--   Casino.PlayersTracking   -> bronze.casino_players_tracking_raw
-- Membership filter excludes NULL and the anonymous-entry sentinel (5).

CREATE OR REPLACE FUNCTION gold.fn_visit_sessions(p_from_date date, p_to_date date)
RETURNS TABLE (
    gaming_day        date,
    membership        bigint,
    visit_no          int,
    casino_entry_time time,
    casino_exit_time  time,
    session_start     time,
    session_finish    time,
    averbet           numeric(19,4)
)
LANGUAGE sql STABLE AS $$
    WITH visits_base AS (
        SELECT
            v.datework AS gaming_day,
            v.membership,
            ROW_NUMBER() OVER (
                PARTITION BY v.datework, v.membership
                ORDER BY COALESCE(v.time_in, v.created), v.time_out
            )::int AS visit_no,
            v.time_in,
            v.time_out
        FROM bronze.person_visits_raw v
        WHERE v.datework BETWEEN p_from_date AND p_to_date
          AND v.membership IS NOT NULL
          AND v.membership <> 5
    ),
    game_sessions AS (
        SELECT
            pt.datework::date AS gaming_day,
            pt.membership,
            ps.idplayersession,
            ps.timestart,
            ps.timefinish,
            ps.averbet
        FROM bronze.manage_player_sessions_raw ps
        JOIN bronze.casino_players_tracking_raw pt
          ON pt.idplayerstracking = ps.idplayerstracking
        WHERE pt.datework BETWEEN p_from_date AND p_to_date
          AND pt.membership IS NOT NULL
          AND pt.membership <> 5
          AND COALESCE(ps.isdeleted, false) = false
    )
    SELECT
        vb.gaming_day,
        vb.membership,
        vb.visit_no,
        vb.time_in::time  AS casino_entry_time,
        vb.time_out::time AS casino_exit_time,
        gs.timestart::time  AS session_start,
        gs.timefinish::time AS session_finish,
        gs.averbet
    FROM visits_base vb
    LEFT JOIN game_sessions gs
      ON gs.gaming_day = vb.gaming_day
     AND gs.membership = vb.membership
     AND (
            (gs.timestart  IS NOT NULL AND gs.timefinish IS NOT NULL
                AND gs.timestart  <  vb.time_out AND gs.timefinish > vb.time_in)
         OR (gs.timestart  IS NOT NULL AND gs.timefinish IS NULL
                AND gs.timestart  >= vb.time_in  AND gs.timestart  <  vb.time_out)
         OR (gs.timestart  IS NULL     AND gs.timefinish IS NOT NULL
                AND gs.timefinish >  vb.time_in  AND gs.timefinish <= vb.time_out)
         )
    ORDER BY vb.gaming_day DESC, vb.membership, vb.visit_no, gs.timestart, gs.idplayersession;
$$;
