CREATE SCHEMA IF NOT EXISTS archive;

CREATE TABLE IF NOT EXISTS archive.person_visits_raw AS
SELECT * FROM bronze.person_visits_raw WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS archive.cashdesk_transactions_raw AS
SELECT * FROM bronze.cashdesk_transactions_raw WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS archive.casino_players_tracking_raw AS
SELECT * FROM bronze.casino_players_tracking_raw WHERE 1 = 0;

CREATE TABLE IF NOT EXISTS archive.manage_player_sessions_raw AS
SELECT * FROM bronze.manage_player_sessions_raw WHERE 1 = 0;

CREATE OR REPLACE FUNCTION dw_control.sp_bronze_archive_or_purge(
    p_cutoff_date date,
    p_mode text DEFAULT 'archive_delete'
)
RETURNS TABLE (
    table_name text,
    archived_rows bigint,
    deleted_rows bigint
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_archived bigint;
    v_deleted bigint;
BEGIN
    IF p_mode NOT IN ('archive_delete', 'delete_only') THEN
        RAISE EXCEPTION 'Invalid p_mode: %. Use archive_delete or delete_only', p_mode;
    END IF;

    IF p_mode = 'archive_delete' THEN
        INSERT INTO archive.person_visits_raw
        SELECT *
        FROM bronze.person_visits_raw
        WHERE datework < p_cutoff_date;

        GET DIAGNOSTICS v_archived = ROW_COUNT;
    ELSE
        v_archived := 0;
    END IF;

    DELETE FROM bronze.person_visits_raw
    WHERE datework < p_cutoff_date;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RETURN QUERY SELECT 'bronze.person_visits_raw'::text, v_archived, v_deleted;

    IF p_mode = 'archive_delete' THEN
        INSERT INTO archive.cashdesk_transactions_raw
        SELECT *
        FROM bronze.cashdesk_transactions_raw
        WHERE datework < p_cutoff_date;

        GET DIAGNOSTICS v_archived = ROW_COUNT;
    ELSE
        v_archived := 0;
    END IF;

    DELETE FROM bronze.cashdesk_transactions_raw
    WHERE datework < p_cutoff_date;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RETURN QUERY SELECT 'bronze.cashdesk_transactions_raw'::text, v_archived, v_deleted;

    IF p_mode = 'archive_delete' THEN
        INSERT INTO archive.casino_players_tracking_raw
        SELECT *
        FROM bronze.casino_players_tracking_raw
        WHERE datework < p_cutoff_date;

        GET DIAGNOSTICS v_archived = ROW_COUNT;
    ELSE
        v_archived := 0;
    END IF;

    DELETE FROM bronze.casino_players_tracking_raw
    WHERE datework < p_cutoff_date;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RETURN QUERY SELECT 'bronze.casino_players_tracking_raw'::text, v_archived, v_deleted;

    IF p_mode = 'archive_delete' THEN
        INSERT INTO archive.manage_player_sessions_raw
        SELECT *
        FROM bronze.manage_player_sessions_raw
        WHERE timestart::date < p_cutoff_date;

        GET DIAGNOSTICS v_archived = ROW_COUNT;
    ELSE
        v_archived := 0;
    END IF;

    DELETE FROM bronze.manage_player_sessions_raw
    WHERE timestart::date < p_cutoff_date;

    GET DIAGNOSTICS v_deleted = ROW_COUNT;

    RETURN QUERY SELECT 'bronze.manage_player_sessions_raw'::text, v_archived, v_deleted;
END;
$$;
