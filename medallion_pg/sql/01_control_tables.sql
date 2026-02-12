CREATE TABLE IF NOT EXISTS dw_control.etl_watermark (
    source_table        text PRIMARY KEY,
    watermark_column    text NOT NULL,
    watermark_value     text,
    updated_at_utc      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dw_control.etl_run_log (
    run_id              bigserial PRIMARY KEY,
    pipeline_name       text NOT NULL,
    source_table        text,
    status              text NOT NULL,
    started_at_utc      timestamptz NOT NULL DEFAULT now(),
    finished_at_utc     timestamptz,
    rows_read           bigint DEFAULT 0,
    rows_written        bigint DEFAULT 0,
    error_message       text
);

INSERT INTO dw_control.etl_watermark (source_table, watermark_column, watermark_value)
VALUES
    ('Person.Visits', 'Created', NULL),
    ('Manage.Agents_Players', 'dateChange', NULL),
    ('Person.Players', 'Membership', NULL),
    ('Manage.Agents', 'idAgent', NULL),
    ('Casino.Countries', 'idCountry', NULL),
    ('CashDesk.view_Transactions', 'idOper', NULL),
    ('Casino.Transactions_Calculated', 'idOper', NULL),
    ('Manage.PlayerSessions', 'idPlayersTracking', NULL),
    ('Casino.PlayersTracking', 'idPlayersTracking', NULL)
ON CONFLICT (source_table) DO NOTHING;