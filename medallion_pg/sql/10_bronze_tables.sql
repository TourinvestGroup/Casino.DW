CREATE TABLE IF NOT EXISTS bronze.person_visits_raw (
    datework            date,
    membership          bigint,
    time_in             timestamp,
    time_out            timestamp,
    created             timestamp,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_person_visits_raw_day_mem ON bronze.person_visits_raw(datework, membership);
CREATE INDEX IF NOT EXISTS idx_person_visits_raw_created ON bronze.person_visits_raw(created);

CREATE TABLE IF NOT EXISTS bronze.manage_agents_players_raw (
    membership          bigint,
    idagent             int,
    datechange          timestamp,
    created             timestamp,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_agents_players_raw_mem_change ON bronze.manage_agents_players_raw(membership, datechange DESC);

CREATE TABLE IF NOT EXISTS bronze.person_players_raw (
    membership          bigint PRIMARY KEY,
    idagent             int,
    idcountry           int,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.manage_agents_raw (
    idagent             int PRIMARY KEY,
    nameagent           text,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.casino_countries_raw (
    idcountry           int PRIMARY KEY,
    namecountry         text,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.cashdesk_transactions_raw (
    idoper              bigint PRIMARY KEY,
    datework            date,
    timeoper            timestamp,
    membership          bigint,
    idaccount           int,
    directionoper       int,
    totalmoneyue        numeric(19,4),
    chipsue             numeric(19,4),
    isdeleted           boolean,
    iscalculatedindrop  boolean,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_cashdesk_tx_day_mem ON bronze.cashdesk_transactions_raw(datework, membership);
CREATE INDEX IF NOT EXISTS idx_cashdesk_tx_mem_time ON bronze.cashdesk_transactions_raw(membership, timeoper);

CREATE TABLE IF NOT EXISTS bronze.casino_transactions_calculated_raw (
    idoper              bigint PRIMARY KEY,
    deposit             numeric(19,4),
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.manage_player_sessions_raw (
    idplayerstracking   bigint,
    timestart           timestamp,
    realdrop            numeric(19,4),
    handhold            numeric(19,4),
    cashout             numeric(19,4),
    averbet             numeric(19,4),
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_player_sessions_tracking ON bronze.manage_player_sessions_raw(idplayerstracking);

CREATE TABLE IF NOT EXISTS bronze.casino_players_tracking_raw (
    idplayerstracking   bigint PRIMARY KEY,
    datework            date,
    membership          bigint,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_players_tracking_day_mem ON bronze.casino_players_tracking_raw(datework, membership);

-- Future source placeholders (Scenario 2.5, extensible)
CREATE TABLE IF NOT EXISTS bronze.casino_games_raw (
    source_pk           text PRIMARY KEY,
    payload_json        jsonb,
    created_at_src      timestamp,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.casino_tables_raw (
    source_pk           text PRIMARY KEY,
    payload_json        jsonb,
    created_at_src      timestamp,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);