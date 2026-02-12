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

CREATE TABLE IF NOT EXISTS bronze.manage_agent_groups_raw (
    idagentgroup         int PRIMARY KEY,
    idagent              int NOT NULL,
    nameagentgroup       text,
    datebegin            date NOT NULL,
    dateend              date,
    memoagentgroup       text,
    created              timestamp NOT NULL,
    createdby            text NOT NULL,
    modified             timestamp NOT NULL,
    modifiedby           text NOT NULL,
    row_version          int NOT NULL,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_manage_agent_groups_modified ON bronze.manage_agent_groups_raw(modified);

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

CREATE TABLE IF NOT EXISTS bronze.casino_games_ref_raw (
    idgame               int PRIMARY KEY,
    codegame             text NOT NULL,
    namegame             text NOT NULL,
    listorder_game       int NOT NULL,
    idbonussystem        int,
    nmbboxes             int NOT NULL,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.casino_table_types_games_raw (
    idtabletype          int NOT NULL,
    idgame               int NOT NULL,
    listorder            int NOT NULL,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idtabletype, idgame)
);

CREATE TABLE IF NOT EXISTS bronze.casino_table_types_raw (
    idtabletype          int PRIMARY KEY,
    codetabletype        text NOT NULL,
    nametabletype        text NOT NULL,
    listorder_tabletype  int NOT NULL,
    idgametype           int NOT NULL,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.casino_tables_ref_raw (
    idtable              int PRIMARY KEY,
    idtabletype          int NOT NULL,
    nametable            text NOT NULL,
    snmbtable            text,
    codetable            text,
    isvirtual            boolean NOT NULL,
    listorder_table      int NOT NULL,
    idbonusgame          int,
    idbonussystem        int,
    ismarketing          boolean NOT NULL,
    idcurrency           int,
    mysteryguarantee     numeric(19,4) NOT NULL,
    ratemystery          numeric(19,4) NOT NULL,
    lowerlimitmystery    numeric(19,4) NOT NULL,
    upperlimitmystery    numeric(19,4) NOT NULL,
    minplayingbet        numeric(19,4) NOT NULL,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.casino_currency_exch_rates_raw (
    idcasino             int NOT NULL,
    datechange           date NOT NULL,
    idcurrency           int NOT NULL,
    idcurrencyexchrate   int NOT NULL,
    exchrate             numeric(19,8) NOT NULL,
    created              timestamp NOT NULL,
    createdby            text NOT NULL,
    createdhostname      text NOT NULL,
    modified             timestamp NOT NULL,
    modifiedby           text NOT NULL,
    modifiedhostname     text NOT NULL,
    row_version          int NOT NULL,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idcasino, datechange, idcurrency)
);
CREATE INDEX IF NOT EXISTS idx_currency_exch_rates_row_version ON bronze.casino_currency_exch_rates_raw(row_version);

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
    idplayersession     bigint,
    timefinish          timestamp,
    idtable             int,
    idgame              int,
    idslot              int,
    realdrop            numeric(19,4),
    chipsin             numeric(19,4),
    chipsout            numeric(19,4),
    handhold            numeric(19,4),
    cashout             numeric(19,4),
    averbet             numeric(19,4),
    isdeleted           boolean,
    created             timestamp,
    modified            timestamp,
    row_version         int,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS idplayersession bigint;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS timefinish timestamp;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS idtable int;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS idgame int;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS idslot int;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS chipsin numeric(19,4);
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS chipsout numeric(19,4);
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS isdeleted boolean;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS created timestamp;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS modified timestamp;
ALTER TABLE bronze.manage_player_sessions_raw ADD COLUMN IF NOT EXISTS row_version int;
CREATE INDEX IF NOT EXISTS idx_player_sessions_tracking ON bronze.manage_player_sessions_raw(idplayerstracking);
CREATE UNIQUE INDEX IF NOT EXISTS ux_player_sessions_tracking_start ON bronze.manage_player_sessions_raw(idplayerstracking, timestart);

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