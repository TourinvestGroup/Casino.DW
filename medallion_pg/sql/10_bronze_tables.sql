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
CREATE UNIQUE INDEX IF NOT EXISTS ux_agents_players_raw_mem_change ON bronze.manage_agents_players_raw(membership, datechange);
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
    surname             text,
    forename            text,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE bronze.person_players_raw ADD COLUMN IF NOT EXISTS surname text;
ALTER TABLE bronze.person_players_raw ADD COLUMN IF NOT EXISTS forename text;

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
    idarticle           int,
    comment             text,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE bronze.cashdesk_transactions_raw ADD COLUMN IF NOT EXISTS idarticle int;
ALTER TABLE bronze.cashdesk_transactions_raw ADD COLUMN IF NOT EXISTS comment text;
CREATE INDEX IF NOT EXISTS idx_cashdesk_tx_day_mem ON bronze.cashdesk_transactions_raw(datework, membership);
CREATE INDEX IF NOT EXISTS idx_cashdesk_tx_mem_time ON bronze.cashdesk_transactions_raw(membership, timeoper);

CREATE TABLE IF NOT EXISTS bronze.cashdesk_articles_raw (
    idarticle           int PRIMARY KEY,
    pidarticle          int,
    namearticle         text,
    codearticle         text,
    isplayerexpense     boolean,
    isincome            boolean,
    isrestaurant        boolean,
    listorder_article   int,
    _source_system      text DEFAULT 'mssql',
    _loaded_at_utc      timestamptz NOT NULL DEFAULT now()
);

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

CREATE TABLE IF NOT EXISTS bronze.drgt_sessions_raw (
    playerid             bigint NOT NULL,
    ipaddr               bigint,
    id_session           int NOT NULL,
    gamingday            date NOT NULL,
    starttimelocal       timestamp,
    endtimelocal         timestamp,
    totalbet             numeric(19,4),
    promobet             numeric(19,4),
    cashbet              numeric(19,4),
    totalout             numeric(19,4),
    gamesplayed          numeric(19,4),
    win                  numeric(19,4),
    nwl                  numeric(19,4),
    billdrop             numeric(19,4),
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_drgt_sessions_player_session
    ON bronze.drgt_sessions_raw(playerid, id_session);
CREATE INDEX IF NOT EXISTS idx_drgt_sessions_day
    ON bronze.drgt_sessions_raw(gamingday);

CREATE TABLE IF NOT EXISTS bronze.casino_transaction_money_raw (
    idoper               bigint NOT NULL,
    idmoney              int NOT NULL,
    summoney             numeric(19,4),
    exchrate             numeric(19,8),
    exchrate_main        numeric(19,8),
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idoper, idmoney)
);

CREATE TABLE IF NOT EXISTS bronze.manage_player_session_details_raw (
    idplayersessiondetail  bigint PRIMARY KEY,
    idplayersession        bigint NOT NULL,
    _source_system         text DEFAULT 'mssql',
    _loaded_at_utc         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_session_details_session
    ON bronze.manage_player_session_details_raw(idplayersession);

CREATE TABLE IF NOT EXISTS bronze.manage_player_session_detail_chips_raw (
    idplayersessiondetail  bigint NOT NULL,
    idchip                 int NOT NULL,
    qntchips               numeric(19,4),
    _source_system         text DEFAULT 'mssql',
    _loaded_at_utc         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (idplayersessiondetail, idchip)
);

CREATE TABLE IF NOT EXISTS bronze.casino_chips_raw (
    idchip                 int PRIMARY KEY,
    valuechip              numeric(19,4),
    _source_system         text DEFAULT 'mssql',
    _loaded_at_utc         timestamptz NOT NULL DEFAULT now()
);

-- ============================================================
-- Promo bonus tables
-- ============================================================

CREATE TABLE IF NOT EXISTS bronze.promo_player_bonuses_raw (
    idplayerbonus        int PRIMARY KEY,
    typeoper             int,                -- 1=earned, -1=reversed
    datework             date,
    timeoper             timestamp,
    membership           bigint,
    idhall               int,
    idplayerstracking    int,
    idplayersession      int,
    idbonusindicator     int,
    idpresenttype        int,
    idoper               int,
    sumbonuses           numeric(19,4),
    isvisit              boolean,
    idgame               int,
    idslotmanufacturer   int,
    multiplierloyalty    numeric(19,4),
    hours                numeric(19,4),
    handsperhour         int,
    averbet              numeric(19,4),
    percentadt           numeric(19,4),
    comment              text,
    isdeleted            boolean,
    costbonuses          numeric(19,4),
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_promo_player_bonuses_day_mem
    ON bronze.promo_player_bonuses_raw(datework, membership);
CREATE INDEX IF NOT EXISTS idx_promo_player_bonuses_game
    ON bronze.promo_player_bonuses_raw(idgame);

CREATE TABLE IF NOT EXISTS bronze.promo_bonus_indicators_raw (
    idbonusindicator     int PRIMARY KEY,
    codebonusindicator   text,
    namebonusindicator   text,
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bronze.promo_bonus_indicators_games_raw (
    idgame               int,
    idslotmanufacturer   int,
    datechange           date NOT NULL,
    multiplierloyalty    numeric(19,4),
    handsperhour         int,
    percentadt           numeric(19,4),
    _source_system       text DEFAULT 'mssql',
    _loaded_at_utc       timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_promo_bi_games_key
    ON bronze.promo_bonus_indicators_games_raw(COALESCE(idgame, -1), COALESCE(idslotmanufacturer, -1), datechange);

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