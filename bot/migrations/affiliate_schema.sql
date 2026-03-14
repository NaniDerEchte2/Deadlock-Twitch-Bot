-- Vertriebler-Konten
CREATE TABLE IF NOT EXISTS affiliate_accounts (
    twitch_login        TEXT PRIMARY KEY,
    twitch_user_id      TEXT NOT NULL,
    display_name        TEXT,
    email               TEXT NOT NULL,
    full_name           TEXT NOT NULL,
    address_line1       TEXT NOT NULL,
    address_city        TEXT NOT NULL,
    address_zip         TEXT NOT NULL,
    address_country     TEXT NOT NULL DEFAULT 'DE',
    stripe_account_id   TEXT,
    stripe_connected_at TEXT,
    stripe_connect_status TEXT DEFAULT 'pending',
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    is_active           INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS affiliate_pii (
    twitch_login        TEXT PRIMARY KEY REFERENCES affiliate_accounts(twitch_login),
    full_name_enc       BYTEA,
    email_enc           BYTEA,
    address_line1_enc   BYTEA,
    address_city_enc    BYTEA,
    address_zip_enc     BYTEA,
    tax_id_enc          BYTEA,
    address_country     TEXT NOT NULL DEFAULT 'DE',
    ust_status          TEXT NOT NULL DEFAULT 'unknown',
    updated_at          TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_aff_pii_ust_status
    ON affiliate_pii(ust_status);

CREATE TABLE IF NOT EXISTS affiliate_streamer_claims (
    id                      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    affiliate_twitch_login  TEXT NOT NULL REFERENCES affiliate_accounts(twitch_login),
    claimed_streamer_login  TEXT NOT NULL,
    claimed_at              TEXT NOT NULL,
    UNIQUE (claimed_streamer_login)
);
CREATE INDEX IF NOT EXISTS idx_aff_claims_affiliate
    ON affiliate_streamer_claims(affiliate_twitch_login);

CREATE TABLE IF NOT EXISTS affiliate_commissions (
    id                      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    affiliate_twitch_login  TEXT NOT NULL REFERENCES affiliate_accounts(twitch_login),
    streamer_login          TEXT NOT NULL,
    stripe_event_id         TEXT UNIQUE NOT NULL,
    stripe_invoice_id       TEXT,
    stripe_customer_id      TEXT,
    stripe_transfer_id      TEXT,
    brutto_cents            INTEGER NOT NULL,
    commission_cents        INTEGER NOT NULL,
    currency                TEXT NOT NULL DEFAULT 'eur',
    status                  TEXT NOT NULL DEFAULT 'pending',
    period_start            TEXT,
    period_end              TEXT,
    created_at              TEXT NOT NULL,
    transferred_at          TEXT,
    error_message           TEXT
);
CREATE INDEX IF NOT EXISTS idx_aff_comm_affiliate
    ON affiliate_commissions(affiliate_twitch_login, status);
CREATE INDEX IF NOT EXISTS idx_aff_comm_streamer
    ON affiliate_commissions(streamer_login);
CREATE INDEX IF NOT EXISTS idx_aff_comm_created_month
    ON affiliate_commissions(affiliate_twitch_login, created_at);

CREATE TABLE IF NOT EXISTS affiliate_gutschrift_counter (
    year_month          TEXT PRIMARY KEY,
    last_seq            INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS affiliate_gutschriften (
    id                      INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gutschrift_number       TEXT UNIQUE NOT NULL,
    affiliate_twitch_login  TEXT NOT NULL REFERENCES affiliate_accounts(twitch_login),
    period_year             INTEGER NOT NULL,
    period_month            INTEGER NOT NULL,
    net_amount_cents        INTEGER NOT NULL,
    vat_rate_percent        NUMERIC(5,2) NOT NULL DEFAULT 0,
    vat_amount_cents        INTEGER NOT NULL DEFAULT 0,
    gross_amount_cents      INTEGER NOT NULL,
    affiliate_name          TEXT NOT NULL,
    affiliate_address       TEXT NOT NULL,
    affiliate_tax_id        TEXT,
    affiliate_ust_status    TEXT NOT NULL,
    issuer_name             TEXT NOT NULL,
    issuer_address          TEXT NOT NULL,
    issuer_tax_id           TEXT NOT NULL,
    pdf_blob                BYTEA,
    pdf_generated_at        TEXT,
    email_sent_at           TEXT,
    email_error             TEXT,
    commission_ids          TEXT,
    created_at              TEXT NOT NULL,
    UNIQUE (affiliate_twitch_login, period_year, period_month)
);
CREATE INDEX IF NOT EXISTS idx_aff_gutschriften_affiliate
    ON affiliate_gutschriften(affiliate_twitch_login, period_year DESC, period_month DESC);
