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

CREATE TABLE IF NOT EXISTS affiliate_streamer_claims (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    affiliate_twitch_login  TEXT NOT NULL REFERENCES affiliate_accounts(twitch_login),
    claimed_streamer_login  TEXT NOT NULL,
    claimed_at              TEXT NOT NULL,
    UNIQUE (claimed_streamer_login)
);
CREATE INDEX IF NOT EXISTS idx_aff_claims_affiliate
    ON affiliate_streamer_claims(affiliate_twitch_login);

CREATE TABLE IF NOT EXISTS affiliate_commissions (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
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
