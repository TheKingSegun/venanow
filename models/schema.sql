-- ============================================================
-- VenaNow Database Schema
-- PostgreSQL 16+
-- ============================================================

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";  -- For fuzzy text search on descriptions

-- ============================================================
-- USERS
-- ============================================================
CREATE TABLE IF NOT EXISTS users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email           VARCHAR(255) UNIQUE NOT NULL,
    full_name       VARCHAR(255) NOT NULL,
    hashed_password VARCHAR(255) NOT NULL,
    is_active       BOOLEAN DEFAULT TRUE,
    currency        VARCHAR(10) DEFAULT 'NGN',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- CATEGORIES
-- ============================================================
CREATE TABLE IF NOT EXISTS categories (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    slug        VARCHAR(100) NOT NULL UNIQUE,  -- e.g. 'food', 'transport'
    icon        VARCHAR(50),
    color       VARCHAR(20),
    is_income   BOOLEAN DEFAULT FALSE,
    is_system   BOOLEAN DEFAULT TRUE,  -- system-defined vs user-defined
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Seed default categories
INSERT INTO categories (name, slug, icon, color, is_income) VALUES
    ('Food & Dining',      'food',          '🍔', '#e07000', FALSE),
    ('Transport',          'transport',     '🚗', '#1a3f8f', FALSE),
    ('Rent',               'rent',          '🏠', '#c0430a', FALSE),
    ('Utilities',          'utilities',     '💡', '#6a1a8f', FALSE),
    ('Subscriptions',      'subscriptions', '📱', '#8f1a4e', FALSE),
    ('Transfers',          'transfers',     '↔',  '#888888', FALSE),
    ('Business',           'business',      '💼', '#1a6b47', FALSE),
    ('Miscellaneous',      'miscellaneous', '•',  '#aaaaaa', FALSE),
    ('Salary / Income',    'income',        '💰', '#1a6b47', TRUE),
    ('Freelance / Side',   'freelance',     '⚡', '#0f4a2e', TRUE),
    ('Investment Return',  'investment',    '📈', '#1a3f8f', TRUE)
ON CONFLICT (slug) DO NOTHING;

-- ============================================================
-- BANK ACCOUNTS
-- ============================================================
CREATE TABLE IF NOT EXISTS bank_accounts (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    bank_name       VARCHAR(100) NOT NULL,   -- 'GTBank', 'Zenith', 'OPay', etc.
    account_number  VARCHAR(20),
    account_name    VARCHAR(255),
    account_type    VARCHAR(50),             -- 'savings', 'current', 'wallet'
    currency        VARCHAR(10) DEFAULT 'NGN',
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- STATEMENT UPLOADS
-- ============================================================
CREATE TABLE IF NOT EXISTS statement_uploads (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id      UUID REFERENCES bank_accounts(id),
    filename        VARCHAR(500) NOT NULL,
    file_type       VARCHAR(20) NOT NULL,    -- 'pdf', 'csv', 'excel'
    file_size_kb    INTEGER,
    bank_detected   VARCHAR(100),
    period_start    DATE,
    period_end      DATE,
    tx_count        INTEGER DEFAULT 0,
    status          VARCHAR(30) DEFAULT 'pending',  -- pending, processing, done, failed
    error_message   TEXT,
    processed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- TRANSACTIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS transactions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    account_id      UUID REFERENCES bank_accounts(id),
    upload_id       UUID REFERENCES statement_uploads(id),
    category_id     INTEGER REFERENCES categories(id),

    -- Core fields
    tx_date         DATE NOT NULL,
    value_date      DATE,
    description     TEXT NOT NULL,
    raw_description TEXT,          -- Original unprocessed description
    amount          NUMERIC(18,2) NOT NULL,
    tx_type         VARCHAR(10) NOT NULL CHECK (tx_type IN ('debit', 'credit')),
    balance         NUMERIC(18,2),
    reference       VARCHAR(255),

    -- Classification metadata
    channel         VARCHAR(100),  -- 'POS', 'Transfer', 'USSD', 'Card Online', 'ATM'
    merchant        VARCHAR(255),  -- Extracted merchant name
    bank_detected   VARCHAR(100),  -- Fintech: OPay, PalmPay, Kuda, etc.
    classified_by   VARCHAR(20) DEFAULT 'rule',  -- 'rule', 'ml', 'user'
    confidence      NUMERIC(5,4),  -- Classification confidence 0-1
    is_recurring    BOOLEAN DEFAULT FALSE,
    recurring_group VARCHAR(255),  -- Groups recurring txs by merchant/pattern

    -- Dedup fingerprint
    fingerprint     VARCHAR(64) UNIQUE,  -- SHA256(date+amount+description)

    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_tx_user_date    ON transactions(user_id, tx_date DESC);
CREATE INDEX IF NOT EXISTS idx_tx_category     ON transactions(user_id, category_id);
CREATE INDEX IF NOT EXISTS idx_tx_type         ON transactions(user_id, tx_type);
CREATE INDEX IF NOT EXISTS idx_tx_recurring    ON transactions(user_id, is_recurring);
CREATE INDEX IF NOT EXISTS idx_tx_desc_trgm    ON transactions USING gin(description gin_trgm_ops);

-- ============================================================
-- BUDGETS
-- ============================================================
CREATE TABLE IF NOT EXISTS budgets (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    category_id     INTEGER NOT NULL REFERENCES categories(id),
    month           DATE NOT NULL,           -- First day of the month
    amount          NUMERIC(18,2) NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, category_id, month)
);

-- ============================================================
-- GOALS
-- ============================================================
CREATE TABLE IF NOT EXISTS goals (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id             UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name                VARCHAR(255) NOT NULL,
    target_amount       NUMERIC(18,2) NOT NULL,
    current_amount      NUMERIC(18,2) DEFAULT 0,
    target_date         DATE,
    monthly_contribution NUMERIC(18,2),
    is_completed        BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- RECOMMENDATIONS
-- ============================================================
CREATE TABLE IF NOT EXISTS recommendations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type            VARCHAR(30) NOT NULL,   -- 'warning', 'opportunity', 'alert', 'tip'
    category        VARCHAR(100),
    title           VARCHAR(500) NOT NULL,
    body            TEXT NOT NULL,
    impact_amount   NUMERIC(18,2),          -- Potential ₦ saving
    priority        INTEGER DEFAULT 5,      -- 1=high, 10=low
    is_dismissed    BOOLEAN DEFAULT FALSE,
    generated_at    TIMESTAMPTZ DEFAULT NOW(),
    expires_at      TIMESTAMPTZ
);

-- ============================================================
-- FINANCIAL HEALTH METRICS (monthly snapshots)
-- ============================================================
CREATE TABLE IF NOT EXISTS financial_health_snapshots (
    id                  UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id             UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    month               DATE NOT NULL,
    total_income        NUMERIC(18,2) DEFAULT 0,
    total_expenses      NUMERIC(18,2) DEFAULT 0,
    net_savings         NUMERIC(18,2) DEFAULT 0,
    savings_rate        NUMERIC(6,4) DEFAULT 0,
    health_score        INTEGER,            -- 0-100
    score_breakdown     JSONB,              -- Component scores
    expense_by_category JSONB,             -- {category: amount}
    income_sources      JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, month)
);

-- ============================================================
-- VIEWS
-- ============================================================

-- Monthly summary view
CREATE OR REPLACE VIEW monthly_summary AS
SELECT
    user_id,
    DATE_TRUNC('month', tx_date)::DATE AS month,
    SUM(CASE WHEN tx_type = 'credit' THEN amount ELSE 0 END) AS total_income,
    SUM(CASE WHEN tx_type = 'debit'  THEN amount ELSE 0 END) AS total_expenses,
    SUM(CASE WHEN tx_type = 'credit' THEN amount ELSE -amount END) AS net_savings,
    COUNT(*) AS tx_count
FROM transactions
GROUP BY user_id, DATE_TRUNC('month', tx_date);

-- Category spend view
CREATE OR REPLACE VIEW category_spend AS
SELECT
    t.user_id,
    DATE_TRUNC('month', t.tx_date)::DATE AS month,
    c.name AS category,
    c.slug,
    SUM(t.amount) AS total,
    COUNT(*) AS tx_count
FROM transactions t
JOIN categories c ON t.category_id = c.id
WHERE t.tx_type = 'debit'
GROUP BY t.user_id, DATE_TRUNC('month', t.tx_date), c.name, c.slug;
