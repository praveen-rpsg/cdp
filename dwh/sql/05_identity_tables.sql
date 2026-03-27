-- ============================================================================
-- Identity Layer: Identity resolution & graph tables
-- ============================================================================

-- Stores raw identifier observations from all source systems
CREATE TABLE IF NOT EXISTS identity.raw_identifiers (
    id                  BIGSERIAL PRIMARY KEY,
    source_system       TEXT NOT NULL,   -- 'POS', 'ECOM', 'NPS', 'YVM', 'PROMO_CASHBACK', 'LOYALTY'
    identifier_type     TEXT NOT NULL,   -- 'mobile', 'email', 'loyalty_id', 'ecom_customer_id'
    identifier_value    TEXT NOT NULL,
    associated_name     TEXT,
    associated_pincode  TEXT,
    associated_store    TEXT,
    observed_at         TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_id_type_val ON identity.raw_identifiers(identifier_type, identifier_value);
CREATE INDEX IF NOT EXISTS idx_raw_id_source ON identity.raw_identifiers(source_system);

-- Canonical identity: one row per resolved person
CREATE TABLE IF NOT EXISTS identity.unified_profiles (
    unified_id          TEXT PRIMARY KEY,  -- UUID or hash-based canonical ID
    canonical_mobile    TEXT,
    canonical_name      TEXT,
    canonical_email     TEXT,
    canonical_pincode   TEXT,
    primary_store_code  TEXT,
    confidence_score    NUMERIC DEFAULT 1.0,
    merge_count         INT DEFAULT 1,
    first_seen_at       TIMESTAMPTZ,
    last_seen_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_unified_mobile ON identity.unified_profiles(canonical_mobile)
    WHERE canonical_mobile IS NOT NULL;

-- Identity graph edges: links between identifiers
CREATE TABLE IF NOT EXISTS identity.identity_edges (
    edge_id             BIGSERIAL PRIMARY KEY,
    unified_id          TEXT NOT NULL REFERENCES identity.unified_profiles(unified_id),
    identifier_type     TEXT NOT NULL,
    identifier_value    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    confidence          NUMERIC DEFAULT 1.0,
    edge_type           TEXT DEFAULT 'DETERMINISTIC',  -- 'DETERMINISTIC', 'PROBABILISTIC'
    first_seen_at       TIMESTAMPTZ DEFAULT NOW(),
    last_seen_at        TIMESTAMPTZ DEFAULT NOW(),
    is_active           BOOLEAN DEFAULT TRUE,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_edge_unique ON identity.identity_edges(identifier_type, identifier_value, source_system);
CREATE INDEX IF NOT EXISTS idx_edge_unified ON identity.identity_edges(unified_id);

-- Identity merge log: audit trail for identity merges
CREATE TABLE IF NOT EXISTS identity.merge_log (
    merge_id            BIGSERIAL PRIMARY KEY,
    winner_id           TEXT NOT NULL,
    loser_id            TEXT NOT NULL,
    merge_reason        TEXT,
    merge_rule          TEXT,
    confidence          NUMERIC,
    merged_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Identity cluster: groups of related identifiers before resolution
CREATE TABLE IF NOT EXISTS identity.identity_clusters (
    cluster_id          BIGSERIAL PRIMARY KEY,
    cluster_hash        TEXT NOT NULL,
    identifier_type     TEXT NOT NULL,
    identifier_value    TEXT NOT NULL,
    source_system       TEXT NOT NULL,
    associated_data     JSONB,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_cluster_hash ON identity.identity_clusters(cluster_hash);
