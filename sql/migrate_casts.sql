-- -- Create the schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS production;

-- -- Create the production table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS production.farcaster_casts (
--     id BIGINT PRIMARY KEY,
--     created_at TIMESTAMP,
--     updated_at TIMESTAMP,
--     deleted_at TIMESTAMP,
--     timestamp TIMESTAMP,
--     fid BIGINT,
--     hash BYTEA,
--     parent_hash BYTEA,
--     parent_fid BIGINT,
--     parent_url TEXT,
--     text TEXT,
--     embeds TEXT,
--     embeds_array JSONB,
--     root_parent_hash BYTEA,
--     root_parent_url TEXT,
--     computed_tags TEXT[],
--     embed_summaries TEXT[],
--     mentioned_fids BIGINT[],
--     mentions_positions_array INT[]
-- );

-- -- Create index on root_parent_hash
-- CREATE INDEX IF NOT EXISTS idx_production_cast_root_parent_hash
-- ON production.farcaster_casts (root_parent_hash);

-- -- Create index on hash
-- CREATE INDEX IF NOT EXISTS idx_production_cast_hash
-- ON production.farcaster_casts USING HASH (hash);

-- -- Create index on computed_tags
-- CREATE INDEX IF NOT EXISTS idx_production_cast_computed_tags
-- ON production.farcaster_casts USING GIN (computed_tags);

-- -- Create index on fid
-- CREATE INDEX IF NOT EXISTS idx_production_cast_fid
-- ON production.farcaster_casts (fid);

-- -- Create index on parent_hash
-- CREATE INDEX IF NOT EXISTS idx_production_cast_parent_hash
-- ON production.farcaster_casts (parent_hash);

-- -- Create index on root_parent_url
-- CREATE INDEX IF NOT EXISTS idx_production_cast_root_parent_url
-- ON production.farcaster_casts (root_parent_url);

-- -- Create the staging schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS staging;

-- -- Create the staging table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS staging.farcaster_casts (
--     id BIGINT,
--     created_at TIMESTAMP,
--     updated_at TIMESTAMP,
--     deleted_at TIMESTAMP,
--     timestamp TIMESTAMP,
--     fid BIGINT,
--     hash BYTEA,
--     parent_hash BYTEA,
--     parent_fid BIGINT,
--     parent_url TEXT,
--     text TEXT,
--     embeds TEXT,
--     mentions JSONB,
--     mentions_positions TEXT,
--     root_parent_hash BYTEA,
--     root_parent_url TEXT
-- );

-- -- Create index on fid
-- CREATE INDEX IF NOT EXISTS idx_staging_cast_fid
-- ON staging.farcaster_casts (fid);

-- -- Create index on id and updated_at in the staging table
-- CREATE INDEX IF NOT EXISTS idx_staging_casts_id_updated_at
-- ON staging.farcaster_casts (id, updated_at DESC);

-- -- Optimized extract_mentioned_fids function
-- CREATE OR REPLACE FUNCTION extract_mentioned_fids(mentions JSONB)
-- RETURNS BIGINT[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
--     SELECT array_agg((value::text)::BIGINT)
--     FROM jsonb_array_elements_text(mentions);
-- $$;

-- -- Optimized extract_mentions_positions function
-- CREATE OR REPLACE FUNCTION extract_mentions_positions(positions_text TEXT)
-- RETURNS INT[] LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
--     SELECT string_to_array(
--         trim(positions_text, '[]'),
--         ','
--     )::INT[];
-- $$;

-- -- Optimized extract_embeds_array function
-- CREATE OR REPLACE FUNCTION extract_embeds_array(embeds_text TEXT)
-- RETURNS JSONB LANGUAGE SQL IMMUTABLE PARALLEL SAFE AS $$
--     SELECT CASE
--         WHEN embeds_text IS NOT NULL AND embeds_text <> ''
--         THEN embeds_text::JSONB
--         ELSE NULL
--     END;
-- $$;

-- Migration script to process staging records

BEGIN;

WITH deduped AS (
    SELECT DISTINCT ON (id)
        id,
        created_at,
        updated_at,
        deleted_at,
        timestamp,
        fid,
        hash,
        parent_hash,
        parent_fid,
        parent_url,
        text,
        embeds,
        root_parent_hash,
        root_parent_url,
        mentions,
        mentions_positions
    FROM staging.farcaster_casts
    ORDER BY id, updated_at DESC
)
-- Insert deduplicated data into the production table
INSERT INTO production.farcaster_casts (
    id,
    created_at,
    updated_at,
    deleted_at,
    timestamp,
    fid,
    hash,
    parent_hash,
    parent_fid,
    parent_url,
    text,
    embeds,
    embeds_array,
    root_parent_hash,
    root_parent_url,
    mentioned_fids,
    mentions_positions_array
)
SELECT
    id,
    created_at,
    updated_at,
    deleted_at,
    timestamp,
    fid,
    hash,
    parent_hash,
    parent_fid,
    parent_url,
    text,
    embeds,
    extract_embeds_array(embeds),
    root_parent_hash,
    root_parent_url,
    extract_mentioned_fids(mentions),
    extract_mentions_positions(mentions_positions)
FROM deduped
ON CONFLICT (id) DO UPDATE SET
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at,
    deleted_at = EXCLUDED.deleted_at,
    timestamp = EXCLUDED.timestamp,
    fid = EXCLUDED.fid,
    hash = EXCLUDED.hash,
    parent_hash = EXCLUDED.parent_hash,
    parent_fid = EXCLUDED.parent_fid,
    parent_url = EXCLUDED.parent_url,
    text = EXCLUDED.text,
    embeds = EXCLUDED.embeds,
    embeds_array = EXCLUDED.embeds_array,
    root_parent_hash = EXCLUDED.root_parent_hash,
    root_parent_url = EXCLUDED.root_parent_url,
    mentioned_fids = EXCLUDED.mentioned_fids,
    mentions_positions_array = EXCLUDED.mentions_positions_array
RETURNING id;

COMMIT;