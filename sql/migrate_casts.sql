-- Create the schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS production;

-- Create the production table if it doesn't exist
CREATE TABLE IF NOT EXISTS production.farcaster_casts (
    id BIGINT PRIMARY KEY,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    timestamp TIMESTAMP,
    fid BIGINT,
    hash BYTEA,
    parent_hash BYTEA,
    parent_fid BIGINT,
    parent_url TEXT,
    text TEXT,
    embeds TEXT,
    mentions TEXT,
    mentions_positions TEXT,
    root_parent_hash BYTEA,
    root_parent_url TEXT,
    computed_tags TEXT[],
    embed_summaries TEXT[],
    mentioned_fids BIGINT[],
    mentions_positions_array INT[]
);

-- Create index on root_parent_hash
CREATE INDEX IF NOT EXISTS idx_production_cast_root_parent_hash
ON production.farcaster_casts (root_parent_hash);

-- Create index on hash
CREATE INDEX IF NOT EXISTS idx_production_cast_hash
ON production.farcaster_casts (hash);

-- Create index on computed_tags
CREATE INDEX IF NOT EXISTS idx_production_cast_computed_tags
ON production.farcaster_casts USING GIN (computed_tags);

-- Create index on fid
CREATE INDEX IF NOT EXISTS idx_production_cast_fid
ON production.farcaster_casts (fid);

-- Create index on parent_hash
CREATE INDEX IF NOT EXISTS idx_production_cast_parent_hash
ON production.farcaster_casts (parent_hash);

-- Create index on root_parent_url
CREATE INDEX IF NOT EXISTS idx_production_cast_root_parent_url
ON production.farcaster_casts (root_parent_url);

-- Create the staging schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

-- Create the staging table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.farcaster_casts (
    id BIGINT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP,
    timestamp TIMESTAMP,
    fid BIGINT,
    hash BYTEA,
    parent_hash BYTEA,
    parent_fid BIGINT,
    parent_url TEXT,
    text TEXT,
    embeds TEXT,
    mentions JSONB,
    mentions_positions TEXT,
    root_parent_hash BYTEA,
    root_parent_url TEXT
);

-- Create index on fid
CREATE INDEX IF NOT EXISTS idx_staging_cast_fid
ON staging.farcaster_casts (fid);

-- Create index on id and updated_at in the staging table
CREATE INDEX IF NOT EXISTS idx_staging_casts_id_updated_at
ON staging.farcaster_casts (id, updated_at DESC);

-- Create function to extract mentioned_fids from mentions jsonb
CREATE OR REPLACE FUNCTION extract_mentioned_fids(mentions JSONB)
RETURNS BIGINT[] AS $$
BEGIN
    IF mentions IS NOT NULL AND jsonb_array_length(mentions) > 0 THEN
        RETURN ARRAY(
            SELECT value::BIGINT
            FROM jsonb_array_elements_text(mentions) AS elems(value)
        );
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- Create function to extract mentions_positions_array from mentions_positions text
CREATE OR REPLACE FUNCTION extract_mentions_positions(positions_text TEXT)
RETURNS INT[] AS $$
BEGIN
    IF positions_text IS NOT NULL AND positions_text <> '[]' THEN
        RETURN string_to_array(
            trim(both '[]' FROM positions_text),
            ','
        )::INT[];
    ELSE
        RETURN NULL;
    END IF;
END;
$$ LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE;

-- Migration script to process staging records

BEGIN;

-- Define deduped records and perform the insert within the CTE
WITH deduped AS (
    SELECT DISTINCT ON (id) *
    FROM staging.farcaster_casts
    ORDER BY id, updated_at DESC
),
inserted AS (
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
        root_parent_hash = EXCLUDED.root_parent_hash,
        root_parent_url = EXCLUDED.root_parent_url,
        mentioned_fids = EXCLUDED.mentioned_fids,
        mentions_positions_array = EXCLUDED.mentions_positions_array
    RETURNING id
)
-- Delete processed records from the staging table
DELETE FROM staging.farcaster_casts
USING deduped
WHERE staging.farcaster_casts.id = deduped.id;

COMMIT;