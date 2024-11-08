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
    root_parent_url TEXT
);

-- Create the staging schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS staging;

-- Create the staging table if it doesn't exist
CREATE TABLE IF NOT EXISTS staging.farcaster_casts (
    id BIGINT,
    created_at BIGINT,
    updated_at BIGINT,
    deleted_at BIGINT,
    timestamp BIGINT,
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
    root_parent_url TEXT
);

-- Create index on fid
CREATE INDEX IF NOT EXISTS idx_staging_cast_fid
ON staging.farcaster_casts (fid);

-- Migration script with temporary table for deduplication
DO $$
DECLARE
    batch_size INTEGER := 10000;
    last_id BIGINT := 0;
    current_max_id BIGINT;
BEGIN
    -- Create a temporary table with deduplicated data
    CREATE TEMP TABLE temp_deduplicated_casts AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at DESC, ctid ASC
            ) AS rn
        FROM staging.farcaster_casts
    ) sub
    WHERE rn = 1;

    -- Create an index on the temporary table
    CREATE INDEX idx_temp_cast_id ON temp_deduplicated_casts(id);

    -- Process data in batches using the temporary table
    LOOP
        -- Get the maximum id in the current batch
        SELECT MAX(id) INTO current_max_id
        FROM (
            SELECT id
            FROM temp_deduplicated_casts
            WHERE id > last_id
            ORDER BY id
            LIMIT batch_size
        ) sub;

        -- Exit the loop if no more records
        EXIT WHEN current_max_id IS NULL;

        -- Process the current batch and convert timestamps
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
            mentions,
            mentions_positions,
            root_parent_hash,
            root_parent_url
        )
        SELECT
            id,
            TO_TIMESTAMP(created_at / 1000000.0),
            TO_TIMESTAMP(updated_at / 1000000.0),
            TO_TIMESTAMP(deleted_at / 1000000.0),
            TO_TIMESTAMP(timestamp / 1000000.0),
            fid,
            hash,
            parent_hash,
            parent_fid,
            parent_url,
            text,
            embeds,
            mentions,
            mentions_positions,
            root_parent_hash,
            root_parent_url
        FROM
            temp_deduplicated_casts
        WHERE
            id > last_id AND id <= current_max_id
        ORDER BY
            id
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
            mentions = EXCLUDED.mentions,
            mentions_positions = EXCLUDED.mentions_positions,
            root_parent_hash = EXCLUDED.root_parent_hash,
            root_parent_url = EXCLUDED.root_parent_url;

        -- Update last_id for the next batch
        last_id := current_max_id;
    END LOOP;

    -- Drop the temporary table
    DROP TABLE IF EXISTS temp_deduplicated_casts;

END $$ LANGUAGE plpgsql; 