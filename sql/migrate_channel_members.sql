-- Create the schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS production;

-- -- Create the production table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS production.farcaster_channel_members (
--     id BIGINT PRIMARY KEY,
--     created_at TIMESTAMP,
--     updated_at TIMESTAMP,
--     deleted_at TIMESTAMP,
--     timestamp TIMESTAMP,
--     fid BIGINT,
--     channel_id TEXT
-- );

-- -- Create index on fid
-- CREATE INDEX IF NOT EXISTS idx_production_channel_members_fid
-- ON production.farcaster_channel_members (fid);

-- -- Create index on channel_id
-- CREATE INDEX IF NOT EXISTS idx_production_channel_members_channel_id
-- ON production.farcaster_channel_members (channel_id);

-- -- Create the staging schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS staging;

-- -- Create the staging table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS staging.farcaster_channel_members (
--     id BIGINT,
--     created_at TIMESTAMP,
--     updated_at TIMESTAMP,
--     deleted_at TIMESTAMP,
--     timestamp TIMESTAMP,
--     fid BIGINT,
--     channel_id TEXT
-- );

-- -- Create index on fid
-- CREATE INDEX IF NOT EXISTS idx_staging_channel_members_fid
-- ON staging.farcaster_channel_members (fid);

-- Migration script with temporary table for deduplication
DO $$
DECLARE
    batch_size INTEGER := 10000;
    last_id BIGINT := 0;
    current_max_id BIGINT;
BEGIN
    -- Create a temporary table with deduplicated data
    CREATE TEMP TABLE temp_deduplicated_channel_members AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY updated_at DESC, ctid ASC
            ) AS rn
        FROM staging.farcaster_channel_members
    ) sub
    WHERE rn = 1;

    -- Create an index on the temporary table
    CREATE INDEX idx_temp_channel_members_id ON temp_deduplicated_channel_members(id);

    -- Process data in batches using the temporary table
    LOOP
        -- Get the maximum id in the current batch
        SELECT MAX(id) INTO current_max_id
        FROM (
            SELECT id
            FROM temp_deduplicated_channel_members
            WHERE id > last_id
            ORDER BY id
            LIMIT batch_size
        ) sub;

        -- Exit the loop if no more records
        EXIT WHEN current_max_id IS NULL;

        -- Process the current batch and convert timestamps
        INSERT INTO production.farcaster_channel_members (
            id,
            created_at,
            updated_at,
            deleted_at,
            timestamp,
            fid,
            channel_id
        )
        SELECT
            id,
            created_at,
            updated_at,
            deleted_at,
            timestamp,
            fid,
            channel_id
        FROM
            temp_deduplicated_channel_members
        WHERE
            id > last_id 
            AND id <= current_max_id
        ORDER BY
            id
        ON CONFLICT (id) DO UPDATE SET
            created_at = EXCLUDED.created_at,
            updated_at = EXCLUDED.updated_at,
            deleted_at = EXCLUDED.deleted_at,
            timestamp = EXCLUDED.timestamp,
            fid = EXCLUDED.fid,
            channel_id = EXCLUDED.channel_id;

        -- Update last_id for the next batch
        last_id := current_max_id;
    END LOOP;

    -- Drop the temporary table
    DROP TABLE IF EXISTS temp_deduplicated_channel_members;

END $$ LANGUAGE plpgsql;