-- -- Create the schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS production;

-- -- Create the production table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS production.farcaster_profile (
--     fname TEXT,
--     display_name TEXT,
--     avatar_url TEXT,
--     bio TEXT,
--     verified_addresses TEXT[],
--     updated_at TIMESTAMP,
--     fid BIGINT PRIMARY KEY
-- );

-- -- Create index on verified_addresses
-- CREATE INDEX IF NOT EXISTS idx_production_verified_addresses
-- ON production.farcaster_profile
-- USING GIN (verified_addresses);

-- -- Create the staging schema if it doesn't exist
-- CREATE SCHEMA IF NOT EXISTS staging;

-- -- Create the staging table if it doesn't exist
-- CREATE TABLE IF NOT EXISTS staging.farcaster_profile_with_addresses (
--     fname TEXT,
--     display_name TEXT,
--     avatar_url TEXT,
--     bio TEXT,
--     verified_addresses TEXT,
--     updated_at TIMESTAMP,
--     fid BIGINT
-- );

-- CREATE INDEX IF NOT EXISTS idx_staging_fid
-- ON staging.farcaster_profile_with_addresses (fid);

-- -- Define the conversion function
-- CREATE OR REPLACE FUNCTION text_to_text_array(input_text TEXT)
-- RETURNS TEXT[] AS $$
-- BEGIN
--     IF input_text IS NULL THEN
--         RETURN NULL;
--     ELSIF input_text LIKE '[%' THEN
--         RETURN (
--             SELECT array_agg(value)
--             FROM jsonb_array_elements_text(input_text::jsonb) AS t(value)
--         );
--     ELSIF input_text LIKE '%,%' THEN
--         RETURN string_to_array(input_text, ',');
--     ELSE
--         RETURN ARRAY[input_text];
--     END IF;
-- END;
-- $$ LANGUAGE plpgsql IMMUTABLE;

-- Migration script with temporary table for deduplication
DO $$
DECLARE
    batch_size INTEGER := 10000;
    last_fid BIGINT := 0;
    current_max_fid BIGINT;
BEGIN
    -- Create a temporary table with deduplicated data
    CREATE TEMP TABLE temp_deduplicated_profiles AS
    SELECT *
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY fid
                ORDER BY updated_at DESC, fname ASC, ctid ASC
            ) AS rn
        FROM staging.farcaster_profile_with_addresses
    ) sub
    WHERE rn = 1;

    -- Create an index on the temporary table
    CREATE INDEX idx_temp_fid ON temp_deduplicated_profiles(fid);

    -- Process data in batches using the temporary table
    LOOP
        -- Get the maximum fid in the current batch
        SELECT MAX(fid) INTO current_max_fid
        FROM (
            SELECT fid
            FROM temp_deduplicated_profiles
            WHERE fid > last_fid
            ORDER BY fid
            LIMIT batch_size
        ) sub;

        -- Exit the loop if no more records
        EXIT WHEN current_max_fid IS NULL;

        -- Process the current batch and convert verified_addresses
        INSERT INTO production.farcaster_profile (
            fname,
            display_name,
            avatar_url,
            bio,
            verified_addresses,
            updated_at,
            fid
        )
        SELECT
            fname,
            display_name,
            avatar_url,
            bio,
            text_to_text_array(verified_addresses),
            updated_at,
            fid
        FROM
            temp_deduplicated_profiles
        WHERE
            fid > last_fid AND fid <= current_max_fid
        ORDER BY
            fid
        ON CONFLICT (fid) DO UPDATE SET
            fname = EXCLUDED.fname,
            display_name = EXCLUDED.display_name,
            avatar_url = EXCLUDED.avatar_url,
            bio = EXCLUDED.bio,
            verified_addresses = EXCLUDED.verified_addresses,
            updated_at = EXCLUDED.updated_at;

        -- Update last_fid for the next batch
        last_fid := current_max_fid;
    END LOOP;

    -- Drop the temporary table
    DROP TABLE IF EXISTS temp_deduplicated_profiles;

END $$ LANGUAGE plpgsql; 