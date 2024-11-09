import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { bucketName, getTableNameFromKey } from './s3';
import { processCasts } from './embedCasts';

// Function to process a single file
export async function processFile(key: string) {
  const s3Url = `s3://${bucketName}/${key}`;

  // Ingest the Parquet file into PostgreSQL
  const tableName = getTableNameFromKey(key);
  const copyCommand = `
    COPY staging.${tableName}
    FROM '${s3Url}'
    WITH (format 'parquet');
  `;

  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  try {
    await client.connect();
    await client.query(copyCommand);
    console.log(`Successfully ingested file: ${key}`);

    // Run migration scripts if necessary
    await runMigrationScripts(tableName, client);

    if (tableName === 'farcaster_casts') {
      await processCastsAfterMigration(tableName, client);
    }
  } catch (err) {
    console.error(`Error ingesting file ${key}:`, err);
  } finally {
    await client.end();
  }
}

// Function to run migration scripts based on the table name
async function runMigrationScripts(tableName: string, client: Client) {
  const migrations: Record<string, string> = {
    farcaster_profile_with_addresses: 'migrate_profiles.sql',
    farcaster_casts: 'migrate_casts.sql',
  };

  const migrationFile = migrations[tableName];
  if (migrationFile) {
    const migrateScript = fs.readFileSync(
      path.resolve(process.cwd(), 'sql', migrationFile),
      'utf-8'
    );
    await client.query(migrateScript);
    console.log(`Migration script executed for ${tableName}`);
  }
}

// Function to process casts after migration
async function processCastsAfterMigration(tableName: string, client: Client) {
  if (tableName === 'farcaster_casts') {
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      // Fetch data in batches
      const res = await client.query(
        'SELECT * FROM staging.farcaster_casts ORDER BY id LIMIT $1 OFFSET $2',
        [batchSize, offset]
      );

      if (res.rows.length === 0) {
        hasMore = false;
        continue;
      }

      await processCasts(res.rows, client);
      console.log(
        `Successfully processed batch of ${res.rows.length} casts (offset: ${offset})`
      );

      offset += batchSize;
    }
  }
}
