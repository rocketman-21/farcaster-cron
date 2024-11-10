import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { bucketName, getTableNameFromKey, IngestionType } from './s3';
import { processCastsFromStagingTable } from './processCasts';

// Function to process a single file
export async function processFile(key: string, type: IngestionType) {
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

    if (type === 'casts') {
      await processCastsFromStagingTable(type, client);
    }

    // Run migration scripts if necessary
    await runMigrationScripts(tableName, client);
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
