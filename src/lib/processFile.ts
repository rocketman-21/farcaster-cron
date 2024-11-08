import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { bucketName, getTableNameFromKey } from './s3';
import { postToEmbeddingsQueueRequest } from './queue';
import { JobBody } from './job';

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
    // await runMigrationScripts(tableName, client);

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
    // Fetch data from the staging table
    const res = await client.query('SELECT * FROM staging.farcaster_casts');
    const casts: {
      id: bigint;
      created_at: Date;
      updated_at: Date;
      deleted_at: Date | null;
      timestamp: Date;
      fid: bigint;
      hash: Buffer;
      parent_hash: Buffer | null;
      parent_fid: bigint | null;
      parent_url: string | null;
      text: string;
      embeds: string | null;
      mentions: string | null;
      mentions_positions: string | null;
      root_parent_hash: Buffer | null;
      root_parent_url: string | null;
    }[] = res.rows;

    for (const cast of casts) {
      const payload: JobBody = {
        type: 'cast',
        content: cast.text,
        externalId: cast.id.toString(),
        users: [],
        groups: [],
        tags: [],
      };

      // Include the fid (user ID) in the users array
      if (cast.fid) {
        payload.users.push(cast.fid.toString());

        // Get verified addresses for the cast author
        const profileRes = await client.query(
          'SELECT verified_addresses FROM production.farcaster_profile WHERE fid = $1',
          [cast.fid]
        );
        if (profileRes.rows[0]?.verified_addresses) {
          payload.users.push(...profileRes.rows[0].verified_addresses);
        }
      }

      // Parse mentions and add to users array along with their verified addresses
      if (cast.mentions) {
        try {
          const mentionsArray = JSON.parse(cast.mentions);
          if (Array.isArray(mentionsArray)) {
            for (const mention of mentionsArray) {
              payload.users.push(mention.toString());

              // Get verified addresses for mentioned users
              const mentionProfileRes = await client.query(
                'SELECT verified_addresses FROM production.farcaster_profile WHERE fid = $1',
                [mention]
              );
              if (mentionProfileRes.rows[0]?.verified_addresses) {
                payload.users.push(
                  ...mentionProfileRes.rows[0].verified_addresses
                );
              }
            }
          }
        } catch (error) {
          console.error(`Error parsing mentions for cast ${cast.id}:`, error);
        }
      }

      // Add groups based on root_parent_url
      if (cast.root_parent_url) {
        payload.groups.push(cast.root_parent_url);
      }

      // Map root_parent_url to tags
      const tagMappings: Record<string, string[]> = {
        'https://warpcast.com/~/channel/flows': ['flows'],
        'chain://eip155:1/erc721:0x9c8ff314c9bc7f6e59a9d9225fb22946427edc03': [
          'grants',
        ],
        'https://warpcast.com/~/channel/yellow': ['drafts'],
        // Add additional mappings as needed
      };

      if (cast.root_parent_url && tagMappings[cast.root_parent_url]) {
        payload.tags.push(...tagMappings[cast.root_parent_url]);
      }

      try {
        await postToEmbeddingsQueueRequest(payload);
        console.log(`Successfully posted cast ${cast.id} to embeddings queue`);
      } catch (err) {
        console.error(
          `Failed to post cast ${cast.id} to embeddings queue:`,
          err
        );
      }
    }
  }
}
