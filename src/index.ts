import cron from 'node-cron';
import { exec } from 'child_process';
import { promisify } from 'util';
import fs from 'fs';
import path from 'path';

// Import AWS SDK v3 modules
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3';

require('dotenv').config();

const execAsync = promisify(exec);

// Configure AWS S3 Client
const s3Client = new S3Client({
  region: 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY || '',
    secretAccessKey: process.env.AWS_SECRET_KEY || '',
  },
});

// S3 bucket and prefixes
const bucketName = 'tf-premium-parquet';
const parquetPrefix =
  'public-postgres/farcaster/v2/incremental/farcaster-profile_with_addresses';

// Path to store the latest processed timestamp
const timestampFilePath = path.resolve(__dirname, 'latest_timestamp.txt');

// Initialize latestProcessedTimestamp from the file
let latestProcessedTimestamp = 0;
if (fs.existsSync(timestampFilePath)) {
  const timestampStr = fs.readFileSync(timestampFilePath, 'utf-8');
  latestProcessedTimestamp = parseInt(timestampStr, 10) || 0;
}

// Set min time as 10 minutes ago
const minTime = Date.now() - 10 * 60 * 1000;

// Schedule the cron job
cron.schedule('*/5 * * * * *', async () => {
  console.log('Checking for new Parquet files...');
  console.log({
    startTime: `${new Date(minTime)} (${minTime})`,
    lastProcessed: latestProcessedTimestamp
      ? `${new Date(latestProcessedTimestamp)} (${latestProcessedTimestamp})`
      : 'never',
  });

  let continuationToken: string | undefined = undefined;

  try {
    let params = {
      Bucket: bucketName,
      Prefix: parquetPrefix,
      MaxKeys: 1000, // Adjust as needed
      ContinuationToken: continuationToken as string | undefined,
    };
    do {
      if (continuationToken) {
        params.ContinuationToken = continuationToken;
      }

      const data = await s3Client.send(new ListObjectsV2Command(params));
      const objects = data.Contents || [];

      // Process the retrieved objects
      for (const item of objects) {
        const key = item.Key!;
        if (key.endsWith('.parquet')) {
          const timestamp = extractTimestampFromKey(key);
          if (timestamp > latestProcessedTimestamp && timestamp > minTime) {
            // Process the file
            console.log(
              `Processing new file: ${key} with timestamp ${new Date(
                timestamp
              )}`
            );
            await processFile(key);
            // Update the latest processed timestamp
            latestProcessedTimestamp = timestamp;
            fs.writeFileSync(timestampFilePath, timestamp.toString());
          }
        }
      }

      continuationToken = data.NextContinuationToken;
    } while (continuationToken);
  } catch (error) {
    console.error('Error processing Parquet files:', error);
  }
});

// Function to extract the timestamp from the S3 key
function extractTimestampFromKey(key: string): number {
  const basename = path.basename(key);
  const match = basename.match(/^farcaster-.+?-\d+-(\d+)\.parquet$/);
  return match ? parseInt(match[1], 10) * 1000 : 0;
}

// Function to process a single file
async function processFile(key: string) {
  const s3Url = `s3://${bucketName}/${key}`;

  // Ingest the Parquet file into PostgreSQL
  const tableName = getTableNameFromKey(key);
  const copyCommand = `
    COPY staging.${tableName}
    FROM '${s3Url}'
    WITH (format 'parquet');
  `;

  // Execute the COPY command using psql
  const psqlCommand = `psql "${process.env.DB_URL}" -c "${copyCommand}"`;

  await execAsync(psqlCommand);

  console.log(`Successfully ingested file: ${key}`);

  // Run migration scripts if necessary
  await runMigrationScripts(tableName);
}

// Helper function to extract the table name from the S3 key
function getTableNameFromKey(key: string): string {
  const basename = path.basename(key);
  const match = basename.match(/^farcaster-(.+?)-\d+-\d+\.parquet$/);
  return match ? `farcaster_${match[1]}` : 'unknown_table';
}

// Function to run migration scripts based on the table name
async function runMigrationScripts(tableName: string) {
  // Add logic to run migration scripts for specific tables
  // For example:
  if (tableName === 'farcaster_profile_with_addresses') {
    const psqlCommand = `psql "${process.env.DB_URL}" -f "${path.resolve(
      process.cwd(),
      'sql',
      'migrate_profiles.sql'
    )}"`;

    await execAsync(psqlCommand);

    console.log(
      'Migration script executed for farcaster_profile_with_addresses'
    );
  }
}
