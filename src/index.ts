import cron from 'node-cron';
import fs from 'fs';
import path from 'path';

// Import AWS SDK v3 modules
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import {
  bucketName,
  extractTimestampFromKey,
  prefixes,
  s3Client,
} from './lib/s3';
import { processFile } from './lib/processFile';

// Path to store the latest processed timestamps
const timestampDir = path.resolve(__dirname, 'timestamps');
if (!fs.existsSync(timestampDir)) {
  fs.mkdirSync(timestampDir);
}

// Initialize latestProcessedTimestamps for each type
const latestProcessedTimestamps: Record<string, number> = {};
for (const type of Object.keys(prefixes)) {
  const timestampPath = path.resolve(timestampDir, `${type}_timestamp.txt`);
  if (fs.existsSync(timestampPath)) {
    const timestampStr = fs.readFileSync(timestampPath, 'utf-8');
    latestProcessedTimestamps[type] = parseInt(timestampStr, 10) || 0;
  } else {
    latestProcessedTimestamps[type] = 0;
  }
}

// Set min time as 10 minutes ago
const minTime = Date.now() - 10 * 60 * 1000;

const isDev = process.env.NODE_ENV === 'development';
const fiveSeconds = '*/5 * * * * *';
const twoMinutes = '*/2 * * * *';

// Schedule the cron job
cron.schedule(isDev ? fiveSeconds : twoMinutes, async () => {
  console.log('Checking for new Parquet files...');
  console.log({
    startTime: `${new Date(minTime)} (${minTime})`,
    lastProcessed: Object.entries(latestProcessedTimestamps).reduce(
      (acc, [type, timestamp]) => ({
        ...acc,
        [type]: timestamp ? `${new Date(timestamp)} (${timestamp})` : 'never',
      }),
      {}
    ),
  });

  // Process each prefix
  for (const [type, prefix] of Object.entries(prefixes)) {
    console.log(`Processing ${type} Parquet files...`);
    let continuationToken: string | undefined = undefined;

    try {
      let params = {
        Bucket: bucketName,
        Prefix: prefix,
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
            if (
              timestamp > (latestProcessedTimestamps[type] || 0) &&
              timestamp > minTime
            ) {
              // Process the file
              console.log(
                `Processing new file: ${key} with timestamp ${new Date(
                  timestamp
                )}`
              );
              await processFile(key);
              // Update the latest processed timestamp for this type
              latestProcessedTimestamps[type] = timestamp;
              fs.writeFileSync(
                path.resolve(timestampDir, `${type}_timestamp.txt`),
                timestamp.toString()
              );
            }
          }
        }

        continuationToken = data.NextContinuationToken;
      } while (continuationToken);
    } catch (error) {
      console.error(`Error processing ${type} Parquet files:`, error);
    }
  }
});
