import fs from 'fs';
import path from 'path';
import { ListObjectsV2Command } from '@aws-sdk/client-s3';
import {
  bucketName,
  extractTimestampFromKey,
  IngestionType,
  prefixes,
  s3Client,
} from '../lib/s3';
import { processFile } from '../lib/parquet-file/process';

// Path to store the latest processed timestamps
export const timestampDir = path.resolve(__dirname, 'timestamps');
if (!fs.existsSync(timestampDir)) {
  fs.mkdirSync(timestampDir);
}

// Initialize latestProcessedTimestamps for each type
export const initializeTimestamps = () => {
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
  return latestProcessedTimestamps;
};

// Process files for a given type
export const processParquetFiles = async (
  type: IngestionType,
  prefix: string,
  latestProcessedTimestamps: Record<string, number>,
  minTime: number
) => {
  const processedKeys = new Set<string>();

  console.log(`Processing ${type} Parquet files...`);
  let continuationToken: string | undefined = undefined;

  try {
    let params = {
      Bucket: bucketName,
      Prefix: prefix,
      MaxKeys: 1000,
      ContinuationToken: continuationToken as string | undefined,
    };

    do {
      if (continuationToken) {
        params.ContinuationToken = continuationToken;
      }

      const data = await s3Client.send(new ListObjectsV2Command(params));
      const objects = data.Contents || [];

      for (let i = 0; i < objects.length; i++) {
        const item = objects[i];
        const key = item.Key!;
        if (key.endsWith('.parquet')) {
          const timestamp = extractTimestampFromKey(key);
          if (
            timestamp > (latestProcessedTimestamps[type] || 0) &&
            timestamp > minTime &&
            !processedKeys.has(key)
          ) {
            processedKeys.add(key);
            // Update the latest processed timestamp for this type
            latestProcessedTimestamps[type] = timestamp;
            fs.writeFileSync(
              path.resolve(timestampDir, `${type}_timestamp.txt`),
              timestamp.toString()
            );

            // Process the file
            console.log(
              `Processing new file: ${key} with timestamp ${new Date(
                timestamp
              )}`
            );
            await processFile(key, type);
          }
        }
      }

      continuationToken = data.NextContinuationToken;
    } while (continuationToken);
  } catch (error) {
    console.error(`Error processing ${type} Parquet files:`, error);
  }
};

// Log processing status
export const logProcessingStatus = (
  minTime: number,
  latestProcessedTimestamps: Record<string, number>
) => {
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
};
