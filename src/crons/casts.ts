import {
  timestampDir,
  initializeTimestamps,
  processParquetFiles,
  logProcessingStatus,
} from './utils';
import { prefixes } from '../lib/s3';

// Set min time as 10 minutes ago
const minTime = Date.now() - 24 * 60 * 60 * 1000;

export const casts = async () => {
  const latestProcessedTimestamps = initializeTimestamps();
  logProcessingStatus(minTime, latestProcessedTimestamps);

  // Process each prefix
  const type = 'casts';
  const prefix = prefixes.casts;
  await processParquetFiles(type, prefix, latestProcessedTimestamps, minTime);
};
