import {
  timestampDir,
  initializeTimestamps,
  processParquetFiles,
  logProcessingStatus,
} from './utils';
import { prefixes } from '../lib/s3';

// Set min time as 1 week ago
const minTime = Date.now() - 7 * 24 * 60 * 60 * 1000;

export const profiles = async () => {
  const latestProcessedTimestamps = initializeTimestamps();
  logProcessingStatus(minTime, latestProcessedTimestamps);

  // Process each prefix
  const type = 'profiles';
  const prefix = prefixes.profiles;
  await processParquetFiles(type, prefix, latestProcessedTimestamps, minTime);
};
