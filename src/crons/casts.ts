import {
  timestampDir,
  initializeTimestamps,
  processParquetFiles,
  logProcessingStatus,
} from './utils';
import { prefixes } from '../lib/s3';
import fs from 'fs';
import path from 'path';
import { downloadProfiles } from './download-profiles';

// Set min time as 20 minutes ago
const minTime = Date.now() - 20 * 60 * 1000;

export const casts = async () => {
  // Check if profiles file exists and download if needed
  const profilesPath = path.resolve(__dirname, '../data/profiles.csv');
  if (!fs.existsSync(profilesPath)) {
    console.log('Profiles file not found, downloading profiles first...');
    await downloadProfiles();
  }

  const latestProcessedTimestamps = initializeTimestamps();
  logProcessingStatus(minTime, latestProcessedTimestamps);

  // Process each prefix
  const type = 'casts';
  const prefix = prefixes.casts;
  await processParquetFiles(type, prefix, latestProcessedTimestamps, minTime);
};
