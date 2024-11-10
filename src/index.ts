import cron from 'node-cron';
import { casts } from './crons/casts';
import { profiles } from './crons/profiles';
import { downloadProfiles } from './crons/download-profiles';

const isDev = process.env.NODE_ENV === 'development';
const fiveSeconds = '*/5 * * * * *';
const twoMinutes = '*/2 * * * *';
const twentyFourHours = '0 0 */24 * *';

const isProcessing: Record<string, boolean> = {
  casts: false,
  profiles: false,
  downloadProfiles: false,
};

const isEnabled: Record<string, boolean> = {
  casts: false,
  profiles: true,
  downloadProfiles: false,
};

// Ingest casts from parquet files in S3
cron.schedule(isDev ? fiveSeconds : twoMinutes, async () => {
  if (!isEnabled.casts) {
    return;
  }
  if (isProcessing.casts) {
    console.log('Already processing casts, skipping...');
    return;
  }
  isProcessing.casts = true;
  await casts();
  isProcessing.casts = false;
});

// Ingest profiles from parquet files in S3
cron.schedule(isDev ? fiveSeconds : twoMinutes, async () => {
  if (!isEnabled.profiles) {
    return;
  }
  if (isProcessing.profiles) {
    console.log('Already processing profiles, skipping...');
    return;
  }
  isProcessing.profiles = true;
  await profiles();
  isProcessing.profiles = false;
});

// Download profiles to CSV
cron.schedule(isDev ? fiveSeconds : twentyFourHours, async () => {
  if (!isEnabled.downloadProfiles) {
    return;
  }
  if (isProcessing.downloadProfiles) {
    console.log('Already downloading profiles, skipping...');
    return;
  }
  isProcessing.downloadProfiles = true;
  await downloadProfiles();
  isProcessing.downloadProfiles = false;
});
