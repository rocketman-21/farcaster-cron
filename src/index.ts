import { builderProfiles } from './crons/builder-profiles';
import cron from 'node-cron';
import { casts } from './crons/casts';
import { profiles } from './crons/profiles';
import { downloadProfiles } from './crons/download-profiles';
import { channelMembers } from './crons/channel-members';
import { downloadNounishCitizens } from './crons/download-nounish-citizens';
import { downloadGrants } from './crons/download-grants';

const isDev = process.env.NODE_ENV === 'development';
const fiveSeconds = '*/5 * * * * *';
const twoMinutes = '*/2 * * * *';
const tenMinutes = '*/10 * * * *';
const twoHours = '0 0 */2 * *';
const threeDays = '0 0 */3 * *';
const sevenDays = '0 0 */7 * *';

const schedules: Record<string, { dev: string; prod: string }> = {
  casts: { dev: fiveSeconds, prod: tenMinutes },
  profiles: { dev: fiveSeconds, prod: tenMinutes },
  downloadProfiles: { dev: fiveSeconds, prod: twoHours },
  'channel-members': { dev: fiveSeconds, prod: tenMinutes },
  downloadNounishCitizens: { dev: fiveSeconds, prod: twoHours },
  downloadGrants: { dev: fiveSeconds, prod: twoHours },
  builderProfiles: { dev: fiveSeconds, prod: sevenDays },
};

const isProcessing: Record<string, boolean> = {
  casts: false,
  profiles: false,
  downloadProfiles: false,
  'channel-members': false,
  downloadNounishCitizens: false,
  downloadGrants: false,
  builderProfiles: false,
};

const isEnabled: Record<string, boolean> = {
  casts: true,
  profiles: true,
  downloadProfiles: true,
  'channel-members': true,
  downloadNounishCitizens: true,
  downloadGrants: true,
  builderProfiles: true,
};

const getSchedule = (key: keyof typeof schedules) => {
  return isDev ? schedules[key].dev : schedules[key].prod;
};

// Ingest casts from parquet files in S3
cron.schedule(getSchedule('casts'), async () => {
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
cron.schedule(getSchedule('profiles'), async () => {
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

// Ingest channel members from parquet files in S3
cron.schedule(getSchedule('channel-members'), async () => {
  if (!isEnabled['channel-members']) {
    return;
  }
  if (isProcessing['channel-members']) {
    console.log('Already processing channel members, skipping...');
    return;
  }
  isProcessing['channel-members'] = true;
  await channelMembers();
  isProcessing['channel-members'] = false;
});

// Download nounish citizens to CSV
cron.schedule(getSchedule('downloadNounishCitizens'), async () => {
  if (!isEnabled.downloadNounishCitizens) {
    return;
  }
  if (isProcessing.downloadNounishCitizens) {
    console.log('Already downloading nounish citizens, skipping...');
    return;
  }
  isProcessing.downloadNounishCitizens = true;
  await downloadNounishCitizens();
  isProcessing.downloadNounishCitizens = false;
});

// Download grants to CSV
cron.schedule(getSchedule('downloadGrants'), async () => {
  if (!isEnabled.downloadGrants) {
    return;
  }
  if (isProcessing.downloadGrants) {
    console.log('Already downloading grants, skipping...');
    return;
  }
  isProcessing.downloadGrants = true;
  await downloadGrants();
  isProcessing.downloadGrants = false;
});

// Download profiles to CSV
cron.schedule(getSchedule('downloadProfiles'), async () => {
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

// Generate builder profiles
cron.schedule(getSchedule('builderProfiles'), async () => {
  if (!isEnabled.builderProfiles) {
    return;
  }
  if (isProcessing.builderProfiles) {
    console.log('Already generating builder profiles, skipping...');
    return;
  }
  isProcessing.builderProfiles = true;
  await builderProfiles();
  isProcessing.builderProfiles = false;
});
