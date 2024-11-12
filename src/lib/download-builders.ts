import fs from 'fs';
import path from 'path';
import { downloadProfiles } from '../crons/download-profiles';
import { downloadNounishCitizens } from '../crons/download-nounish-citizens';

export const ensureDataFilesExist = async () => {
  // Create data directory if it doesn't exist
  const dataDir = path.resolve(__dirname, '../data');
  if (!fs.existsSync(dataDir)) {
    fs.mkdirSync(dataDir, { recursive: true });
  }

  // Check and download profiles if needed
  const profilesPath = path.resolve(dataDir, 'profiles.csv');
  if (!fs.existsSync(profilesPath)) {
    console.log('Profiles file not found, downloading profiles first...');
    await downloadProfiles();
  }

  // Check and download nounish citizens if needed
  const nounishPath = path.resolve(dataDir, 'nounish-citizens.csv');
  if (!fs.existsSync(nounishPath)) {
    console.log('Nounish citizens file not found, downloading first...');
    await downloadNounishCitizens();
  }
};
