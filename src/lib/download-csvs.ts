import fs from 'fs';
import path from 'path';
import { downloadProfiles } from '../crons/download-profiles';
import { downloadNounishCitizens } from '../crons/download-nounish-citizens';
import { downloadGrants } from '../crons/download-grants';
import { parse } from 'csv-parse/sync';

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

  // Check and download grants if needed
  const grantsPath = path.resolve(dataDir, 'grants.csv');
  if (!fs.existsSync(grantsPath)) {
    console.log('Grants file not found, downloading first...');
    await downloadGrants();
  }
};

// Helper function to get profiles from CSV
export const getFidToVerifiedAddresses = () => {
  const profilesPath = path.resolve(__dirname, '../data/profiles.csv');
  const profiles = new Map<string, string[]>();

  const lines = fs.readFileSync(profilesPath, 'utf-8').split('\n');
  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const [fid, _, addresses] = line.split(',');
    profiles.set(fid, addresses ? addresses.split('|') : []);
  }

  return profiles;
};

// Helper function to get grants from CSV
export const getGrants = () => {
  // Read and parse grants CSV
  const csvPath = path.resolve(__dirname, '../data/grants.csv');
  const csvContent = fs.readFileSync(csvPath, 'utf-8');
  const grants = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
  });
  return grants;
};

// Helper function to get nounish citizens from CSV
export const getNounishCitizens = () => {
  // Read and parse nounish citizens CSV
  const csvPath = path.resolve(__dirname, '../data/nounish-citizens.csv');
  const csvContent = fs.readFileSync(csvPath, 'utf-8');
  const nounishCitizens = parse(csvContent, {
    columns: true,
    skip_empty_lines: true,
  });
  return nounishCitizens;
};
