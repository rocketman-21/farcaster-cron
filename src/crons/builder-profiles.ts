import { Client } from 'pg';
import {
  ensureDataFilesExist,
  getAddressToFid,
  getGrants,
} from '../lib/download-csvs';
import { postBuilderProfileRequest } from '../lib/queue';
import { BuilderProfileJobBody } from '../lib/job';
import { downloadGrants } from './download-grants';
import { downloadProfiles } from './download-profiles';

export const builderProfiles = async () => {
  console.log('Starting builder profiles generation...');

  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  await downloadGrants();
  await downloadProfiles();

  try {
    console.log('Ensuring data files exist...');
    await ensureDataFilesExist();

    console.log('Getting grants...');
    const grants = getGrants();

    const addressToFid = getAddressToFid();
    // Process citizens in batches of 1
    const batchSize = 1;
    for (let i = 0; i < grants.length; i += batchSize) {
      const batch = grants.slice(i, i + batchSize);
      if (batch.length === 0) continue;

      // Process each grant with its own FID
      const jobs: BuilderProfileJobBody[] = batch
        .map((grant) => {
          const fid = addressToFid.get(grant.recipient);
          if (!fid) {
            console.log(`No FID found for address ${grant.recipient}`);
            return null;
          }
          return {
            fid: fid.toString(),
          };
        })
        .filter((job): job is BuilderProfileJobBody => job !== null);

      console.log(`Processing batch of ${jobs.length} builders...`);
      if (jobs.length > 0) {
        await postBuilderProfileRequest(jobs);
      }

      // wait 30 seconds
      await new Promise((resolve) => setTimeout(resolve, 30000));
    }

    console.log('Completed processing all builder profiles');
  } catch (err) {
    console.error(`Error processing builder profiles:`, err);
    throw err;
  } finally {
    await client.end();
    console.log('Database connection closed');
  }
};
