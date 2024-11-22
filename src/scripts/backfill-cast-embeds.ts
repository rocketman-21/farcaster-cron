import { Client } from 'pg';
import { embedProductionCasts } from '../lib/embedding/embed-casts';
import {
  ensureDataFilesExist,
  getFidToFname,
  getFidToVerifiedAddresses,
} from '../lib/download-csvs';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import { FarcasterCast, NounishCitizen } from '../types/types';

let lastProcessedId = 0;
const lastProcessedFid = 368096;

export async function backfillEmbed() {
  console.log('Starting cast embed backfill...');
  const fidToFname = getFidToFname();
  const profiles = getFidToVerifiedAddresses();

  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  console.log('Ensuring data files exist...');
  await ensureDataFilesExist();

  try {
    console.log('Connecting to database...');
    await client.connect();
    console.log('Successfully connected to database');

    // Read and parse nounish citizens CSV
    console.log('Reading nounish citizens CSV...');
    const csvPath = path.resolve(__dirname, '../data/nounish-citizens.csv');
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const nounishCitizens: NounishCitizen[] = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
    });
    console.log(`Found ${nounishCitizens.length} nounish citizens`);

    // Get array of unique FIDs greater than 512534
    const uniqueFids = Array.from(
      new Set(nounishCitizens.map((c) => Number(c.fid)))
    )
      .filter((fid) => fid < lastProcessedFid)
      .sort((a, b) => b - a);
    console.log(`Processing ${uniqueFids.length} unique FIDs`);

    const batchSize = 1000;
    const totalToProcess = 1e7;
    let totalProcessed = 0;
    const startTime = Date.now();

    // Process 2 FIDs at a time
    for (let i = 0; i < uniqueFids.length; i += 2) {
      const currentFids = uniqueFids.slice(i, i + 2);
      let currentLastId = lastProcessedId;
      console.log(`\nStarting batch for FIDs: ${currentFids.join(', ')}`);

      while (totalProcessed < totalToProcess) {
        const queryStartTime = Date.now();
        const res = await client.query<FarcasterCast>(
          `SELECT * FROM production.farcaster_casts 
          WHERE id > $1 
          AND fid = ANY($2::bigint[])
          AND parent_hash IS NULL
          ORDER BY id 
          LIMIT $3`,
          [currentLastId, currentFids, batchSize]
        );

        if (res.rows.length === 0) {
          console.log(
            `No more casts found for FIDs: ${currentFids.join(', ')}`
          );
          break;
        }

        console.log(`Embedding ${res.rows.length} casts...`);
        const embedStartTime = Date.now();
        await embedProductionCasts(res.rows, fidToFname, profiles);
        const embedDuration = Date.now() - embedStartTime;

        currentLastId = res.rows[res.rows.length - 1].id;
        totalProcessed += res.rows.length;
        const queryDuration = Date.now() - queryStartTime;
        const avgTimePerCast = queryDuration / res.rows.length;

        console.log(
          `Batch stats:
          - Processed ${res.rows.length} casts for FIDs ${currentFids.join(
            ', '
          )}
          - Last ID: ${currentLastId}
          - Total processed: ${totalProcessed}
          - Query duration: ${queryDuration}ms
          - Embed duration: ${embedDuration}ms
          - Avg time per cast: ${avgTimePerCast.toFixed(2)}ms`
        );
      }

      // Reset lastProcessedId for next FID batch
      lastProcessedId = currentFids[1] || currentFids[0]; // Use first FID if second doesn't exist
      console.log(`Completed processing for FIDs: ${currentFids.join(', ')}`);
    }

    const totalDuration = (Date.now() - startTime) / 1000;
    console.log(`\nBackfill complete:
    - Total casts processed: ${totalProcessed}
    - Total duration: ${totalDuration.toFixed(2)}s
    - Average rate: ${(totalProcessed / totalDuration).toFixed(
      2
    )} casts/second`);
  } catch (err) {
    console.error(`Error processing casts:`, err);
  } finally {
    console.log('Closing database connection...');
    await client.end();
    console.log('Database connection closed');
  }
}
