import { Client } from 'pg';
import { embedProductionCasts } from '../embedding/embed-casts';
import { FarcasterCast } from '../../types/types';
import { getFidToFname, getFidToVerifiedAddresses } from '../download-csvs';

export async function backfillCastEmbeds(members: { fid: number }[]) {
  console.log('Starting cast embed backfill for new members...');
  const fidToFname = getFidToFname();
  const profiles = getFidToVerifiedAddresses();

  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  try {
    await client.connect();
    console.log('Successfully connected to database');

    // Get unique FIDs from members
    const uniqueFids = Array.from(new Set(members.map((m) => m.fid))).sort(
      (a, b) => b - a
    );
    console.log(`Processing casts for ${uniqueFids.length} unique FIDs`);

    const batchSize = 1000;
    let totalProcessed = 0;
    const startTime = Date.now();

    // Process 2 FIDs at a time for efficiency
    for (let i = 0; i < uniqueFids.length; i += 2) {
      const currentFids = uniqueFids.slice(i, i + 2);
      let lastProcessedId = 0;
      console.log(`\nStarting batch for FIDs: ${currentFids.join(', ')}`);

      while (true) {
        const queryStartTime = Date.now();
        const res = await client.query<
          FarcasterCast & { author_fname: string }
        >(
          `SELECT c.*, p.fname as author_fname 
          FROM production.farcaster_casts c
          LEFT JOIN production.farcaster_profile p ON c.fid = p.fid
          WHERE c.id > $1 
          AND c.fid = ANY($2::bigint[])
          AND c.parent_hash IS NULL
          ORDER BY c.id 
          LIMIT $3`,
          [lastProcessedId, currentFids, batchSize]
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

        lastProcessedId = res.rows[res.rows.length - 1].id;
        totalProcessed += res.rows.length;
        const queryDuration = Date.now() - queryStartTime;
        const avgTimePerCast = queryDuration / res.rows.length;

        console.log(
          `Batch stats:
          - Processed ${res.rows.length} casts for FIDs ${currentFids.join(
            ', '
          )}
          - Last ID: ${lastProcessedId}
          - Total processed: ${totalProcessed}
          - Query duration: ${queryDuration}ms
          - Embed duration: ${embedDuration}ms
          - Avg time per cast: ${avgTimePerCast.toFixed(2)}ms`
        );
      }
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
    throw err;
  } finally {
    console.log('Closing database connection...');
    await client.end();
    console.log('Database connection closed');
  }
}
