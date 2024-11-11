import { Client } from 'pg';
import { embedCasts } from '../lib/embedCasts';

let lastProcessedId = 0;

export async function backfillEmbed() {
  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  try {
    await client.connect();

    // AND root_parent_url IN (
    //   'https://warpcast.com/~/channel/vrbs',
    //   'chain://eip155:1/erc721:0x9c8ff314c9bc7f6e59a9d9225fb22946427edc03',
    //   'chain://eip155:1/erc721:0x558bfff0d583416f7c4e380625c7865821b8e95c',
    //   'https://warpcast.com/~/channel/flows',
    //   'https://warpcast.com/~/channel/yellow'
    // )
    const batchSize = 500;
    const totalToProcess = 1e7;
    let totalProcessed = 0;
    while (totalProcessed < totalToProcess) {
      // Fetch data in batches using ID instead of offset
      const res = await client.query(
        `SELECT * FROM production.farcaster_casts 
        WHERE id > $1 
        AND root_parent_url = 'https://warpcast.com/~/channel/flows'
        ORDER BY id 
        LIMIT $2`,
        [lastProcessedId, batchSize]
      );

      if (res.rows.length === 0) {
        console.log('No more casts available to process.');
        break;
      }

      await embedCasts(res.rows);

      // Update last processed ID
      lastProcessedId = res.rows[res.rows.length - 1].id;
      console.log(
        `Successfully embedded batch of ${res.rows.length} casts (last ID: ${lastProcessedId})`
      );

      totalProcessed += res.rows.length;
    }
  } catch (err) {
    console.error(`Error processing casts:`, err);
  } finally {
    await client.end();
  }
}
