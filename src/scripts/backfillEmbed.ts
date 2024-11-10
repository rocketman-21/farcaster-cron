import { Client } from 'pg';
import { processCasts } from '../lib/embedCasts';
import fs from 'fs';
import path from 'path';

export async function backfillEmbed() {
  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  const lastIdFile = path.resolve(
    __dirname,
    '../timestamps/last_processed_id.txt'
  );
  let lastProcessedId = 0;

  // Read last processed ID if file exists
  if (fs.existsSync(lastIdFile)) {
    lastProcessedId = parseInt(fs.readFileSync(lastIdFile, 'utf-8'), 10);
    console.log(`Resuming from ID: ${lastProcessedId}`);
  }

  try {
    await client.connect();

    const batchSize = 500;
    const totalToProcess = 1e7;
    let totalProcessed = 0;

    while (totalProcessed < totalToProcess) {
      // Fetch data in batches using ID instead of offset
      const res = await client.query(
        'SELECT * FROM production.farcaster_casts WHERE id > $1 ORDER BY id LIMIT $2',
        [lastProcessedId, batchSize]
      );

      if (res.rows.length === 0) {
        console.log('No more casts available to process.');
        break;
      }

      await processCasts(res.rows, client);

      // Update last processed ID
      lastProcessedId = res.rows[res.rows.length - 1].id;
      fs.writeFileSync(lastIdFile, lastProcessedId.toString());

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