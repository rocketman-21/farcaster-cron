import { Pool } from 'pg';

const pool = new Pool({
  connectionString: process.env.DB_URL,
  max: 100,
});

export async function fixMentions() {
  const startTime = Date.now();
  console.log('Starting mentions fix...');

  const maxConcurrentBatches = 100;
  const startId = 5858872174;
  const endId = 6000000000;
  const batchRange = 50000;

  try {
    for (
      let currentStart = startId;
      currentStart < endId;
      currentStart += batchRange
    ) {
      const currentEnd = Math.min(currentStart + batchRange, endId);
      console.log(`Processing ID range ${currentStart} to ${currentEnd}`);

      // Step 1: Fetch all IDs that need processing
      const client = await pool.connect();

      const { rows } = await client.query(
        `
        SELECT id FROM "production"."farcaster_casts" 
        WHERE id BETWEEN $1 AND $2 AND mentions IS NOT NULL
        `,
        [currentStart, currentEnd]
      );

      client.release();

      const totalRecords = rows.length;
      const batchSize = Math.ceil(totalRecords / 200); // Split into 1000 batches

      // Step 2: Divide IDs into batches
      const batches: number[][] = [];
      for (let i = 0; i < totalRecords; i += batchSize) {
        const batch = rows.slice(i, i + batchSize).map((row) => row.id);
        batches.push(batch.sort((a, b) => a - b));
      }

      // Step 3: Process batches with controlled concurrency
      let batchIndex = 0;
      while (batchIndex < batches.length) {
        const currentBatches = batches.slice(
          batchIndex,
          batchIndex + maxConcurrentBatches
        );

        const batchPromises = currentBatches.map((batch) =>
          processBatch(batch)
        );

        await Promise.all(batchPromises);

        batchIndex += currentBatches.length;

        const elapsedMinutes = ((Date.now() - startTime) / 1000 / 60).toFixed(
          2
        );
        console.log(
          `Completed ${batchIndex} batches out of ${batches.length} (${elapsedMinutes} minutes elapsed)`
        );
      }

      const totalMinutes = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
      console.log(
        `Completed range ${currentStart}-${currentEnd} in ${totalMinutes} minutes`
      );
    }

    const finalMinutes = ((Date.now() - startTime) / 1000 / 60).toFixed(2);
    console.log(
      `Mentions fix completed successfully in ${finalMinutes} minutes`
    );
  } catch (error) {
    console.error('An error occurred during the mentions fix process:', error);
  } finally {
    await pool.end();
  }
}

async function processBatch(batchIds: number[]) {
  const client = await pool.connect();

  try {
    console.log(
      `Processing batch for IDs: ${batchIds[0]} to ${batchIds.slice(-1)[0]}...`
    );

    const result = await client.query(
      `
      WITH updated_data AS (
        SELECT
          id,
          (
            SELECT array_agg((value::text)::BIGINT)
            FROM jsonb_array_elements_text(fc.mentions::jsonb)
          ) AS mentioned_fids,
          string_to_array(trim(fc.mentions_positions, '[]'), ',')::INT[] AS mentions_positions_array
        FROM "production"."farcaster_casts" fc
        WHERE id BETWEEN $1 AND $2 AND mentions IS NOT NULL
      )
      UPDATE "production"."farcaster_casts" AS fc
      SET
        mentioned_fids = ud.mentioned_fids,
        mentions_positions_array = ud.mentions_positions_array
      FROM updated_data ud
      WHERE fc.id = ud.id;
      `,
      [batchIds[0], batchIds.slice(-1)[0]]
    );

    console.log(
      `Successfully processed ${result.rowCount} rows in batch IDs ${
        batchIds[0]
      }-${batchIds.slice(-1)[0]}`
    );
  } catch (error) {
    console.error(
      `Failed to process batch IDs ${batchIds[0]}-${batchIds.slice(-1)[0]}:`,
      error
    );
    throw error;
  } finally {
    client.release();
  }
}
