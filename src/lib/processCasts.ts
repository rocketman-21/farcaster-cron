import { Client } from 'pg';
import { IngestionType } from './s3';
import { embedCasts } from './embedCasts';

// Function to process casts after migration
export async function processCastsFromStagingTable(
  type: IngestionType,
  client: Client
) {
  if (type === 'casts') {
    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    while (hasMore) {
      // Fetch data in batches
      const res = await client.query(
        'SELECT * FROM staging.farcaster_casts ORDER BY id LIMIT $1 OFFSET $2',
        [batchSize, offset]
      );

      if (res.rows.length === 0) {
        hasMore = false;
        continue;
      }

      await embedCasts(res.rows);
      console.log(
        `Successfully processed batch of ${res.rows.length} casts (offset: ${offset})`
      );

      offset += batchSize;
    }
  }
}
