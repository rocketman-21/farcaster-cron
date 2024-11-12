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
        `SELECT * FROM staging.farcaster_casts 
         WHERE root_parent_url IN (
           'https://warpcast.com/~/channel/vrbs',
           'chain://eip155:1/erc721:0x9c8ff314c9bc7f6e59a9d9225fb22946427edc03',
           'chain://eip155:1/erc721:0x558bfff0d583416f7c4e380625c7865821b8e95c',
           'https://warpcast.com/~/channel/flows',
           'https://warpcast.com/~/channel/yellow'
         )
         ORDER BY id LIMIT $1 OFFSET $2`,
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
