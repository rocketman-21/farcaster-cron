import { Client } from 'pg';
import { IngestionType } from './s3';
import { embedCasts } from './embedCasts';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';

interface NounishCitizen {
  fid: string;
  fname: string;
  channel_id: string;
}

// Function to process casts after migration
export async function processCastsFromStagingTable(
  type: IngestionType,
  client: Client
) {
  if (type === 'casts') {
    // Read and parse nounish citizens CSV
    const csvPath = path.resolve(__dirname, '../data/nounish-citizens.csv');
    const csvContent = fs.readFileSync(csvPath, 'utf-8');
    const nounishCitizens: NounishCitizen[] = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
    });

    // Create set of nounish citizen FIDs for faster lookups
    const nounishFids = new Set(nounishCitizens.map((row) => Number(row.fid)));

    const batchSize = 10000;
    let offset = 0;
    let hasMore = true;
    while (hasMore) {
      // Fetch data in batches
      const res = await client.query(
        `SELECT * FROM staging.farcaster_casts 
         WHERE parent_hash IS NULL
         ORDER BY id LIMIT $1 OFFSET $2`,
        [batchSize, offset]
      );

      if (res.rows.length === 0) {
        hasMore = false;
        continue;
      }

      console.log(`Processing batch of ${res.rows.length} casts`);

      // Filter rows in TypeScript
      const filteredRows = res.rows.filter((row) => {
        const fid = Number(row.fid);
        const validUrls = [
          'https://warpcast.com/~/channel/vrbs',
          'chain://eip155:1/erc721:0x9c8ff314c9bc7f6e59a9d9225fb22946427edc03',
          'chain://eip155:1/erc721:0x558bfff0d583416f7c4e380625c7865821b8e95c',
          'https://warpcast.com/~/channel/flows',
        ];

        // Include cast if either URL matches or author is a nounish citizen
        return validUrls.includes(row.root_parent_url) || nounishFids.has(fid);
      });

      if (filteredRows.length === 0) {
        offset += batchSize;
        continue;
      }

      await embedCasts(filteredRows);
      console.log(
        `Successfully embedded batch of ${filteredRows.length} casts (offset: ${offset}, non-filtered: ${res.rows.length})`
      );

      offset += batchSize;
    }
  }
}
