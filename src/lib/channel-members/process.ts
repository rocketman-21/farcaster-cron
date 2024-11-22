import { Client } from 'pg';
import { IngestionType } from '../s3';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import { backfillCastEmbeds } from '../backfill-members/backfill-member-casts';
import { ChannelMember } from '../../types/types';
import { NounishCitizen } from '../../types/types';
import { nounishChannels } from '../channels';

// Function to process members after migration
export async function processMembersFromStagingTable(
  type: IngestionType,
  client: Client
) {
  if (type === 'channel-members') {
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
      // Fetch data in batches from staging that don't exist in production
      const res = await client.query<ChannelMember>(
        `SELECT s.* FROM staging.farcaster_channel_members s
         LEFT JOIN production.farcaster_channel_members p ON s.id = p.id
         WHERE p.id IS NULL
         AND s.channel_id = ANY($1)
         ORDER BY s.id LIMIT $2 OFFSET $3`,
        [nounishChannels, batchSize, offset]
      );

      if (res.rows.length === 0) {
        hasMore = false;
        continue;
      }

      console.log(`Processing batch of ${res.rows.length} members`);

      // Filter for nounish citizens
      const filteredRows = res.rows.filter((row) => {
        const fid = Number(row.fid);
        return nounishFids.has(fid);
      });

      if (filteredRows.length > 0) {
        await backfillCastEmbeds(filteredRows);
        console.log(
          `Successfully processed batch of ${filteredRows.length} members (offset: ${offset}, non-filtered: ${res.rows.length})`
        );
      }

      offset += batchSize;
    }
  }
}
