import { Client } from 'pg';
import { IngestionType } from './s3';
import { embedStagingCasts } from './embedding/embed-casts';
import fs from 'fs';
import path from 'path';
import { parse } from 'csv-parse/sync';
import { NounishCitizen, Grant, StagingFarcasterCast } from '../types/types';
import { checkGrantUpdates } from './is-grant-update';
import { getFidToFname, getFidToVerifiedAddresses } from './download-csvs';
import { getGrants } from './download-csvs';

// Helper function to check if root parent URL is valid
const isValidRootParentUrl = (rootParentUrl: string | null) => {
  const validUrls = [
    'https://warpcast.com/~/channel/vrbs',
    'chain://eip155:1/erc721:0x9c8ff314c9bc7f6e59a9d9225fb22946427edc03',
    'chain://eip155:1/erc721:0x558bfff0d583416f7c4e380625c7865821b8e95c',
    'https://warpcast.com/~/channel/flows',
  ];
  return !!(rootParentUrl && validUrls.includes(rootParentUrl));
};

// Filter function for casts
const filterCasts = (row: StagingFarcasterCast, nounishFids: Set<number>) => {
  const fid = Number(row.fid);
  return isValidRootParentUrl(row.root_parent_url) || nounishFids.has(fid);
};

// Function to process casts after migration
export async function processCastsFromStagingTable(
  type: IngestionType,
  client: Client
) {
  const fidToFname = getFidToFname();
  const fidToVerifiedAddresses = getFidToVerifiedAddresses();

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

    console.log('Processing casts from staging table');

    while (hasMore) {
      const res = await client.query<
        StagingFarcasterCast & { author_fname: string }
      >(
        `SELECT c.*, p.fname as author_fname 
         FROM staging.farcaster_casts c
         LEFT JOIN production.farcaster_profile p ON c.fid = p.fid
         WHERE c.parent_hash IS NULL
         ORDER BY c.id LIMIT $1 OFFSET $2`,
        [batchSize, offset]
      );

      const rows = res.rows;
      hasMore = rows.length > 0;

      if (rows.length === 0) {
        offset += batchSize;
        continue;
      }

      console.log(
        `Processing batch of ${rows.length} casts (offset: ${offset})`
      );

      const filteredRows = rows.filter((row) => filterCasts(row, nounishFids));

      if (filteredRows.length > 0) {
        console.log(
          `Embedding batch of ${filteredRows.length} casts (offset: ${offset}, non-filtered: ${rows.length})`
        );
        await embedStagingCasts(
          filteredRows,
          fidToFname,
          fidToVerifiedAddresses
        );
        console.log(
          `Successfully embedded batch of ${filteredRows.length} casts (offset: ${offset}, non-filtered: ${rows.length})`
        );
      }

      const filteredRowsWithGrantData = getFilteredRowsWithGrantData(rows);

      if (filteredRowsWithGrantData.length > 0) {
        console.log(
          `Checking grant updates for batch of ${filteredRowsWithGrantData.length} casts (offset: ${offset})`
        );
        await checkGrantUpdates(filteredRowsWithGrantData, fidToFname);
        console.log(
          `Successfully checked grant updates for batch of ${filteredRowsWithGrantData.length} casts (offset: ${offset})`
        );
      }

      offset += batchSize;
    }
  }
}

function getFilteredRowsWithGrantData(
  rows: StagingFarcasterCast[]
): StagingFarcasterCast[] {
  const grants = getGrants();
  const profiles = getFidToVerifiedAddresses();

  return rows
    .map((row) => {
      const result = filterGrantRecipients(row, profiles, grants);
      if (!result.isValid) return null;
      return row;
    })
    .filter((row): row is NonNullable<typeof row> => row !== null);
}

function filterGrantRecipients(
  cast: StagingFarcasterCast,
  profiles: Map<string, string[]>,
  grants: Grant[]
): { isValid: boolean } {
  // Get profile for this cast's FID
  const verifiedAddresses = profiles.get(cast.fid.toString());

  if (!verifiedAddresses) {
    console.error(`No profile found for FID ${cast.fid}`);
  }

  if (!verifiedAddresses || verifiedAddresses.length === 0) {
    return { isValid: false };
  }

  // Handle case where verified_addresses is a string instead of array
  const addresses = Array.isArray(verifiedAddresses)
    ? verifiedAddresses
    : [verifiedAddresses];

  // Find all matching grants for this profile's addresses
  const matchingGrants = grants.filter((grant) =>
    addresses.some(
      (address) => address.toLowerCase() === grant.recipient.toLowerCase()
    )
  );

  if (matchingGrants.length === 0) {
    return { isValid: false };
  }

  return {
    isValid: true,
  };
}
