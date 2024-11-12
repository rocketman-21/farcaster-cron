import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { FarcasterProfile } from '../types/types';

const downloadProfiles = async () => {
  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  try {
    await client.connect();

    // Create output directory if it doesn't exist
    const outputDir = path.resolve(__dirname, '../data');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const outputFile = path.resolve(outputDir, 'profiles.csv');
    const writeStream = fs.createWriteStream(outputFile);

    // Write CSV header
    writeStream.write('fid,fname,verified_addresses\n');

    const batchSize = 10000;
    let offset = 0;
    let hasMore = true;

    console.log('Starting profile download...');

    while (hasMore) {
      const result = await client.query<
        Pick<FarcasterProfile, 'fid' | 'fname' | 'verified_addresses'>
      >(
        `SELECT fid, fname, verified_addresses 
         FROM production.farcaster_profile 
         ORDER BY fid 
         LIMIT $1 OFFSET $2`,
        [batchSize, offset]
      );

      if (result.rows.length === 0) {
        hasMore = false;
        continue;
      }

      // Process each row and write to CSV
      result.rows.forEach((row) => {
        const addresses = Array.isArray(row.verified_addresses)
          ? row.verified_addresses.join('|')
          : '';
        writeStream.write(`${row.fid},${row.fname || ''},${addresses}\n`);
      });

      console.log(`Processed ${offset + result.rows.length} profiles`);
      offset += batchSize;
    }

    writeStream.end();
    console.log(`Profile download complete. File saved to: ${outputFile}`);
  } catch (err) {
    console.error('Error downloading profiles:', err);
  } finally {
    await client.end();
  }
};

export { downloadProfiles };
