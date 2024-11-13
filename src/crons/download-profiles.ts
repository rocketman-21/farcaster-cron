import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { FarcasterProfile } from '../types/types';
import { Writable } from 'stream';
import CSV from 'fast-csv';
import QueryStream from 'pg-query-stream';

const downloadProfiles = async () => {
  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });
  // Create output directory if it doesn't exist
  const outputDir = path.resolve(__dirname, '../data');
  if (!fs.existsSync(outputDir)) {
    fs.mkdirSync(outputDir, { recursive: true });
  }
  const outputFile = path.resolve(outputDir, 'profiles.csv');

  try {
    await client.connect();

    const tmpFile = `${outputFile}.tmp`;

    const writeStream = fs.createWriteStream(tmpFile);

    // Create a CSV formatter
    const csvStream = CSV.format({
      headers: ['fid', 'fname', 'verified_addresses'],
    });
    csvStream.pipe(writeStream);

    console.log('Starting profile download...');

    // Create a query stream without ORDER BY
    const query = new QueryStream(
      `SELECT fid, fname, verified_addresses 
       FROM production.farcaster_profile`,
      [], // No query parameters
      { highWaterMark: 10000 }
    );
    const dbStream = client.query(query);

    // Keep track of number of profiles
    let profileCount = 0;

    // Pipe database rows to CSV
    const processStream = new Writable({
      objectMode: true,
      write(row: FarcasterProfile, encoding, callback) {
        const addresses = Array.isArray(row.verified_addresses)
          ? row.verified_addresses.join('|')
          : '';
        csvStream.write({
          fid: row.fid,
          fname: row.fname || '',
          verified_addresses: addresses,
        });
        profileCount++;
        callback();
      },
    });

    dbStream.pipe(processStream);

    // Wait for the stream to finish
    await new Promise<void>((resolve, reject) => {
      processStream.on('finish', resolve);
      processStream.on('error', reject);
      dbStream.on('error', reject);
    });

    csvStream.end();

    // Wait for write stream to finish before renaming
    await new Promise<void>((resolve) => writeStream.on('finish', resolve));

    // Atomically rename tmp file to final filename
    fs.renameSync(tmpFile, outputFile);

    console.log(
      `Profile download complete. ${profileCount} profiles streamed. File saved to: ${outputFile}`
    );
  } catch (err) {
    console.error('Error downloading profiles:', err);
    // Clean up tmp file if it exists
    if (fs.existsSync(`${outputFile}.tmp`)) {
      fs.unlinkSync(`${outputFile}.tmp`);
    }
  } finally {
    await client.end();
  }
};

export { downloadProfiles };
