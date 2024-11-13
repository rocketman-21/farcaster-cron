import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { FarcasterProfile } from '../types/types';
import { Writable } from 'stream';
import CSV from 'fast-csv';
import QueryStream from 'pg-query-stream';
import { finished } from 'stream/promises';

let isDownloadingProfiles = false;

const downloadProfiles = async () => {
  if (isDownloadingProfiles) {
    console.log('Profile download is already in progress.');
    return;
  }

  isDownloadingProfiles = true;

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
    console.log('Connected to the database.');

    const tmpFile = `${outputFile}.tmp`;

    const writeStream = fs.createWriteStream(tmpFile);
    writeStream.on('error', (err) => {
      console.error('WriteStream Error:', err);
    });

    // Create a CSV formatter
    const csvStream = CSV.format({
      headers: ['fid', 'fname', 'verified_addresses'],
    });
    csvStream.on('error', (err) => {
      console.error('CSV Stream Error:', err);
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
    dbStream.on('error', (err) => {
      console.error('DB Stream Error:', err);
    });

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
    processStream.on('error', (err) => {
      console.error('ProcessStream Error:', err);
    });

    dbStream.pipe(processStream);

    // Wait for the stream to finish
    await finished(processStream);

    // End the CSV stream
    csvStream.end();

    // Wait for the CSV stream to finish writing
    await finished(csvStream);

    // Wait for the write stream to finish
    await finished(writeStream);

    // Check if the temporary file exists before renaming
    if (!fs.existsSync(tmpFile)) {
      throw new Error(`Temporary file not found: ${tmpFile}`);
    }

    // Rename the temporary file to the final output file
    fs.renameSync(tmpFile, outputFile);

    console.log(
      `Profile download complete. ${profileCount} profiles streamed. File saved to: ${outputFile}`
    );
  } catch (err) {
    console.error('Error downloading profiles:', err);

    // Clean up tmp file if it exists
    if (fs.existsSync(`${outputFile}.tmp`)) {
      fs.unlinkSync(`${outputFile}.tmp`);
      console.log('Temporary file removed due to an error.');
    }
  } finally {
    await client.end();
    isDownloadingProfiles = false;
    console.log('Database connection closed.');
  }
};

export { downloadProfiles };
