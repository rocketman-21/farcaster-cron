import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { FarcasterProfile } from '../types/types';
import { Writable } from 'stream';
import CSV from 'fast-csv';
import QueryStream from 'pg-query-stream';
import { finished } from 'stream/promises';

let isDownloadingProfiles = false;
let downloadPromise: Promise<void> | null = null;

const downloadProfiles = async (): Promise<void> => {
  if (isDownloadingProfiles) {
    console.log(
      'Profile download is already in progress. Awaiting current download.'
    );
    return downloadPromise!;
  }

  isDownloadingProfiles = true;

  // Create the download promise
  downloadPromise = (async () => {
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
      console.log(`Connected to the database at ${new Date().toISOString()}.`);

      const tmpFile = `${outputFile}.tmp`;

      const writeStream = fs.createWriteStream(tmpFile);
      writeStream.on('error', (err) => {
        console.error('WriteStream Error:', err);
      });

      // Create a CSV formatter with headers
      const csvStream = CSV.format({
        headers: ['fid', 'fname', 'verified_addresses'],
        alwaysWriteHeaders: true, // Ensure headers are written even if no data
      });
      csvStream.on('error', (err) => {
        console.error('CSV Stream Error:', err);
      });
      csvStream.pipe(writeStream);

      console.log('Starting profile download...');

      // Create a query stream
      const query = new QueryStream(
        `SELECT fid, fname, verified_addresses 
         FROM production.farcaster_profile`,
        [],
        { highWaterMark: 10000 }
      );
      const dbStream = client.query(query);
      dbStream.on('error', (err) => {
        console.error('DB Stream Error:', err);
      });

      // Keep track of the number of profiles
      let profileCount = 0;

      // Pipe database rows to CSV
      const processStream = new Writable({
        objectMode: true,
        write(row: FarcasterProfile, encoding, callback) {
          const addresses = Array.isArray(row.verified_addresses)
            ? row.verified_addresses
                .map((addr) => addr.replace(/"/g, ''))
                .join('|')
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
      downloadPromise = null;
      console.log('Database connection closed.');
    }
  })();

  // Return the promise so callers can await it
  return downloadPromise;
};

export { downloadProfiles };
