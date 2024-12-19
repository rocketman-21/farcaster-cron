import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { Grant } from '../types/types';
import { Writable } from 'stream';
import CSV from 'fast-csv';
import QueryStream from 'pg-query-stream';
import { finished } from 'stream/promises';

let isDownloadingGrants = false;
let downloadPromise: Promise<void> | null = null;

const downloadGrants = async (): Promise<void> => {
  if (isDownloadingGrants) {
    console.log(
      'Grants download is already in progress. Awaiting current download.'
    );
    return downloadPromise!;
  }

  isDownloadingGrants = true;

  // Create the download promise
  downloadPromise = (async () => {
    // Create a new PostgreSQL client
    const client = new Client({
      connectionString: process.env.FLOWS_DB_URL,
    });

    // Create output directory if it doesn't exist
    const outputDir = path.resolve(__dirname, '../data');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    const outputFile = path.resolve(outputDir, 'grants.csv');

    try {
      await client.connect();

      const tmpFile = `${outputFile}.tmp`;

      const writeStream = fs.createWriteStream(tmpFile);

      // Create a CSV formatter
      const csvStream = CSV.format({
        headers: ['id', 'recipient', 'description', 'parent_contract'],
      });
      csvStream.pipe(writeStream);

      console.log('Starting grants download...');

      // Create a query stream
      const query = new QueryStream(
        `SELECT id, recipient, description, "parent_contract"
         FROM "onchain"."Grant"
         WHERE "is_active" = true`,
        [], // No query parameters
        { highWaterMark: 10000 }
      );
      const dbStream = client.query(query);

      // Keep track of number of grants
      let grantCount = 0;

      // Pipe database rows to CSV
      const processStream = new Writable({
        objectMode: true,
        write(row: Grant, encoding, callback) {
          csvStream.write({
            id: row.id,
            recipient: row.recipient || '',
            description: row.description || '',
            parent_contract: row.parent_contract || '',
          });
          grantCount++;
          callback();
        },
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

      // Rename the temporary file to the final output file
      fs.renameSync(tmpFile, outputFile);

      console.log(
        `Grants download complete. ${grantCount} grants streamed. File saved to: ${outputFile}`
      );
    } catch (err) {
      console.error('Error downloading grants:', err);
      // Clean up tmp file if it exists
      if (fs.existsSync(`${outputFile}.tmp`)) {
        fs.unlinkSync(`${outputFile}.tmp`);
      }
    } finally {
      await client.end();
      isDownloadingGrants = false;
      downloadPromise = null;
      console.log('Database connection closed.');
    }
  })();

  // Return the promise so callers can await it
  return downloadPromise;
};

export { downloadGrants };
