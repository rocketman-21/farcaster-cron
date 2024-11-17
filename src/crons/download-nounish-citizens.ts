import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { ChannelMember, FarcasterProfile } from '../types/types';
import { Writable } from 'stream';
import CSV from 'fast-csv';
import QueryStream from 'pg-query-stream';
import { finished } from 'stream/promises';
import { nounishChannels } from '../lib/channels';

let isDownloadingNounishCitizens = false;
let downloadPromise: Promise<void> | null = null;

const downloadNounishCitizens = async (): Promise<void> => {
  if (isDownloadingNounishCitizens) {
    console.log(
      'Nounish citizens download is already in progress. Awaiting current download.'
    );
    return downloadPromise!;
  }

  isDownloadingNounishCitizens = true;

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
    const outputFile = path.resolve(outputDir, 'nounish-citizens.csv');

    try {
      await client.connect();

      const tmpFile = `${outputFile}.tmp`;

      const writeStream = fs.createWriteStream(tmpFile);

      // Create a CSV formatter
      const csvStream = CSV.format({
        headers: ['fid', 'fname', 'channel_id'],
      });
      csvStream.pipe(writeStream);

      console.log('Starting nounish citizens download...');
      // Create a query stream
      const query = new QueryStream(
        `SELECT DISTINCT ON (cm.fid) cm.fid, p.fname, cm.channel_id
         FROM production.farcaster_channel_members cm
         LEFT JOIN production.farcaster_profile p ON cm.fid = p.fid
         WHERE cm.channel_id = ANY($1)
         AND cm.deleted_at IS NULL`,
        [nounishChannels],
        { highWaterMark: 10000 }
      );
      const dbStream = client.query(query);

      // Keep track of number of citizens
      let citizenCount = 0;

      // Pipe database rows to CSV
      const processStream = new Writable({
        objectMode: true,
        write(
          row: ChannelMember & Pick<FarcasterProfile, 'fname'>,
          encoding,
          callback
        ) {
          csvStream.write({
            fid: row.fid,
            fname: row.fname || '',
            channel_id: row.channel_id,
          });
          citizenCount++;
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
        `Nounish citizens download complete. ${citizenCount} citizens streamed. File saved to: ${outputFile}`
      );
    } catch (err) {
      console.error('Error downloading nounish citizens:', err);
      // Clean up tmp file if it exists
      if (fs.existsSync(`${outputFile}.tmp`)) {
        fs.unlinkSync(`${outputFile}.tmp`);
      }
    } finally {
      await client.end();
      isDownloadingNounishCitizens = false;
      downloadPromise = null;
      console.log('Database connection closed.');
    }
  })();

  // Return the promise so callers can await it
  return downloadPromise;
};

export { downloadNounishCitizens };
