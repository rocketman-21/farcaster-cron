import { Client } from 'pg';
import fs from 'fs';
import path from 'path';

const downloadNounishCitizens = async () => {
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

    const outputFile = path.resolve(outputDir, 'nounish-citizens.csv');
    const writeStream = fs.createWriteStream(outputFile);

    // Write CSV header
    writeStream.write('fid,fname,channel_id\n');

    const batchSize = 10000;
    let offset = 0;
    let hasMore = true;

    const channels = [
      'vrbs',
      'nouns',
      'gnars',
      'flows',
      'nouns-animators',
      'nouns-draws',
      'nouns-impact',
      'nouns-retro',
    ];

    console.log('Starting nounish citizens download...');
    while (hasMore) {
      const result = await client.query(
        `SELECT DISTINCT cm.fid, p.fname, cm.channel_id
         FROM production.farcaster_channel_members cm
         LEFT JOIN production.farcaster_profile p ON cm.fid = p.fid
         WHERE cm.channel_id = ANY($1)
         AND cm.deleted_at IS NULL
         ORDER BY cm.fid 
         LIMIT $2 OFFSET $3`,
        [channels, batchSize, offset]
      );

      if (result.rows.length === 0) {
        hasMore = false;
        console.log('No more nounish citizens to download');
        continue;
      }

      // Process each row and write to CSV
      result.rows.forEach((row) => {
        writeStream.write(`${row.fid},${row.fname || ''},${row.channel_id}\n`);
      });

      console.log(`Processed ${offset + result.rows.length} citizens`);
      offset += batchSize;
    }

    writeStream.end();
    console.log(
      `Nounish citizens download complete. File saved to: ${outputFile}`
    );
  } catch (err) {
    console.error('Error downloading nounish citizens:', err);
  } finally {
    await client.end();
  }
};

export { downloadNounishCitizens };
