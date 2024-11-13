import { Client } from 'pg';
import fs from 'fs';
import path from 'path';
import { Grant } from '../types/types';

const downloadGrants = async () => {
  // Create a new PostgreSQL client
  const client = new Client({
    connectionString: process.env.FLOWS_DB_URL,
  });

  try {
    await client.connect();

    // Create output directory if it doesn't exist
    const outputDir = path.resolve(__dirname, '../data');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }

    const outputFile = path.resolve(outputDir, 'grants.csv');
    const writeStream = fs.createWriteStream(outputFile);

    // Write CSV header with only needed fields
    writeStream.write('id,recipient,description,parentContract\n');

    const batchSize = 1000;
    let offset = 0;
    let hasMore = true;

    console.log('Starting grants download...');

    while (hasMore) {
      const result = await client.query<Grant>(
        `SELECT id, recipient, description, "parentContract"
         FROM "public"."Grant"
         ORDER BY id 
         LIMIT $1 OFFSET $2`,
        [batchSize, offset]
      );

      if (result.rows.length === 0) {
        hasMore = false;
        continue;
      }

      // Process each row and write to CSV
      result.rows.forEach((row) => {
        // Escape any commas in the description field
        const escapedDescription = row.description
          ? `"${row.description.replace(/"/g, '""')}"`
          : '';

        writeStream.write(
          `${row.id},${row.recipient},${escapedDescription},${row.parentContract}\n`
        );
      });

      console.log(`Processed ${offset + result.rows.length} grants`);
      offset += batchSize;
    }

    writeStream.end();
    console.log(`Grants download complete. File saved to: ${outputFile}`);
  } catch (err) {
    console.error('Error downloading grants:', err);
  } finally {
    await client.end();
  }
};

export { downloadGrants };
