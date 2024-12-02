import { Client } from 'pg';
import { IsGrantUpdateJobBody } from '../lib/job';
import { postBulkIsGrantsUpdateRequest } from '../lib/queue';
import { cleanTextForEmbedding } from '../lib/embed';
import { getCastEmbedUrls } from '../lib/getCastEmbedUrls';
import { insertMentionsIntoText } from '../lib/mentions/add-mentions';
import { getFidToFname } from '../lib/download-csvs';

export async function testGrantUpdate() {
  console.log('Testing grant update check...');

  // Create PostgreSQL client
  const client = new Client({
    connectionString: process.env.DB_URL,
  });

  try {
    await client.connect();
    console.log('Connected to database');

    const fidToFname = getFidToFname();
    // const FIDS = [
    //   '500037',
    //   '836625',
    //   '397777',
    //   '398364',
    //   '486993',
    //   '369904',
    //   '20188',
    //   '238425',
    //   '477861',
    //   '20721',
    //   '423296',
    //   '478527',
    //   '191593',
    //   '517552',
    //   '527313',
    //   '410324',
    //   '318675',
    //   '482556',
    //   '861605',
    //   '15441',
    //   '374641',
    //   '7759',
    //   '477919',
    //   '263648',
    //   '852627',
    //   '191770',
    //   '546204',
    //   '468723',
    //   '848983',
    //   '843413',
    //   '870004',
    //   '2112',
    //   '862501',
    //   '13894',
    //   '374408',
    //   '388453',
    //   '847988',
    //   '15069',
    //   '399306',
    //   '874748',
    //   '848985',
    //   '272919',
    // ];

    const FIDS = ['277501'];

    // Process each FID sequentially
    for (const fid of FIDS) {
      try {
        await processGrantsForFid(fid, client, fidToFname);
      } catch (error) {
        console.error(`Error processing FID ${fid}:`, error);
        // Continue with next FID even if one fails
      }
    }
  } catch (error) {
    console.error('Failed to process grant updates:', error);
    throw error;
  } finally {
    await client.end();
  }
}

testGrantUpdate().catch(console.error);

async function processGrantsForFid(
  fid: string,
  client: Client,
  fidToFname: Map<string, string>
) {
  console.log(`\nProcessing FID: ${fid}`);

  const res = await client.query(
    `SELECT * FROM production.farcaster_casts 
       WHERE fid = $1
       AND timestamp > NOW() - INTERVAL '2 months'
       AND parent_hash IS NULL
       ORDER BY timestamp DESC`,
    [fid]
  );

  console.log(`Found ${res.rows.length} casts for FID ${fid}`);

  const payloads: IsGrantUpdateJobBody[] = [];

  // Process each cast into a payload
  for (const cast of res.rows) {
    const textWithMentions = insertMentionsIntoText(
      cast.text,
      cast.mentions_positions_array || [],
      cast.mentioned_fids || [],
      fidToFname
    );

    const payload: IsGrantUpdateJobBody = {
      castContent: cleanTextForEmbedding(textWithMentions),
      castHash: `0x${cast.hash.toString('hex')}`,
      urls: getCastEmbedUrls(cast.embeds),
      builderFid: cast.fid.toString(),
    };

    if (payload.castContent || payload.urls.length > 0) {
      payloads.push(payload);
    }
  }

  console.log(`Created ${payloads.length} payloads for FID ${fid}`);

  // Send payloads in batches of 10
  const BATCH_SIZE = 10;
  for (let i = 0; i < payloads.length; i += BATCH_SIZE) {
    const batch = payloads.slice(i, i + BATCH_SIZE);
    await postBulkIsGrantsUpdateRequest(batch);
    console.log(
      `Successfully sent batch of ${batch.length} grant update checks for FID ${fid} (offset: ${i})`
    );
  }
}
