import { IsGrantUpdateJobBody } from './job';
import { postBulkIsGrantsUpdateRequest } from './queue';
import { StagingFarcasterCast } from '../types/types';
import { cleanTextForEmbedding } from './embed';
import { getCastEmbedUrls } from './getCastEmbedUrls';
import { insertMentionsIntoText } from './mentions/add-mentions';

export async function checkGrantUpdates(
  casts: StagingFarcasterCast[],
  fidToFname: Map<string, string>
) {
  const payloads: IsGrantUpdateJobBody[] = [];

  for (const cast of casts) {
    // Use the ordered arrays of grantIds and parentContracts
    const textWithMentions = insertMentionsIntoText(
      cast.text,
      JSON.parse(cast.mentions_positions || '[]'),
      cast.mentions || [],
      fidToFname
    );

    const payload: IsGrantUpdateJobBody = {
      castContent: cleanTextForEmbedding(textWithMentions),
      castHash: `0x${cast.hash.toString('hex')}`,
      urls: getCastEmbedUrls(cast.embeds, fidToFname),
      builderFid: cast.fid.toString(),
    };

    if (payload.castContent || payload.urls.length > 0) {
      payloads.push(payload);
    } else {
      console.log(
        `Skipping cast ${cast.id} because it has no content or urls`,
        cast
      );
    }
  }

  const BATCH_SIZE = 10;
  // Send payloads in batches
  for (let i = 0; i < payloads.length; i += BATCH_SIZE) {
    const batch = payloads.slice(i, i + BATCH_SIZE);
    await postBulkIsGrantsUpdateRequest(batch);
    console.log(
      `Successfully checked grant updates for batch of ${batch.length} casts (offset: ${i})`
    );
  }
}
