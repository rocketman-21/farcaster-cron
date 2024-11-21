import { IsGrantUpdateJobBody } from './job';
import { postBulkIsGrantsUpdateRequest } from './queue';
import { Grant, StagingFarcasterCast } from '../types/types';
import { cleanTextForEmbedding } from './embed';
import { getCastEmbedUrls } from './getCastEmbedUrls';

export async function checkGrantUpdates(
  casts: (StagingFarcasterCast & { grantIds: string[] })[],
  grants: Grant[]
) {
  const payloads: IsGrantUpdateJobBody[] = [];

  for (const cast of casts) {
    // Use the ordered arrays of grantIds and parentContracts
    for (let i = 0; i < cast.grantIds.length; i++) {
      const grantId = cast.grantIds[i];

      // Find matching grant
      const grant = grants.find((g) => g.id === grantId);
      if (!grant) {
        console.error(`No grant found for id ${grantId}`);
        continue;
      }

      // Find parent grant where contract matches recipient
      const parentGrant = grants.find(
        (g) => g.recipient.toLowerCase() === grant.parentContract.toLowerCase()
      );

      const payload: IsGrantUpdateJobBody = {
        castContent: cleanTextForEmbedding(cast.text),
        grantDescription: cleanTextForEmbedding(grant.description || ''),
        parentFlowDescription: cleanTextForEmbedding(
          parentGrant?.description || ''
        ),
        castHash: `0x${cast.hash.toString('hex')}`,
        grantId: grant.id,
        urls: getCastEmbedUrls(cast.embeds),
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
  }

  const BATCH_SIZE = 10;
  // Send payloads in batches
  for (let i = 0; i < payloads.length; i += BATCH_SIZE) {
    const batch = payloads.slice(i, i + BATCH_SIZE);
    await postBulkIsGrantsUpdateRequest(batch);
    console.log(
      `Successfully called embeddings queue for batch of ${batch.length} grant update checks (offset: ${i})`
    );
  }
}
