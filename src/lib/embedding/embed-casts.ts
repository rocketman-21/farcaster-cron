import { JobBody } from '../job';
import { cleanTextForEmbedding } from '../embed';
import { StagingFarcasterCast, FarcasterCast } from '../../types/types';
import { getCastEmbedUrls } from '../getCastEmbedUrls';
import { insertMentionsIntoText } from '../mentions/add-mentions';
import { createBasePayload, sendPayloadsInBatches } from './payloads';
import { finalizePayload } from './payloads';

// Helper function to handle mentions
function handleMentions(
  mentions: number[],
  payload: JobBody,
  fidToVerifiedAddresses: Map<string, string[]>,
  castHash: Buffer
) {
  try {
    if (Array.isArray(mentions)) {
      for (const mention of mentions) {
        const mentionStr = mention.toString();
        payload.tags.push(mentionStr);

        const verifiedAddresses = fidToVerifiedAddresses.get(mentionStr) || [];
        payload.tags.push(...verifiedAddresses);
      }
    }
  } catch (error) {
    console.error(
      `Error parsing mentions for cast 0x${castHash.toString('hex')}:`,
      error
    );
  }
}

// Handler for staging casts
export async function embedStagingCasts(
  casts: StagingFarcasterCast[],
  fidToFname: Map<string, string>,
  fidToVerifiedAddresses: Map<string, string[]>
) {
  if (casts.length === 0) {
    // Handle empty array case, possibly return early or log a warning
    console.warn('No casts to embed');
    return;
  }

  const payloads: JobBody[] = [];

  for (let i = 0; i < casts.length; i++) {
    const cast = casts[i];
    let content = cast.text;
    if (!content || content === '') continue;

    // Insert mentions into content for staging format
    if (cast.mentions_positions && cast.mentions) {
      content = insertMentionsIntoText(
        content,
        JSON.parse(cast.mentions_positions || '[]'),
        cast.mentions || [],
        fidToFname
      );
    }

    content = cleanTextForEmbedding(content);

    const payload = createBasePayload(
      cast,
      content,
      fidToVerifiedAddresses,
      fidToFname
    );
    payload.urls = getCastEmbedUrls(cast.embeds, fidToFname);

    // Handle mentions for staging format
    if (cast.mentions) {
      handleMentions(cast.mentions, payload, fidToVerifiedAddresses, cast.hash);
    }

    finalizePayload(payload);
    payloads.push(payload);
  }

  await sendPayloadsInBatches(payloads);
}

// Handler for production casts
export async function embedProductionCasts(
  casts: FarcasterCast[],
  fidToFname: Map<string, string>,
  fidToVerifiedAddresses: Map<string, string[]>
) {
  if (casts.length === 0) {
    // Handle empty array case, possibly return early or log a warning
    console.warn('No casts to embed');
    return;
  }

  const payloads: JobBody[] = [];

  for (const cast of casts) {
    let content = cast.text;
    if (!content || content === '') continue;

    // Insert mentions into content for production format
    if (cast.mentions_positions_array && cast.mentioned_fids) {
      content = insertMentionsIntoText(
        content,
        cast.mentions_positions_array || [],
        cast.mentioned_fids || [],
        fidToFname
      );
    }

    content = cleanTextForEmbedding(content);

    const payload = createBasePayload(
      cast,
      content,
      fidToVerifiedAddresses,
      fidToFname
    );
    payload.urls = getCastEmbedUrls(cast.embeds, fidToFname);

    // Handle mentions for production format
    if (cast.mentioned_fids) {
      handleMentions(
        cast.mentioned_fids,
        payload,
        fidToVerifiedAddresses,
        cast.hash
      );
    }

    finalizePayload(payload);
    payloads.push(payload);
  }

  await sendPayloadsInBatches(payloads);
}
