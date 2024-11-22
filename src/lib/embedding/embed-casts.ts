import { JobBody } from '../job';
import { cleanTextForEmbedding } from '../embed';
import { StagingFarcasterCast, FarcasterCast } from '../../types/types';
import { getFidToFname, getFidToVerifiedAddresses } from '../download-csvs';
import { getCastEmbedUrls } from '../getCastEmbedUrls';
import { insertMentionsIntoText } from '../mentions/add-mentions';
import { createBasePayload, sendPayloadsInBatches } from './payloads';
import { finalizePayload } from './payloads';

const fidToFname = getFidToFname();
const profiles = getFidToVerifiedAddresses();

// Helper function to handle mentions
function handleMentions(
  mentions: number[],
  payload: JobBody,
  profiles: Map<string, string[]>,
  castHash: Buffer
) {
  try {
    if (Array.isArray(mentions)) {
      for (const mention of mentions) {
        const mentionStr = mention.toString();
        payload.tags.push(mentionStr);

        const verifiedAddresses = profiles.get(mentionStr) || [];
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
export async function embedStagingCasts(casts: StagingFarcasterCast[]) {
  if (casts.length === 0) {
    // Handle empty array case, possibly return early or log a warning
    console.warn('No casts to embed');
    return;
  }

  const payloads: JobBody[] = [];

  for (const cast of casts) {
    let content = cast.text;
    if (!content || content === '') continue;

    // Insert mentions into content for staging format
    if (cast.mentions_positions && cast.mentions) {
      // log the type
      console.warn(cast.mentions_positions);
      console.warn(cast.mentions);
      console.warn(
        `staging cast.mentions_positions: ${typeof cast.mentions_positions}, cast.mentions: ${typeof cast.mentions}`
      );
      content = insertMentionsIntoText(
        content,
        JSON.parse(cast.mentions_positions || '[]'),
        cast.mentions || [],
        fidToFname
      );
    }

    content = cleanTextForEmbedding(content);

    const payload = createBasePayload(cast, content, profiles, fidToFname);
    payload.urls = getCastEmbedUrls(cast.embeds);

    // Handle mentions for staging format
    if (cast.mentions) {
      handleMentions(cast.mentions, payload, profiles, cast.hash);
    }

    finalizePayload(payload);
    payloads.push(payload);
  }

  await sendPayloadsInBatches(payloads);
}

// Handler for production casts
export async function embedProductionCasts(casts: FarcasterCast[]) {
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

    const payload = createBasePayload(cast, content, profiles, fidToFname);
    payload.urls = getCastEmbedUrls(cast.embeds);

    // Handle mentions for production format
    if (cast.mentioned_fids) {
      handleMentions(cast.mentioned_fids, payload, profiles, cast.hash);
    }

    finalizePayload(payload);
    payloads.push(payload);
  }

  await sendPayloadsInBatches(payloads);
}
