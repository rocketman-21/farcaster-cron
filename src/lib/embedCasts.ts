import { JobBody } from './job';
import { postBulkToEmbeddingsQueueRequest } from './queue';
import { cleanTextForEmbedding } from './embed';
import { FarcasterCast } from '../types/types';
import { getFidToVerifiedAddresses } from './download-csvs';
import { getCastEmbedUrls } from './getCastEmbedUrls';

function pushToUsers(users: string[], value: string) {
  // Only validate length for ETH addresses (starting with 0x)
  if (value.startsWith('0x') && value.length !== 42) {
    return;
  }

  users.push(value);
}

export async function embedCasts(
  casts: (FarcasterCast & { author_fname?: string })[]
) {
  // Load profiles once for the batch
  const profiles = getFidToVerifiedAddresses();

  const payloads: JobBody[] = [];

  for (const cast of casts) {
    const content = cleanTextForEmbedding(cast.text);
    if (!content || content == '') continue;

    const payload: JobBody = {
      type: 'cast',
      content,
      externalId: `0x${cast.hash.toString('hex')}`, // Convert buffer to hex string
      users: [],
      groups: [],
      tags: [],
      externalUrl: cast.author_fname
        ? `https://warpcast.com/${cast.author_fname}/0x${cast.hash.toString(
            'hex'
          )}`
        : undefined,
      hashSuffix: cast.fid.toString(),
      urls: [],
    };

    // Include the fid (user ID) and verified addresses in the users array
    if (cast.fid) {
      const fid = cast.fid.toString();
      payload.users.push(fid);

      // Get verified addresses from profiles map
      const verifiedAddresses = profiles.get(fid) || [];
      for (const address of verifiedAddresses) {
        pushToUsers(payload.users, address);
      }
    }

    payload.urls = getCastEmbedUrls(cast.embeds);

    // Parse mentions and add to tags array along with their verified addresses
    if (cast.mentions) {
      try {
        const mentionsArray = JSON.parse(cast.mentions);
        if (Array.isArray(mentionsArray)) {
          for (const mention of mentionsArray) {
            const mentionStr = mention.toString();
            // push mention to tags
            payload.tags.push(mentionStr);

            // Get verified addresses for mentioned users from profiles map
            const verifiedAddresses = profiles.get(mentionStr) || [];
            payload.tags.push(...verifiedAddresses);
          }
        }
      } catch (error) {
        console.error(
          `Error parsing mentions for cast 0x${cast.hash.toString('hex')}:`,
          error
        );
      }
    }

    // Add groups based on root_parent_url and parent_url
    if (cast.root_parent_url) {
      payload.groups.push(cast.root_parent_url);
    }
    if (cast.parent_url) {
      payload.groups.push(cast.parent_url);
    }
    // Deduplicate arrays before pushing payload
    if (payload.tags) payload.tags = getUniqueValues(payload.tags);
    if (payload.users) payload.users = getUniqueValues(payload.users);
    if (payload.urls)
      payload.urls = Array.from(
        new Set(payload.urls.map((item) => item.toString()))
      );
    if (payload.groups) payload.groups = getUniqueValues(payload.groups);

    payloads.push(payload);
  }

  const BATCH_SIZE = 50;
  // Send payloads in batches
  for (let i = 0; i < payloads.length; i += BATCH_SIZE) {
    const batch = payloads.slice(i, i + BATCH_SIZE);
    await postBulkToEmbeddingsQueueRequest(batch);
    console.log(
      `Successfully called embeddings queue for batch of ${batch.length} casts (offset: ${i})`
    );
  }
}

const getUniqueValues = (arr: string[]) => {
  if (!arr || arr.length === 0) return [];
  return Array.from(new Set(arr.map((item) => item.toString().toLowerCase())));
};
