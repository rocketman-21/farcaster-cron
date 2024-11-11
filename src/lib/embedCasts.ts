import fs from 'fs';
import path from 'path';
import { JobBody } from './job';
import { postBulkToEmbeddingsQueueRequest } from './queue';
import { cleanTextForEmbedding } from './embed';

export interface Cast {
  id: bigint;
  created_at: Date;
  updated_at: Date;
  deleted_at: Date | null;
  timestamp: Date;
  fid: bigint;
  hash: Buffer;
  parent_hash: Buffer | null;
  parent_fid: bigint | null;
  parent_url: string | null;
  text: string;
  embeds: string | null;
  mentions: string | null;
  mentions_positions: string | null;
  root_parent_hash: Buffer | null;
  root_parent_url: string | null;
}

function pushToUsers(users: string[], value: string, cast: Cast) {
  // Only validate length for ETH addresses (starting with 0x)
  if (value.startsWith('0x') && value.length !== 42) {
    return;
  }

  users.push(value);
}

// Load profiles from CSV
const loadProfiles = () => {
  const profilesPath = path.resolve(__dirname, '../data/profiles.csv');
  const profiles = new Map<string, string[]>();

  const lines = fs.readFileSync(profilesPath, 'utf-8').split('\n');
  // Skip header
  for (let i = 1; i < lines.length; i++) {
    const line = lines[i].trim();
    if (!line) continue;

    const [fid, _, addresses] = line.split(',');
    profiles.set(fid, addresses ? addresses.split('|') : []);
  }

  return profiles;
};

export async function embedCasts(casts: Cast[]) {
  // Load profiles once for the batch
  const profiles = loadProfiles();

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
        pushToUsers(payload.users, address, cast);
      }
    }

    if (cast.embeds?.length) {
      const urls: { url: string }[] = JSON.parse(cast.embeds);
      payload.urls = urls.map((url) => url.url).filter((url) => url);
    }

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
    if (payload.urls) payload.urls = getUniqueValues(payload.urls);
    if (payload.groups) payload.groups = getUniqueValues(payload.groups);

    payloads.push(payload);
  }

  const BATCH_SIZE = 100;
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
