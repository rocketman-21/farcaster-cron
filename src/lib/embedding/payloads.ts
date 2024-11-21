import { FarcasterCast } from '../../types/types';
import { StagingFarcasterCast } from '../../types/types';
import { JobBody } from '../job';
import { postBulkToEmbeddingsQueueRequest } from '../queue';

// Helper function to create base payload
export function createBasePayload(
  cast: StagingFarcasterCast | FarcasterCast,
  content: string,
  profiles: Map<string, string[]>,
  fidToFname: Map<string, string>
): JobBody {
  const fname = fidToFname.get(cast.fid.toString());
  const payload: JobBody = {
    type: 'cast',
    content,
    externalId: `0x${cast.hash.toString('hex')}`,
    users: [],
    groups: [],
    tags: [],
    externalUrl: fname
      ? `https://warpcast.com/${fname}/0x${cast.hash.toString('hex')}`
      : undefined,
    hashSuffix: cast.fid.toString(),
    urls: [],
  };

  if (cast.fid) {
    const fid = cast.fid.toString();
    payload.users.push(fid);
    const verifiedAddresses = profiles.get(fid) || [];
    for (const address of verifiedAddresses) {
      pushToUsers(payload.users, address);
    }
  }

  if (cast.root_parent_url) {
    payload.groups.push(cast.root_parent_url);
  }
  if (cast.parent_url) {
    payload.groups.push(cast.parent_url);
  }

  return payload;
}

// Helper function to finalize payload by deduplicating arrays
export function finalizePayload(payload: JobBody) {
  if (payload.tags) payload.tags = getUniqueValues(payload.tags);
  if (payload.users) payload.users = getUniqueValues(payload.users);
  if (payload.urls)
    payload.urls = Array.from(
      new Set(payload.urls.map((item) => item.toString()))
    );
  if (payload.groups) payload.groups = getUniqueValues(payload.groups);
}

// Helper function to send payloads in batches
export async function sendPayloadsInBatches(payloads: JobBody[]) {
  const BATCH_SIZE = 50;
  for (let i = 0; i < payloads.length; i += BATCH_SIZE) {
    const batch = payloads.slice(i, i + BATCH_SIZE);
    await postBulkToEmbeddingsQueueRequest(batch);
    console.log(
      `Successfully called embeddings queue for batch of ${batch.length} casts (offset: ${i})`
    );
  }
}

function pushToUsers(users: string[], value: string) {
  // Only validate length for ETH addresses (starting with 0x)
  if (value.startsWith('0x') && value.length !== 42) {
    return;
  }

  users.push(value);
}

const getUniqueValues = (arr: string[]) => {
  if (!arr || arr.length === 0) return [];
  return Array.from(new Set(arr.map((item) => item.toString().toLowerCase())));
};
