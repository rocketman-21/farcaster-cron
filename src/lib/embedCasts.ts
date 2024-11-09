import { Client } from 'pg';
import { JobBody } from './job';
import { postToEmbeddingsQueueRequest } from './queue';

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

export async function processCasts(casts: Cast[], client: Client) {
  const promises = casts.map(async (cast) => {
    const content = cleanTextForEmbedding(cast.text);
    if (!content || content == '') return;

    const payload: JobBody = {
      type: 'cast',
      content,
      externalId: `0x${cast.hash.toString('hex')}`, // Convert buffer to hex string
      users: [],
      groups: [],
      tags: [],
      hashSuffix: cast.fid.toString(),
    };

    // Include the fid (user ID) and verified addresses in the users array
    if (cast.fid) {
      const fid = cast.fid.toString();
      payload.users.push(fid);

      // Get verified addresses for the cast author
      const profileRes = await client.query(
        'SELECT verified_addresses FROM production.farcaster_profile WHERE fid = $1',
        [cast.fid]
      );
      const verifiedAddresses = profileRes.rows[0]?.verified_addresses || [];

      for (const address of verifiedAddresses.flat()) {
        pushToUsers(payload.users, address, cast);
      }
    }

    // Parse mentions and add to tags array along with their verified addresses
    if (cast.mentions) {
      try {
        const mentionsArray = JSON.parse(cast.mentions);
        if (Array.isArray(mentionsArray)) {
          for (const mention of mentionsArray) {
            // push mention to tags
            payload.tags.push(mention.toString());

            // Get verified addresses for mentioned users
            const mentionProfileRes = await client.query(
              'SELECT verified_addresses FROM production.farcaster_profile WHERE fid = $1',
              [mention]
            );
            if (mentionProfileRes.rows[0]?.verified_addresses) {
              const verifiedAddresses =
                mentionProfileRes.rows[0].verified_addresses.flat();
              payload.tags.push(...verifiedAddresses);
            }
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

    return postToEmbeddingsQueueRequest(payload);
  });

  const filteredPromises = promises.filter(Boolean);
  for (let i = 0; i < filteredPromises.length; i += 100) {
    const batch = filteredPromises.slice(i, i + 100);
    await Promise.all(batch);
  }
}

export const cleanTextForEmbedding = (text: string) => {
  return (
    text
      // Remove actual newline and carriage return characters
      .replace(/(\r\n|\n|\r)/g, ' ')
      // Replace escaped newline and carriage return sequences with a space
      .replace(/\\n|\\r/g, ' ')
      // Remove markdown images
      .replace(/!\[[^\]]*\]\([^\)]*\)/g, '')
      // Remove markdown headings
      .replace(/^#+\s/gm, '')
      // Remove markdown list markers (- or *)
      .replace(/^[-*]\s/gm, '')
      // Remove HTML tags if any
      .replace(/<[^>]+>/g, ' ')
      // Normalize multiple spaces to a single space
      .replace(/\s+/g, ' ')
      // Remove unnecessary characters like # and *
      .replace(/[#*]/g, ' ')
      // Trim leading and trailing whitespace
      .trim()
      // Convert to lowercase
      .toLowerCase()
  );
};
