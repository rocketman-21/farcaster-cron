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

export async function processCasts(casts: Cast[], client: Client) {
  for (const cast of casts) {
    const payload: JobBody = {
      type: 'cast',
      content: cast.text,
      externalId: cast.id.toString(),
      users: [],
      groups: [],
      tags: [],
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
      payload.users.push(...verifiedAddresses.flat());
    }

    // Parse mentions and add to users array along with their verified addresses
    if (cast.mentions) {
      try {
        const mentionsArray = JSON.parse(cast.mentions);
        if (Array.isArray(mentionsArray)) {
          for (const mention of mentionsArray) {
            payload.users.push(mention.toString());

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
        console.error(`Error parsing mentions for cast ${cast.id}:`, error);
      }
    }

    // Add groups based on root_parent_url and parent_url
    if (cast.root_parent_url) {
      payload.groups.push(cast.root_parent_url);
    }
    if (cast.parent_url) {
      payload.groups.push(cast.parent_url);
    }

    // Map root_parent_url to tags
    const tagMappings: Record<string, string[]> = {
      'https://warpcast.com/~/channel/flows': ['flows'],
      'chain://eip155:1/erc721:0x9c8ff314c9bc7f6e59a9d9225fb22946427edc03': [
        'grants',
      ],
      'https://warpcast.com/~/channel/yellow': ['drafts'],
    };

    if (cast.root_parent_url && tagMappings[cast.root_parent_url]) {
      payload.tags.push(...tagMappings[cast.root_parent_url]);
    }

    console.log(payload);

    try {
      //   await postToEmbeddingsQueueRequest(payload);
      console.log(`Successfully posted cast ${cast.id} to embeddings queue`);
    } catch (err) {
      console.error(`Failed to post cast ${cast.id} to embeddings queue:`, err);
    }
  }
}
