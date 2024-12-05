import { getCastStringHash } from './cast-hash';

interface CastHash {
  data: number[];
  type: 'Buffer';
}

interface CastId {
  fid: number;
  hash: CastHash;
}

interface CastEmbed {
  castId: CastId;
}

interface UrlEmbed {
  url: string;
}

type Embed = CastEmbed | UrlEmbed;

export function getCastEmbedUrls(
  embeds: string | null,
  fidToFname: Map<string, string>
): string[] {
  if (!embeds?.length) {
    return [];
  }

  try {
    const embedObjects: Embed[] = JSON.parse(embeds);
    const urls: string[] = [];

    for (const embed of embedObjects) {
      if ('url' in embed) {
        if (embed.url) {
          urls.push(embed.url);
        }
      } else if ('castId' in embed) {
        const castId = embed.castId;
        if (castId.hash && 'data' in castId.hash) {
          const hash = Buffer.from(castId.hash.data);
          const castHash = getCastStringHash(hash);
          const fid = castId.fid.toString();
          const fname = fidToFname.get(fid);
          if (fname) {
            urls.push(`https://warpcast.com/${fname}/${castHash}`);
          }
        }
      }
    }

    return urls;
  } catch (e) {
    console.error('Error parsing cast embeds:', e);
    return [];
  }
}
