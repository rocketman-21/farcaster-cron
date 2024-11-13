export function getCastEmbedUrls(embeds: string | null): string[] {
  if (!embeds?.length) {
    return [];
  }

  try {
    const urls: { url: string }[] = JSON.parse(embeds);
    return urls.map((url) => url.url).filter((url) => url);
  } catch (e) {
    console.error('Error parsing cast embeds:', e);
    return [];
  }
}
