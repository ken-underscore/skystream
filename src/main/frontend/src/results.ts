export interface SearchResult { text: string; seenAt: string; url: string }

export function parseResults(value: unknown): SearchResult[] {
  if (!Array.isArray(value)) throw new Error('Expected a JSON array of posts.');
  return value.map((post: unknown) => {
    const { text, seenAt, url } = (post ?? {}) as Record<string, unknown>;
    // Only bsky.app links are rendered, so a bad URL can't become a script or another site's link.
    if (typeof text !== 'string' || typeof seenAt !== 'string' || Number.isNaN(Date.parse(seenAt))
      || typeof url !== 'string' || !url.startsWith('https://bsky.app/')) {
      throw new Error('Each post must have text, a time, and a bsky.app link.');
    }
    return { text, seenAt, url };
  });
}

// English, as the rest of the page is.
const relative = new Intl.RelativeTimeFormat('en');

/** How long ago a time was, in minutes for the first hour and in hours after that. */
export function ago(time: string, now = Date.now()): string {
  const minutes = Math.round((now - Date.parse(time)) / 60_000);
  if (minutes < 1) return 'just now';
  return minutes < 60 ? relative.format(-minutes, 'minute') : relative.format(-Math.round(minutes / 60), 'hour');
}
