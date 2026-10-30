export interface SearchResult { text: string; seenAt: string; url: string; similarity: number }

export function parseResults(value: unknown): SearchResult[] {
  if (!Array.isArray(value)) throw new Error('Expected a JSON array of posts.');
  return value.map((post: unknown) => {
    const { text, seenAt, url, similarity } = (post ?? {}) as Record<string, unknown>;
    // Only bsky.app links are rendered, so a bad URL can't become a script or another site's link.
    if (typeof text !== 'string' || typeof seenAt !== 'string' || Number.isNaN(Date.parse(seenAt))
      || typeof url !== 'string' || !url.startsWith('https://bsky.app/')
      || typeof similarity !== 'number' || !Number.isFinite(similarity)) {
      throw new Error('Each post must have text, a time, a bsky.app link, and a similarity score.');
    }
    return { text, seenAt, url, similarity };
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

/** When a client that searched too often may search again, as in "in 40 seconds", "in 5 minutes" or "in 3 hours". */
export function retryIn(value: unknown): string {
  const { retryAfterSeconds: seconds } = (value ?? {}) as Record<string, unknown>;
  if (typeof seconds !== 'number' || !(seconds >= 0)) throw new Error('Expected the seconds to wait.');
  if (seconds < 60) return relative.format(Math.max(1, Math.ceil(seconds)), 'second');
  return seconds < 3600 ? relative.format(Math.ceil(seconds / 60), 'minute') : relative.format(Math.ceil(seconds / 3600), 'hour');
}

export function parseAnswerText(value: unknown): string {
  const { text } = (value ?? {}) as Record<string, unknown>;
  if (typeof text !== 'string') throw new Error('Expected answer text.');
  return text;
}

/**
 * Splits an answer into text and the result numbers it cites, as in "[2]" or "[1, 3]". Numbers outside the results stay
 * text.
 */
export function citedParts(answer: string, results: number): (string | number)[] {
  const parts: (string | number)[] = [];
  let last = 0;
  for (const match of answer.matchAll(/\[(\d+(?:\s*,\s*\d+)*)\]/g)) {
    const numbers = (match[1] ?? '').split(',').map(Number);
    if (numbers.some(number => number < 1 || number > results)) continue;
    parts.push(answer.slice(last, match.index));
    numbers.forEach((number, i) => parts.push(...(i ? [' ', number] : [number])));
    last = match.index + match[0].length;
  }
  parts.push(answer.slice(last));
  return parts.filter(part => part !== '');
}
