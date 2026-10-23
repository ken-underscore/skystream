export interface WordCount { text: string; count: number }
export interface Snapshot { words: WordCount[]; collectedSeconds: number }

export function parseSnapshot(json: string): Snapshot {
  const value: unknown = JSON.parse(json);
  const { words, collectedSeconds } = (value ?? {}) as Record<string, unknown>;
  if (words === null || typeof words !== 'object' || Array.isArray(words)) {
    throw new Error('Expected a JSON object mapping words to counts.');
  }
  if (typeof collectedSeconds !== 'number' || !Number.isSafeInteger(collectedSeconds) || collectedSeconds < 0) {
    throw new Error('Expected how many seconds of the window were collected.');
  }
  return {
    words: Object.entries(words).map(([text, count]) => {
      if (!text.trim() || typeof count !== 'number' || !Number.isSafeInteger(count) || count <= 0) {
        throw new Error('Each word must have a positive integer count.');
      }
      return { text, count };
    }).sort((a, b) => b.count - a.count || a.text.localeCompare(b.text)),
    collectedSeconds,
  };
}
