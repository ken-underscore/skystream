export interface WordCount { text: string; count: number }

export function parseBucket(json: string): WordCount[] {
  const value: unknown = JSON.parse(json);
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new Error('Expected a JSON object mapping words to counts.');
  }
  return Object.entries(value).map(([text, count]) => {
    if (!text.trim() || typeof count !== 'number' || !Number.isSafeInteger(count) || count <= 0) {
      throw new Error('Each word must have a positive integer count.');
    }
    return { text, count };
  }).sort((a, b) => b.count - a.count || a.text.localeCompare(b.text));
}
