const blueskySearch = 'https://bsky.app/search';

export function searchUrl(word: string): string {
  return `${blueskySearch}?${new URLSearchParams({ q: word })}`;
}
