/** What Bluesky's public API adds to a search result: who posted it, their avatar, its likes and its images. */
export interface Post { name: string; handle: string; avatar: string | undefined; likes: number; images: Image[] }
export interface Image { src: string; alt: string }

const api = 'https://public.api.bsky.app/xrpc/app.bsky.feed.getPosts';
// Only Bluesky's own image host is used, so the API can't point the page at another site.
const cdn = 'https://cdn.bsky.app/';
// Images on posts or accounts labelled with these stay hidden, as Bluesky hides them by default.
const adult = new Set(['porn', 'sexual', 'nudity', 'graphic-media']);

const fields = (value: unknown) => (value ?? {}) as Record<string, unknown>;
const fromCdn = (value: unknown) => typeof value === 'string' && value.startsWith(cdn) ? value : undefined;
const labelled = (labels: unknown) => Array.isArray(labels) && labels.some(label => adult.has(String(fields(label).val)));

/** The AT URI that Bluesky's API names a post by, from its bsky.app link. */
export function atUri(url: string): string | undefined {
  const match = /^https:\/\/bsky\.app\/profile\/([^/]+)\/post\/([^/?#]+)$/.exec(url);
  return match ? `at://${match[1]}/app.bsky.feed.post/${match[2]}` : undefined;
}

/** Fetches up to 25 posts by AT URI. Posts that were deleted or aren't public are missing from the map. */
export async function fetchPosts(uris: string[]): Promise<Map<string, Post>> {
  const response = await fetch(`${api}?${new URLSearchParams(uris.map(uri => ['uris', uri]))}`);
  if (!response.ok) throw new Error(`Bluesky answered ${response.status}.`);
  const { posts } = fields(await response.json());
  if (!Array.isArray(posts)) throw new Error('Expected posts from Bluesky.');
  return new Map(posts.flatMap(parsePost));
}

function parsePost(value: unknown): [string, Post][] {
  const { uri, author, likeCount, labels, embed } = fields(value);
  const { handle, displayName, avatar, labels: authorLabels } = fields(author);
  if (typeof uri !== 'string' || typeof handle !== 'string') return [];
  return [[uri, {
    name: typeof displayName === 'string' && displayName.trim() ? displayName : handle,
    handle,
    avatar: fromCdn(avatar),
    likes: typeof likeCount === 'number' && Number.isSafeInteger(likeCount) ? likeCount : 0,
    images: labelled(labels) || labelled(authorLabels) ? [] : images(embed),
  }]];
}

/** A post's images, from an image or gallery embed, alone or beside a quoted post. */
function images(embed: unknown): Image[] {
  const view = fields(embed);
  const media = view.$type === 'app.bsky.embed.recordWithMedia#view' ? fields(view.media) : view;
  const list = media.$type === 'app.bsky.embed.images#view' ? media.images
    : media.$type === 'app.bsky.embed.gallery#view' ? media.items : undefined;
  if (!Array.isArray(list)) return [];
  return list.flatMap((image: unknown) => {
    const { thumb, thumbnail, alt } = fields(image);
    const src = fromCdn(thumb ?? thumbnail);
    return src ? [{ src, alt: typeof alt === 'string' ? alt : '' }] : [];
  });
}
