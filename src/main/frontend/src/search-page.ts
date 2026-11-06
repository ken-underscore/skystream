import './style.css';
import { ago, citedParts, parseAnswerText, parseResults, retryIn } from './results';
import type { SearchResult } from './results';
import { atUri, fetchPosts } from './bluesky';
import type { Image, Post } from './bluesky';

function byId(id: string): HTMLElement {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing page element: ${id}`);
  return found;
}
const status = byId('result-status');
const error = byId('error');
const results = byId('results');
const answer = byId('answer');
const answerText = byId('answer-text');
const similarityMeaning = byId('similarity-meaning').textContent;
// The form submits as a plain GET, so each search is its own page in history and can be shared as a link.
const input = byId('q') as HTMLInputElement;
const query = new URLSearchParams(location.search).get('q')?.trim() ?? '';
input.value = query;
if (query) search(query);

function search(query: string) {
  // A shared or edited link can carry a longer query than the input allows, and the server would refuse it.
  if (query.length > input.maxLength) {
    error.textContent = `Could not search: enter a search of at most ${input.maxLength} characters.`;
    error.hidden = false;
    return;
  }
  status.textContent = 'Searching…';
  const source = new EventSource(`/api/search/stream?${new URLSearchParams({ q: query })}`);
  let found: SearchResult[] | undefined;
  let written = '';
  // Each handler closes the stream when it ends, since EventSource would otherwise reconnect and search again.
  const end = (message?: string) => {
    source.close();
    answerText.removeAttribute('aria-busy');
    if (message !== undefined) answerText.textContent = message;
  };
  const fail = (message: string) => {
    end();
    status.textContent = '';
    error.textContent = `Could not search: ${message}`;
    error.hidden = false;
  };
  source.addEventListener('results', event => {
    try {
      found = parseResults(JSON.parse(event.data));
    } catch (cause) {
      fail(cause instanceof Error ? cause.message : 'the results were invalid.');
      return;
    }
    const cards = found.map(card);
    results.replaceChildren(...cards.map(({ item }) => item));
    fill(cards);
    status.textContent = found.length === 1 ? `The post closest in meaning to “${query}”.`
      : found.length ? `The ${found.length} posts closest in meaning to “${query}”, best match first.`
      : 'No posts are searchable yet. Captured posts become searchable once they’re embedded, usually within seconds.';
    answer.hidden = found.length === 0;
    answerText.setAttribute('aria-busy', 'true');
    answerText.textContent = 'Writing a summary…';
  });
  source.addEventListener('answer', event => {
    try {
      written += parseAnswerText(JSON.parse(event.data));
    } catch {
      end('Could not read the summary.');
      return;
    }
    answerText.replaceChildren(...citedParts(written, found?.length ?? 0).map(part => typeof part === 'string'
      ? part : citation(part)));
  });
  source.addEventListener('done', () => end());
  source.addEventListener('failed', () => end('Could not write a summary this time. The results below are unaffected.'));
  source.addEventListener('paused', () => end('Summaries are paused until midnight UTC: today’s limit is used up. '
    + 'Until then, results may be less relevant.'));
  source.addEventListener('limited', event => {
    let wait = 'later';
    try {
      wait = retryIn(JSON.parse(event.data));
    } catch {
      // The message still says why, without the wait.
    }
    fail(`you’ve searched a lot in a short time. Try again ${wait}.`);
  });
  // The browser hides why a stream failed, so the likeliest causes are named instead.
  source.addEventListener('error', () => found
    ? end('The summary stopped early.')
    : fail('the server didn’t answer. It may be down, or not have search enabled.'));
}

/** Fills the cards from Bluesky. If Bluesky can't be reached, they keep the stored text. */
async function fill(cards: ReturnType<typeof card>[]) {
  const uris = cards.flatMap(({ uri }) => uri ? [uri] : []);
  if (!uris.length) return;
  let posts: Map<string, Post>;
  try {
    posts = await fetchPosts(uris);
  } catch {
    return;
  }
  for (const { uri, fill } of cards) if (uri) fill(posts.get(uri));
}

// Escape hides an open similarity tip until the pointer or focus leaves its score.
document.addEventListener('keydown', event => {
  if (event.key === 'Escape') {
    document.querySelectorAll('.similarity:is(:hover, :focus)').forEach(open => open.classList.add('dismissed'));
  }
});

function citation(result: number): HTMLAnchorElement {
  const link = element('a', 'cite', String(result));
  link.href = `#result-${result}`;
  link.setAttribute('aria-label', `Result ${result}`);
  return link;
}

function element<K extends keyof HTMLElementTagNameMap>(tag: K, className = '', ...children: (Node | string)[]) {
  const created = document.createElement(tag);
  if (className) created.className = className;
  created.append(...children);
  return created;
}

/**
 * A result as a post card, showing the stored text at once. Filling it from Bluesky adds the author, likes and images,
 * or says the post is gone.
 */
function card(result: SearchResult, index: number) {
  const number = index + 1;
  const time = element('time', '', ago(result.seenAt));
  time.dateTime = result.seenAt;
  time.title = new Date(result.seenAt).toLocaleString();
  const avatar = element('span', 'avatar');
  const names = element('span', 'names', element('span', '', time));
  const who = element('div', 'who', avatar, names);
  const text = element('p', 'text', result.text);
  // The tip shows on hover or focus. Screen readers hear the same words once, from the page's shared description.
  const tip = element('span', 'tip', similarityMeaning);
  tip.setAttribute('aria-hidden', 'true');
  const similarity = element('span', 'similarity', result.similarity.toFixed(2), tip);
  similarity.tabIndex = 0;
  similarity.setAttribute('aria-describedby', 'similarity-meaning');
  const undismiss = () => similarity.classList.remove('dismissed');
  similarity.addEventListener('pointerleave', undismiss);
  similarity.addEventListener('blur', undismiss);
  const link = element('a', '', 'Open on Bluesky ↗');
  link.href = result.url;
  link.target = '_blank';
  link.rel = 'noopener';
  const foot = element('p', 'foot', similarity, link);
  const item = element('li', '', element('span', 'n', String(number).padStart(2, '0')),
    element('article', 'post', who, text, foot));
  item.id = `result-${number}`;
  return {
    item,
    uri: atUri(result.url),
    fill(post: Post | undefined) {
      if (!post) {
        who.remove();
        link.remove();
        text.className = 'gone';
        text.textContent = 'This post is no longer available on Bluesky.';
        return;
      }
      if (post.avatar) {
        const image = element('img', 'avatar');
        image.src = post.avatar;
        image.alt = '';
        avatar.replaceWith(image);
      }
      names.replaceChildren(element('b', '', post.name), element('span', '', `@${post.handle} · `, time));
      if (post.images.length) text.after(media(post.images));
      const heart = element('span', '', '♡ ');
      heart.setAttribute('aria-hidden', 'true');
      link.before(element('span', '', heart, post.likes.toLocaleString('en'),
        element('span', 'visually-hidden', post.likes === 1 ? ' like' : ' likes')));
    },
  };
}

/** Up to four images, the fourth showing how many more there are. */
function media(images: Image[]): HTMLDivElement {
  return element('div', 'media', ...images.slice(0, 4).map((image, i) => {
    const shown = element('img');
    shown.src = image.src;
    shown.alt = image.alt;
    shown.loading = 'lazy';
    const more = i === 3 && images.length > 4 ? [element('span', 'more', `+${images.length - 4}`)] : [];
    return element('span', '', shown, ...more);
  }));
}
