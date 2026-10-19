import './style.css';
import { ago, parseResults } from './results';
import type { SearchResult } from './results';

function element(id: string): HTMLElement {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing page element: ${id}`);
  return found;
}
const status = element('result-status');
const error = element('error');
const results = element('results');
// The form submits as a plain GET, so each search is its own page in history and can be shared as a link.
const query = new URLSearchParams(location.search).get('q')?.trim() ?? '';
(element('q') as HTMLInputElement).value = query;
if (query) void search(query);

async function search(query: string) {
  status.textContent = 'Searching…';
  try {
    const response = await fetch(`/api/search?${new URLSearchParams({ q: query })}`);
    if (!response.ok) throw new Error(failure(response.status));
    const found = parseResults(await response.json().catch(() => { throw new Error('the server’s answer wasn’t JSON.'); }));
    results.replaceChildren(...found.map(render));
    status.textContent = found.length
      ? `The ${found.length === 1 ? 'post' : `${found.length} posts`} closest in meaning to “${query}”.`
      : 'No posts are searchable yet. Captured posts become searchable once they’re embedded, usually within seconds.';
  } catch (cause) {
    status.textContent = '';
    error.textContent = `Could not search: ${cause instanceof Error ? cause.message : 'Unknown error'}`;
    error.hidden = false;
  }
}

function failure(httpStatus: number): string {
  if (httpStatus === 400) return 'enter a search of at most 200 characters.';
  if (httpStatus === 404) return 'search is not enabled on this server.';
  return `the server answered ${httpStatus}.`;
}

function render(result: SearchResult): HTMLLIElement {
  const text = document.createElement('p');
  text.className = 'text';
  text.textContent = result.text;
  const time = document.createElement('time');
  time.dateTime = result.seenAt;
  time.title = new Date(result.seenAt).toLocaleString();
  time.textContent = ago(result.seenAt);
  const link = document.createElement('a');
  link.href = result.url;
  link.target = '_blank';
  link.rel = 'noopener';
  link.textContent = 'View on Bluesky';
  const meta = document.createElement('p');
  meta.className = 'meta';
  meta.append(time, ' · ', link);
  const item = document.createElement('li');
  item.append(text, meta);
  return item;
}
