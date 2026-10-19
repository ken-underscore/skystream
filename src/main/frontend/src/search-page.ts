import './style.css';
import { ago, citedParts, parseAnswerText, parseResults } from './results';
import type { SearchResult } from './results';

function element(id: string): HTMLElement {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing page element: ${id}`);
  return found;
}
const status = element('result-status');
const error = element('error');
const results = element('results');
const answer = element('answer');
const answerText = element('answer-text');
// The form submits as a plain GET, so each search is its own page in history and can be shared as a link.
const query = new URLSearchParams(location.search).get('q')?.trim() ?? '';
(element('q') as HTMLInputElement).value = query;
if (query) search(query);

function search(query: string) {
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
    results.replaceChildren(...found.map(render));
    status.textContent = found.length
      ? `The ${found.length === 1 ? 'post' : `${found.length} posts`} closest in meaning to “${query}”.`
      : 'No posts are searchable yet. Captured posts become searchable once they’re embedded, usually within seconds.';
    answer.hidden = found.length === 0;
    answerText.setAttribute('aria-busy', 'true');
    answerText.textContent = 'Writing an answer…';
  });
  source.addEventListener('answer', event => {
    try {
      written += parseAnswerText(JSON.parse(event.data));
    } catch {
      end('Could not read the answer.');
      return;
    }
    answerText.replaceChildren(...citedParts(written, found?.length ?? 0).map(part => typeof part === 'string'
      ? part : Object.assign(document.createElement('a'), { href: `#result-${part}`, textContent: `[${part}]` })));
  });
  source.addEventListener('done', () => end());
  source.addEventListener('failed', () => end('Could not write an answer this time. The results above are unaffected.'));
  // The browser hides why a stream failed, so the likeliest causes are named instead.
  source.addEventListener('error', () => found
    ? end('The answer stopped early.')
    : fail('the server didn’t answer. It may be down, or not have search enabled.'));
}

function render(result: SearchResult, index: number): HTMLLIElement {
  const text = document.createElement('p');
  text.className = 'text';
  text.textContent = result.text;
  const number = document.createElement('span');
  number.className = 'number';
  number.textContent = `[${index + 1}]`;
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
  meta.append(number, ' · ', time, ' · ', link);
  const item = document.createElement('li');
  item.id = `result-${index + 1}`;
  item.append(text, meta);
  return item;
}
