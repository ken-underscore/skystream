import './style.css';
import { parseBucket } from './bucket';
import { createCloud } from './cloud';
import { connectStream } from './stream';

function element(id: string): HTMLElement {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing page element: ${id}`);
  return found;
}
const demo = new URLSearchParams(location.search).get('demo') === '1';
const empty = element('empty');
const error = element('error');
const mode = element('mode') as HTMLAnchorElement;
mode.href = demo ? location.pathname : '?demo=1';
mode.textContent = demo ? 'Connect live' : 'Try demo';
const renderer = createCloud(element('cloud'), (placed, selected) => {
  empty.hidden = placed > 0;
  empty.textContent = selected ? 'No words fit this view.' : 'No words in this bucket.';
  element('summary').textContent = `Showing ${placed} of ${selected} selected words.`;
});
const disconnect = connectStream(demo, json => {
  try {
    const words = parseBucket(json);
    renderer.update(words);
    error.hidden = true;
    element('updated').textContent = `Received at ${new Date().toLocaleTimeString()}`;
    element('title').textContent = words.length
      ? `Latest 30-second snapshot: ${words.length} unique words. Larger words occurred more often.`
      : 'Latest 30-second snapshot contains no words.';
  } catch (cause) {
    error.textContent = `Could not read bucket: ${cause instanceof Error ? cause.message : 'Invalid data'}`;
    error.hidden = false;
  }
}, value => {
  element('status').textContent = value;
  element('status').dataset.connected = String(value === 'Connected' || value === 'Demo mode');
});
function cleanup() { disconnect(); renderer.destroy(); }
window.addEventListener('pagehide', cleanup, { once: true });
window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
if (import.meta.hot) import.meta.hot.dispose(cleanup);
