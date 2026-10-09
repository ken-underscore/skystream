import './style.css';
import { parseBucket } from './bucket';
import { createCloud } from './cloud';
import type { CloudTransition } from './cloud';
import { connectStream } from './stream';
import { loadRefreshInterval, refreshIntervals, saveRefreshInterval } from './refresh';
import { loadSnapshotWindow, saveSnapshotWindow, snapshotWindows } from './snapshot';

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
const transitionDescriptions: Record<CloudTransition, string> = {
  crossfade: 'The old cloud fades out while the new cloud fades in.',
  move: 'Existing words move and resize; arriving and departing words fade.',
  fade: 'The old cloud fades away before the new cloud appears.',
  none: 'Each new cloud appears immediately.',
};
const transition = element('transition') as HTMLSelectElement;
transition.addEventListener('change', () => {
  const mode = transition.value as CloudTransition;
  element('transition-description').textContent = transitionDescriptions[mode];
  renderer.setTransition(mode);
});
const refresh = element('refresh') as HTMLSelectElement;
refresh.value = String(loadRefreshInterval());
const snapshot = element('snapshot') as HTMLSelectElement;
snapshot.replaceChildren(...snapshotWindows.map(window => new Option(window.label, String(window.seconds))));
snapshot.value = String(loadSnapshotWindow());
refresh.addEventListener('change', () => {
  const interval = Number(refresh.value);
  if (!refreshIntervals.includes(interval)) return;
  saveRefreshInterval(interval);
  restartStream();
});
function startStream() {
  const windowLabel = snapshot.selectedOptions.item(0)?.textContent ?? '30 seconds';
  return connectStream(demo, Number(snapshot.value), Number(refresh.value), json => {
    try {
      const words = parseBucket(json);
      renderer.update(words);
      element('updated').textContent = `Received at ${new Date().toLocaleTimeString()}`;
      element('title').textContent = words.length
        ? `Latest snapshot (${windowLabel}): ${words.length} unique words. Larger words occurred more often.`
        : `Latest snapshot (${windowLabel}) contains no words.`;
      error.hidden = true;
    } catch (cause) {
      error.textContent = `Could not read bucket: ${cause instanceof Error ? cause.message : 'Invalid data'}`;
      error.hidden = false;
    }
  }, value => {
    element('status').textContent = value;
    element('status').dataset.connected = String(value === 'Connected' || value === 'Demo mode');
  });
}
let disconnect = startStream();
function restartStream() {
  disconnect();
  error.hidden = true;
  disconnect = startStream();
}
snapshot.addEventListener('change', () => {
  saveSnapshotWindow(Number(snapshot.value));
  restartStream();
});
function cleanup() { disconnect(); renderer.destroy(); }
window.addEventListener('pagehide', cleanup, { once: true });
window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
if (import.meta.hot) import.meta.hot.dispose(cleanup);
