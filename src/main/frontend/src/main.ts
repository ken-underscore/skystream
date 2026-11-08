import './style.css';
import { createChart } from './chart';
import { connectStream } from './stream';
import { loadRefreshInterval, refreshIntervals, saveRefreshInterval } from './refresh';
import { duration, loadSnapshotWindow, parseSnapshot, saveSnapshotWindow, snapshotWindows } from './snapshot';

/** How many of the snapshot's most used words the chart shows. The server sends this many: see `TOP_WORDS`. */
const shown = 50;

function element(id: string): HTMLElement {
  const found = document.getElementById(id);
  if (!found) throw new Error(`Missing page element: ${id}`);
  return found;
}
// Demo mode is for the development server only.
const demo = import.meta.env.DEV && new URLSearchParams(location.search).get('demo') === '1';
const chart = element('chart') as HTMLOListElement;
const empty = element('empty');
const error = element('error');
const collected = element('collected');
const status = element('status');
const update = createChart(chart);

// The window reads as the end of the heading, as in "in the last hour", not "in the last 1 hour".
const inHeading = (label: string) => label.replace(/^1 /, '');
const snapshot = element('snapshot') as HTMLSelectElement;
snapshot.replaceChildren(...snapshotWindows.map(window => new Option(inHeading(window.label), String(window.seconds))));
snapshot.value = String(loadSnapshotWindow());
const refresh = element('refresh') as HTMLSelectElement;
refresh.replaceChildren(...refreshIntervals.map(interval => new Option(interval.label, String(interval.milliseconds))));
refresh.value = String(loadRefreshInterval());

function startStream() {
  const lastWindow = `the last ${snapshot.selectedOptions.item(0)?.textContent ?? '30 seconds'}`;
  const windowSeconds = Number(snapshot.value);
  chart.setAttribute('aria-label', `The ${shown} words used most in ${lastWindow}, with how often each was used`);
  return connectStream(demo, windowSeconds, Number(refresh.value), json => {
    try {
      const { words, collectedSeconds } = parseSnapshot(json);
      update(words.slice(0, shown));
      empty.hidden = words.length > 0;
      empty.textContent = 'No words in this window yet.';
      // The app holds only what it has collected since it began, minus any time it was down.
      collected.hidden = collectedSeconds >= windowSeconds;
      collected.textContent = collectedSeconds
        ? `Only ${duration(collectedSeconds)} of ${lastWindow} has been collected so far.`
        : `Nothing from ${lastWindow} has been collected yet.`;
      element('updated').textContent = `Updated ${new Date().toLocaleTimeString()}`;
      error.hidden = true;
    } catch (cause) {
      error.textContent = `Could not read the latest words: ${cause instanceof Error ? cause.message : 'invalid data'}`;
      error.hidden = false;
    }
  }, value => {
    const live = value === 'Connected' || value === 'Demo mode';
    status.textContent = value === 'Connected' ? 'Live' : value;
    status.dataset.live = String(live);
  });
}
let disconnect = startStream();
function restartStream() {
  disconnect();
  error.hidden = collected.hidden = true;
  disconnect = startStream();
}
snapshot.addEventListener('change', () => {
  saveSnapshotWindow(Number(snapshot.value));
  restartStream();
});
refresh.addEventListener('change', () => {
  saveRefreshInterval(Number(refresh.value));
  restartStream();
});
window.addEventListener('pagehide', () => disconnect(), { once: true });
window.addEventListener('pageshow', event => { if (event.persisted) location.reload(); });
if (import.meta.hot) import.meta.hot.dispose(() => disconnect());
