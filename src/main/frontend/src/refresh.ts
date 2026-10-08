export const refreshIntervals = [0, 5_000, 10_000, 30_000, 60_000];
const storageKey = 'skystream.refreshIntervalMs';

export function loadRefreshInterval(): number {
  try {
    const saved = Number(window.localStorage.getItem(storageKey));
    return refreshIntervals.includes(saved) ? saved : 0;
  } catch {
    return 0;
  }
}

export function saveRefreshInterval(interval: number) {
  try {
    window.localStorage.setItem(storageKey, String(interval));
  } catch {
    // The setting still works for this visit when browser storage is unavailable.
  }
}

export function createRefreshScheduler<T>(display: (snapshot: T) => void, initialInterval: number) {
  let interval = initialInterval;
  let lastDisplay = -Infinity;
  let pending: { snapshot: T } | undefined;
  let timer: number | undefined;
  let destroyed = false;

  function flush() {
    timer = undefined;
    if (destroyed || !pending) return;
    const snapshot = pending.snapshot;
    pending = undefined;
    lastDisplay = performance.now();
    display(snapshot);
  }

  function schedule() {
    if (timer !== undefined) window.clearTimeout(timer);
    timer = undefined;
    if (destroyed || !pending) return;
    const remaining = Math.max(0, interval - (performance.now() - lastDisplay));
    if (remaining === 0) flush();
    else timer = window.setTimeout(flush, remaining);
  }

  return {
    update(snapshot: T) {
      if (destroyed) return;
      pending = { snapshot };
      if (timer === undefined) schedule();
    },
    setInterval(milliseconds: number) { interval = milliseconds; schedule(); },
    destroy() {
      destroyed = true;
      pending = undefined;
      if (timer !== undefined) window.clearTimeout(timer);
      timer = undefined;
    },
  };
}
