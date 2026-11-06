export const refreshIntervals = [
  { milliseconds: 0, label: 'every batch' },
  { milliseconds: 5_000, label: 'every 5 seconds' },
  { milliseconds: 10_000, label: 'every 10 seconds' },
  { milliseconds: 30_000, label: 'every 30 seconds' },
  { milliseconds: 60_000, label: 'every minute' },
];
const storageKey = 'skystream.refreshIntervalMs';

export function loadRefreshInterval(): number {
  try {
    const saved = Number(window.localStorage.getItem(storageKey));
    return refreshIntervals.some(interval => interval.milliseconds === saved) ? saved : 0;
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
