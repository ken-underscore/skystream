export const refreshIntervals = [
  { milliseconds: 0, label: 'Every batch' },
  { milliseconds: 5_000, label: '5 seconds' },
  { milliseconds: 10_000, label: '10 seconds' },
  { milliseconds: 30_000, label: '30 seconds' },
  { milliseconds: 60_000, label: '60 seconds' },
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
