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
