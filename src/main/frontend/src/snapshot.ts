export const snapshotWindows = [
  { seconds: 30, label: '30 seconds' },
  { seconds: 60, label: '1 minute' },
  { seconds: 300, label: '5 minutes' },
  { seconds: 600, label: '10 minutes' },
  { seconds: 1800, label: '30 minutes' },
  { seconds: 3600, label: '1 hour' },
];
const storageKey = 'skystream.snapshotWindowSeconds';

export function loadSnapshotWindow(): number {
  try {
    const saved = Number(window.localStorage.getItem(storageKey));
    return snapshotWindows.some(window => window.seconds === saved) ? saved : 30;
  } catch {
    return 30;
  }
}

export function saveSnapshotWindow(seconds: number) {
  try {
    window.localStorage.setItem(storageKey, String(seconds));
  } catch {
    // The setting still works for this visit when browser storage is unavailable.
  }
}
