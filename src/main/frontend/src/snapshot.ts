export const snapshotWindows = [
  { seconds: 30, label: '30 seconds' },
  { seconds: 60, label: '1 minute' },
  { seconds: 300, label: '5 minutes' },
  { seconds: 600, label: '10 minutes' },
  { seconds: 1800, label: '30 minutes' },
  { seconds: 3600, label: '1 hour' },
  { seconds: 10800, label: '3 hours' },
  { seconds: 28800, label: '8 hours' },
  { seconds: 86400, label: '24 hours' },
];
const storageKey = 'skystream.snapshotWindowSeconds';

const amount = (value: number, unit: 'hour' | 'minute' | 'second') =>
  new Intl.NumberFormat('en', { style: 'unit', unit, unitDisplay: 'long' }).format(value);

/** A length of time in words, to the minute once it's a minute or more, as in "2 hours 10 minutes". */
export function duration(seconds: number): string {
  if (seconds < 60) return amount(seconds, 'second');
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor(seconds % 3600 / 60);
  return [hours && amount(hours, 'hour'), minutes && amount(minutes, 'minute')].filter(Boolean).join(' ');
}

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
