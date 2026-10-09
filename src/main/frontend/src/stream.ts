export function connectStream(demo: boolean, windowSeconds: number, refreshIntervalMs: number, onBucket: (json: string) => void, onStatus: (status: string) => void): () => void {
  let closed = false;
  if (demo) {
    onStatus('Demo mode');
    const words = 'bluesky community music books art science nature coffee games space garden ocean travel movies technology friends photography weather coding cats dogs food sports culture learning design stories morning weekend stars creativity language city forest birds flowers conversation ideas discovery'.split(' ');
    const bucket = () => Object.fromEntries(words.map(word => [word, 1 + Math.floor(Math.random() ** 2 * 100)]));
    // Synthetic history lets all six windows work immediately in the demo.
    const history = Array.from({ length: 3600 / 5 }, bucket);
    const send = () => {
      if (closed) return;
      const counts: Record<string, number> = {};
      for (const bucket of history.slice(-windowSeconds / 5)) {
        for (const [word, count] of Object.entries(bucket)) counts[word] = (counts[word] ?? 0) + count;
      }
      onBucket(JSON.stringify(counts));
    };
    send();
    const timer = window.setInterval(() => {
      history.shift();
      history.push(bucket());
      if (refreshIntervalMs === 0) send();
    }, 5_000);
    const delivery = refreshIntervalMs > 0 ? window.setInterval(send, refreshIntervalMs) : undefined;
    return () => { closed = true; window.clearInterval(timer); window.clearInterval(delivery); };
  }
  onStatus('Connecting…');
  const source = new EventSource(`/api/word-buckets?windowSeconds=${windowSeconds}&refreshIntervalMs=${refreshIntervalMs}`);
  source.onopen = () => { if (!closed) onStatus('Connected'); };
  source.onerror = () => { if (!closed) onStatus('Reconnecting…'); };
  source.onmessage = event => { if (!closed) onBucket(event.data); };
  return () => { closed = true; source.close(); };
}
