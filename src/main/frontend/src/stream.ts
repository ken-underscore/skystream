export function connectStream(demo: boolean, onBucket: (json: string) => void, onStatus: (status: string) => void): () => void {
  if (demo) {
    onStatus('Demo mode');
    const words = 'bluesky community music books art science nature coffee games space garden ocean travel movies technology friends photography weather coding cats dogs food sports culture learning design stories morning weekend stars creativity language city forest birds flowers conversation ideas discovery'.split(' ');
    const send = () => onBucket(JSON.stringify(Object.fromEntries(words.map(word => [word, 1 + Math.floor(Math.random() ** 2 * 100)]))));
    send();
    const timer = window.setInterval(send, 30_000);
    return () => window.clearInterval(timer);
  }
  onStatus('Connecting…');
  const source = new EventSource('/api/word-buckets');
  source.onopen = () => onStatus('Connected');
  source.onerror = () => onStatus('Reconnecting…');
  source.onmessage = event => onBucket(event.data);
  return () => source.close();
}
