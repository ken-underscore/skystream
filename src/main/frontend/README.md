# Skystream frontend

TypeScript receives combined 30-second word-count snapshots, d3-cloud calculates positions, and D3 draws SVG text. Each snapshot replaces the previous cloud when layout finishes. The top 100 words are selected; fewer may fit. Hover over words to see counts.

## Run

From `src/main/frontend`:

```sh
pnpm install
pnpm dev
```

Open the URL Vite prints. Click **Try demo**, or append `?demo=1`, for an immediate sample snapshot followed by updates every 30 seconds without Java. Demo mode never connects to the backend.

```sh
pnpm check   # Check TypeScript
pnpm build   # Check types and build into dist/
pnpm preview # Preview the production build
```

## Backend contract

The frontend opens `GET /api/word-buckets` through native `EventSource`. Vite proxies `/api` to `http://localhost:8080` during development; adjust `vite.config.ts` if needed.

Respond with `Content-Type: text/event-stream`. After six successfully persisted five-second buckets, send one unnamed message containing combined word counts for the completed 30-second window, ending with a blank line:

```text
data: {"hello":12,"world":7}

```

Send JSON, not Java's `Map.toString()` output. Keys must be nonblank words, with positive safe integer counts. `{}` clears the cloud. No timestamps, named events, replay, or historical accumulation are required. Flush each event and avoid buffering the stream.

The browser reconnects automatically after connection errors, preserving the previous cloud while waiting. Invalid data reports an error and preserves the cloud. The Java endpoint, persistence, database aggregation, and snapshot publication are your backend work. The browser renders each received snapshot immediately; it does not count six events or run a refresh timer.

For production, serve `dist/` and `/api` on the same origin, or configure a hosting proxy. Vite's development proxy is not part of the production build.

## Code

- `src/main.ts`: page setup.
- `src/stream.ts`: SSE connection and demo feed.
- `src/bucket.ts`: JSON validation and sorting.
- `src/cloud.ts`: font sizing, cancellable layouts, rendering, and resizing.
- `src/style.css`: responsive page styling.

Java sources and Maven configuration are unchanged.
