# Skystream frontend

TypeScript receives combined word-count snapshots, d3-cloud calculates positions, and D3 draws SVG text. Each snapshot replaces the previous cloud when layout finishes. The top 100 words are selected; fewer may fit. Hover over words to see counts.

Use **Snapshot** to choose the preceding 30 seconds (default), 1 minute, 5 minutes, 10 minutes, 30 minutes, or 1 hour. Each browser remembers its choice locally. Changing it closes the previous SSE stream, discards pending redraws, and requests a new aggregate. The first snapshot for the new window appears immediately, even with a slower refresh setting. Until it arrives, the previous cloud stays visible. Windows use canonical bucket timestamps rather than browser time; a database with less history returns whatever is available.

Use the **Transition** selector to compare crossfade (the default), smooth movement and resizing, fade out / fade in, or instant replacement. Changing the selection previews that style using the current snapshot. Transitions last 650 ms and unfinished animations are interrupted when a newer layout is ready. The first cloud appears immediately, and the browser's reduced-motion preference disables animations. Moving words can overlap briefly while traveling to their final positions.

Use **Refresh** to display every batch (the default), or refresh at most once every 5, 10, 30, or 60 seconds. Each browser remembers its choice locally. The first snapshot appears immediately; while waiting, newer snapshots replace the pending one instead of queuing clouds. Changing the interval reschedules any pending update. Refresh and snapshot duration are independent. Timed settings only redraw when new data is available, and can never provide updates faster than the backend publishes them.

## Run

From `src/main/frontend`:

```sh
pnpm install
pnpm dev
```

Open the URL Vite prints. Click **Try demo**, or append `?demo=1`, for an immediate sample snapshot followed by updates every five seconds without Java. Demo mode aggregates synthetic five-second buckets for the selected duration, with one hour of sample history available immediately. Demo mode never connects to the backend.

```sh
pnpm check   # Check TypeScript
pnpm build   # Check types and build into dist/
pnpm preview # Preview the production build
```

## Backend contract

The frontend opens `GET /api/word-buckets?windowSeconds=<seconds>` through native `EventSource`, using `30`, `60`, `300`, `600`, `1800`, or `3600`. Omitting the parameter defaults to `30`; invalid values return HTTP 400. Vite proxies `/api` to `http://localhost:8080` during development; adjust `vite.config.ts` if needed.

Respond with `Content-Type: text/event-stream`. On subscription, query the latest published checkpoint for the selected duration; before the first flush after startup, wait for committed progress. After every successfully committed bucket flush, send one unnamed message containing combined word counts for that duration, ending with a blank line:

```text
data: {"hello":12,"world":7}

```

Send JSON, not Java's `Map.toString()` output. Keys must be nonblank words, with positive safe integer counts. `{}` clears the cloud. No timestamps, named events, replay, or historical accumulation are required. Flush each event and avoid buffering the stream.

The browser reconnects automatically after connection errors, preserving the previous cloud while waiting. Invalid data reports an error and preserves the cloud. Java handles persistence, database aggregation, and snapshot publication. The browser displays the latest received snapshot according to its refresh preference. Canonical bucket timing, additive updates, and checkpoint recovery are described in the project README.

For production, serve `dist/` and `/api` on the same origin, or configure a hosting proxy. Vite's development proxy is not part of the production build.

## Code

- `src/main.ts`: page setup.
- `src/stream.ts`: SSE connection and demo feed.
- `src/refresh.ts`: refresh scheduling and browser preference storage.
- `src/snapshot.ts`: snapshot duration options and browser preference storage.
- `src/bucket.ts`: JSON validation and sorting.
- `src/cloud.ts`: font sizing, cancellable layouts, rendering, and resizing.
- `src/style.css`: responsive page styling.

See the project README for Java and PostgreSQL startup instructions.
