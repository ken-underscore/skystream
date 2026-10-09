# Skystream frontend

TypeScript receives combined word-count snapshots, d3-cloud calculates positions, and D3 draws SVG text. Each snapshot replaces the previous cloud when layout finishes. The top 100 words are selected; fewer may fit. Hover over words to see counts.

Use **Snapshot** to choose the preceding 30 seconds (default), 1 minute, 5 minutes, 10 minutes, 30 minutes, or 1 hour. Each browser remembers its choice locally. Changing it closes the previous SSE stream and connects for the new window. The server sends its current cached snapshot immediately, even with a slower refresh setting. Until it arrives, the previous cloud stays visible. Windows use canonical bucket timestamps rather than browser time; a database with less history returns whatever is available.

Use the **Transition** selector to compare crossfade (the default), smooth movement and resizing, fade out / fade in, or instant replacement. Changing the selection previews that style using the current snapshot. Transitions last 650 ms and unfinished animations are interrupted when a newer layout is ready. The first cloud appears immediately, and the browser's reduced-motion preference disables animations. Moving words can overlap briefly while traveling to their final positions.

Use **Refresh** to receive every cache refresh (the **Every batch** default), or receive the server's current cache every 5, 10, 30, or 60 seconds. Each browser remembers its choice locally. Refresh and snapshot duration are independent. Changing either preference reconnects SSE, receives the first available cached result immediately, and then follows the selected interval. Timed delivery can repeat unchanged results. The browser renders received snapshots directly; it has no separate redraw timer.

## Run

From `src/main/frontend`:

```sh
pnpm install
pnpm dev
```

Open the URL Vite prints. Click **Try demo**, or append `?demo=1`, for an immediate sample snapshot without Java. Demo mode aggregates synthetic five-second buckets for the selected duration, with one hour of sample history available immediately, and delivers results at the selected refresh rate. Demo mode never connects to the backend.

```sh
pnpm check   # Check TypeScript
pnpm build   # Check types and build into dist/
pnpm preview # Preview the production build
```

## Backend contract

The frontend opens `GET /api/word-buckets?windowSeconds=<seconds>&refreshIntervalMs=<milliseconds>` through native `EventSource`. Window values are `30`, `60`, `300`, `600`, `1800`, or `3600`; refresh values are `0`, `5000`, `10000`, `30000`, or `60000`. Defaults are `30` and `0`, and invalid values return HTTP 400. Vite proxies `/api` to `http://localhost:8080` during development; adjust `vite.config.ts` if needed.

Respond with `Content-Type: text/event-stream`. Send the current cached window immediately on subscription, then on cache updates (`refreshIntervalMs=0`) or at the selected timer interval. Caches start empty until the first committed batch and successful aggregate query after startup. Each unnamed message contains combined word counts for the selected duration, ending with a blank line:

```text
data: {"hello":12,"world":7}

```

Send JSON, not Java's `Map.toString()` output. Keys must be nonblank words, with positive safe integer counts. `{}` clears the cloud. No timestamps, named events, replay, or historical accumulation are required. Flush each event and avoid buffering the stream.

The browser reconnects automatically after connection errors, preserving the previous cloud while waiting. Invalid data reports an error and preserves the cloud. Java handles persistence, shared cache refreshes, and delivery timing; the browser validates and draws each received snapshot. Canonical bucket timing, additive updates, and checkpoint recovery are described in the project README.

For production, serve `dist/` and `/api` on the same origin, or configure a hosting proxy. Vite's development proxy is not part of the production build.

## Code

- `src/main.ts`: page setup.
- `src/stream.ts`: SSE connection and demo feed.
- `src/refresh.ts`: supported refresh intervals and browser preference storage.
- `src/snapshot.ts`: snapshot duration options and browser preference storage.
- `src/bucket.ts`: JSON validation and sorting.
- `src/cloud.ts`: font sizing, cancellable layouts, rendering, and resizing.
- `src/style.css`: responsive page styling.

See the project README for Java and PostgreSQL startup instructions.
