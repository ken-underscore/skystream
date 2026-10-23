# Skystream frontend

TypeScript receives combined word-count snapshots, d3-cloud calculates positions, and D3 draws SVG text. Each snapshot replaces the previous cloud when layout finishes. The top 100 words are selected; fewer may fit. Hover over words to see counts. Click a word, or Tab to it and press Enter, to open its Bluesky search results in a new tab; the cloud keeps updating in the original tab.

Use **Snapshot** to choose the preceding 30 seconds (default), 1 minute, 5 minutes, 10 minutes, 30 minutes, 1 hour, 3 hours, 8 hours, or 24 hours. Windows over an hour update every 5 minutes. When the app hasn't collected all of a window yet, because it hasn't run that long or was down, a note says how much it has. Each browser remembers its choice locally. Changing it closes the previous SSE stream and connects for the new window. The server sends its current cached snapshot immediately, even with a slower refresh setting. Until it arrives, the previous cloud stays visible. Windows use canonical bucket timestamps rather than browser time.

Use the **Transition** selector to compare crossfade (the default), smooth movement and resizing, fade out / fade in, or instant replacement. Changing the selection previews that style using the current snapshot. Transitions last 650 ms and unfinished animations are interrupted when a newer layout is ready. The first cloud appears immediately, and the browser's reduced-motion preference disables animations. Moving words can overlap briefly while traveling to their final positions.

Use **Refresh** to receive every cache refresh (the **Every batch** default), or receive the newest cache refresh at most once every 5, 10, 30, or 60 seconds. Each browser remembers its choice locally. Refresh and snapshot duration are independent. Changing either preference reconnects SSE, receives the first available cached result immediately, and then follows the selected interval. Unchanged results are not sent again. The browser renders received snapshots directly; it has no separate redraw timer.

The search page, `search.html`, searches the last 24 hours of English posts by meaning through `GET /api/search/stream?q=`, which needs the backend running with search enabled (see the project README). Each search is a plain form submission, so it has its own URL and history entry. Results are numbered and show each post's text, its similarity to the search, how long ago it was seen, and a link to it on Bluesky. Claude's answer streams in below them, its citations, such as `[2]`, linking to the results. The page closes the event stream when it ends, since `EventSource` would otherwise reconnect and search again.

## Run

From `src/main/frontend`:

```sh
pnpm install
pnpm dev
```

Open the URL Vite prints. Click **Try demo**, or append `?demo=1`, for an immediate sample snapshot without Java. Demo mode aggregates synthetic five-second buckets for the selected duration, with one hour of sample history available immediately (longer windows scale it up), and delivers results at the selected refresh rate. Demo mode never connects to the backend.

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

- `src/main.ts`: word cloud page setup.
- `src/stream.ts`: SSE connection and demo feed.
- `src/refresh.ts`: supported refresh intervals and browser preference storage.
- `src/snapshot.ts`: snapshot duration options and browser preference storage.
- `src/bucket.ts`: JSON validation and sorting.
- `src/cloud.ts`: font sizing, cancellable layouts, rendering, and resizing.
- `src/search.ts`: Bluesky search links for words.
- `src/search-page.ts`: search page setup, the event stream, and rendering.
- `src/results.ts`: search result and answer validation, citations, and relative times.
- `src/style.css`: responsive page styling.

See the project README for Java and PostgreSQL startup instructions.
