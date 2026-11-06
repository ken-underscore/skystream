# Skystream frontend

The words page, `index.html`, shows the 50 words Bluesky's English posts used most in a chosen window, ranked, each with a bar sized against the top word and its count. TypeScript receives combined word-count snapshots and D3 joins them to a list of links, keyed by word. When a snapshot arrives, rows slide to their new ranks, bars and counts change in place, and words entering or leaving the top 50 fade. The browser's reduced-motion preference turns the animation off. Click a word, or Tab to it and press Enter, to open its Bluesky search results in a new tab; the chart keeps updating in the original tab. The page also has a search box for the search page.

Choose the window in the heading: the preceding 30 seconds (default), minute, 5 minutes, 10 minutes, 30 minutes, hour, 3 hours, 8 hours, or 24 hours. The 30-minute and 1-hour windows update every minute, and longer ones every 5 minutes. When the app hasn't collected all of a window yet, because it hasn't run that long or was down, a note says how much it has. Each browser remembers its choice locally. Changing it closes the previous SSE stream and connects for the new window. The server sends its current cached snapshot immediately, even with a slower refresh setting. Until it arrives, the previous chart stays visible. Windows use canonical bucket timestamps rather than browser time.

Use **Refresh** to receive every cache refresh (the **every batch** default), or receive the newest cache refresh at most once every 5, 10, or 30 seconds, or every minute. Each browser remembers its choice locally. Refresh and window are independent. Changing either preference reconnects SSE, receives the first available cached result immediately, and then follows the selected interval. Unchanged results are not sent again. The browser renders received snapshots directly; it has no separate redraw timer.

The search page, `search.html`, searches the last 24 hours of English posts by meaning through `GET /api/search/stream?q=`, which needs the backend running with search enabled (see the project README). Each search is a plain form submission, so it has its own URL and history entry. Results are numbered post cards, best match first, showing each post's text, how long ago it was seen, its similarity to the search (hover or focus it for what the number means), and a link to it on Bluesky. The cards then fill in from Bluesky's public API (`app.bsky.feed.getPosts`, which allows any origin): the author's name, handle and avatar, the like count, and up to four images. Images come only from Bluesky's image host, and stay hidden on posts or accounts labelled as adult or graphic, as Bluesky hides them by default. A post that was deleted or isn't public says so instead of showing its text. If Bluesky can't be reached, the cards keep the stored text. The generated summary streams in above the results, its citations linking to them. The page closes the event stream when it ends, since `EventSource` would otherwise reconnect and search again. When the backend's daily budget is spent, the page says summaries are paused until midnight UTC. A visitor who has searched too often is told how long to wait.

## Run

From `src/main/frontend`:

```sh
pnpm install
pnpm dev
```

Open the URL Vite prints. Click **Demo**, or append `?demo=1`, for an immediate sample snapshot without Java. Demo mode aggregates synthetic five-second buckets for the selected duration, with one hour of sample history available immediately (longer windows scale it up), and delivers results at the selected refresh rate. Demo mode never connects to the backend.

```sh
pnpm check   # Check TypeScript
pnpm build   # Check types and build into dist/
pnpm preview # Preview the production build
```

## Backend contract

The frontend opens `GET /api/word-buckets?windowSeconds=<seconds>&refreshIntervalMs=<milliseconds>` through native `EventSource`. Window values are `30`, `60`, `300`, `600`, `1800`, `3600`, `10800`, `28800`, or `86400`; refresh values are `0`, `5000`, `10000`, `30000`, or `60000`. Defaults are `30` and `0`, and invalid values return HTTP 400. Vite proxies `/api` to `http://localhost:8080` during development; adjust `vite.config.ts` if needed.

Respond with `Content-Type: text/event-stream`. Send the current cached window immediately on subscription, then on cache updates (`refreshIntervalMs=0`) or at the selected timer interval. Caches start empty until the first committed batch and successful aggregate query after startup. Each unnamed message contains combined word counts for the selected duration, ending with a blank line:

```text
data: {"hello":12,"world":7}

```

Send JSON, not Java's `Map.toString()` output. Keys must be nonblank words, with positive safe integer counts. `{}` clears the chart. No timestamps, named events, replay, or historical accumulation are required. Flush each event and avoid buffering the stream.

The browser reconnects automatically after connection errors, preserving the previous chart while waiting. Invalid data reports an error and preserves the chart. Java handles persistence, shared cache refreshes, and delivery timing; the browser validates and draws each received snapshot. Canonical bucket timing, additive updates, and checkpoint recovery are described in the project README.

For production, serve `dist/` and `/api` on the same origin, or configure a hosting proxy. Vite's development proxy is not part of the production build.

## Code

- `src/main.ts`: words page setup.
- `src/stream.ts`: SSE connection and demo feed.
- `src/refresh.ts`: supported refresh intervals and browser preference storage.
- `src/snapshot.ts`: snapshot validation and sorting, duration options, and browser preference storage.
- `src/chart.ts`: the ranked bar chart and its animation.
- `src/search.ts`: Bluesky search links for words.
- `src/search-page.ts`: search page setup, the event stream, and post cards.
- `src/bluesky.ts`: post details from Bluesky's public API.
- `src/results.ts`: search result and answer validation, citations, and relative times.
- `src/style.css`: responsive page styling, with IBM Plex fonts bundled from Fontsource.

See the project README for Java and PostgreSQL startup instructions.
