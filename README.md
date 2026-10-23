# Skystream

Jetstream post commits become canonical five-second word-count buckets in PostgreSQL. The accumulator flushes when an event enters a newer bucket. After committed flushes, one PostgreSQL filtered-aggregate query refreshes a shared in-memory cache containing six durations: 30 seconds (default), 1 minute, 5 minutes, 10 minutes, 30 minutes, and 1 hour. SSE delivers the selected cached window at each visitor's refresh rate. Each browser independently saves both preferences.

## Run locally

Requires Java 21, Docker with Compose, and the frontend's pnpm setup.

From the project root:

```sh
./mvnw spring-boot:run
```

Spring Boot starts PostgreSQL from `compose.yaml`, discovers its mapped port and credentials, initializes the bucket schema, and connects to Jetstream on startup. The Compose volume retains data. The HTTP server defaults to port 8080.

For a clean start, stop any running instance of the app and run:

```sh
./mvnw spring-boot:run -Dspring-boot.run.profiles=clean-start
```

The `clean-start` profile drops `bucket_word`, `word_bucket`, `consumer_checkpoint`, and `search_post`, then recreates them using `schema.sql` before ingestion starts. This deletes all stored word counts, captured search posts, and checkpoint progress; Jetstream starts at the live tip. Use this command each time you want to reset the application tables.

In another terminal:

```sh
cd src/main/frontend
pnpm install
pnpm dev
```

Open Vite's printed URL without `?demo=1`. With **Every batch**, expect a snapshot roughly every five seconds during continuous live traffic. Publication is driven by stream progress, so gaps and catch-up replay can change the wall-clock cadence. Timed **Refresh** settings limit server delivery to at most once every 5, 10, 30, or 60 seconds; **Snapshot** controls how much history each result contains.

```sh
curl -N http://localhost:8080/api/word-buckets
curl -N 'http://localhost:8080/api/word-buckets?windowSeconds=3600&refreshIntervalMs=30000'
./mvnw test
```

Tests use temporary PostgreSQL 17 containers, not the Compose database, and disable the real Jetstream connection. Database tests skip when Docker is unavailable. GitHub Actions (`.github/workflows/ci.yml`) runs the Java tests and the frontend build for pull requests to `main` and pushes to `main`; both must pass before a pull request can merge.

## Accumulation and flushing

- Parse `witnessedAt` as a UTC instant. Bucket starts are epoch multiples of five seconds: an event at `12:00:07` belongs to `[12:00:05, 12:00:10)`. `time` and `createdAt` do not determine membership.
- Retain the existing English/create and URL filtering rules. Stop words are loaded once from `src/main/resources/stopwords/english.txt`, containing the [CountWordsFree English list](https://countwordsfree.com/stopwords) and standalone digits `0`–`9`. Multi-digit numbers remain eligible. Words retain apostrophes between letters; curly apostrophes normalize to straight apostrophes before stop-word matching, so contractions such as `don't` are filtered as whole words. Surrounding quotes and other punctuation are separators. Only words using ASCII letters and numeric tokens using ASCII digits are counted. All accepted commits advance sequence progress, including those without usable words.
- Skip events at or below the loaded checkpoint. Within a session, also skip events at or below the greatest already accepted sequence, preventing duplicates in the pending batch. This assumes ordered delivery from the same Jetstream instance.
- Accumulate word-count increments by bucket start. Older buckets can always receive contributions from new sequences; there is no closing threshold or lateness allowance.
- Once an event's bucket start exceeds the greatest bucket start already observed, flush all pending increments. Include the triggering event's increments and sequence in that flush. The first event establishes the initial maximum; it does not flush by itself.
- No local timer or completion hook flushes pending contributions. On disconnect they are discarded in memory and reconstructed from replay after the durable checkpoint.

For example, events 100 and 101 at `:01` and `:02`, followed by event 102 at `:05`, produce a batch containing the increments for both `:00` and `:05`, with sequence 102. A later event 103 at `:03` adds to the old `:00` bucket and is saved at the next forward boundary.

## Database and checkpoints

`word_bucket` uses `started_at` as its primary key. The end is always start plus five seconds; no run ID or end column is stored. `bucket_word` stores additive counts keyed by bucket start and word.

The singleton `consumer_checkpoint` stores the latest persisted sequence and greatest observed bucket start. `WordBucketRepository.saveBatch` locks this row and writes all bucket increments and checkpoint progress in one transaction. When search is enabled, the same transaction saves the batch's captured posts and deletes to `search_post`. A repeated batch already covered by the checkpoint does nothing. A batch whose preceding sequence differs from the checkpoint is rejected rather than risking duplicate contributions.

Persistence is sequential with no prefetch. Transient database errors get up to three retries with backoff. The repository returns its result only after the transaction commits.

After persistence commits, `WordSnapshotService` notifies a separate cache worker. `WordBucketRepository.snapshotCounts` reads the preceding hour in one SQL statement and uses `SUM(count) FILTER (WHERE ...)` to calculate the shorter windows. Each range is `[maxBucketStart - duration, maxBucketStart)`, excluding the partially observed current bucket even though its initial contributions are already saved. Gaps without rows naturally contribute zero. Corrections to older buckets appear in refreshed windows that still contain them. Less available history produces a partial window; empty windows are empty maps. Each window keeps only its 100 most frequent words, the most the browser draws, breaking ties at the cutoff alphabetically as the browser does.

The worker publishes the immutable word maps for every window together only after the whole query succeeds. While it runs, visitors use the previous cached set. Transient query failures receive up to three retries; failed refreshes are logged and preserve the previous cache while ingestion continues. The next committed batch triggers another attempt. Query work is sequential and keeps recent pending progress when it falls behind, so it does not build an unbounded backlog during catch-up. The cache starts empty on each application startup and is populated after a committed batch; existing database history is not loaded into it during startup.

## Reconnects

`JetstreamConsumer` always uses the same configured Jetstream host. On error or normal upstream disconnect it reloads the checkpoint and reconnects with `cursor=latestSeq`. The raw cursor is inclusive, so that event is received again and skipped. Each session gets a fresh accumulator initialized from durable progress.

The WebSocket client accepts frame payloads up to 1 MiB, allowing Jetstream events larger than Reactor Netty's default 64 KiB limit.

A new database has sequence zero and starts at the live tip. Resume depends on the server retaining the requested history; archive backfill for cursors outside the WebSocket lookback is not implemented. Do not change upstream instances or run multiple ingestion processes against the same checkpoint. There is one checkpoint for the current fixed filters and aggregation rules; changing those rules does not rebuild older counts. Retention cleanup is not implemented.

The latest complete cache set stays in a shared replay sink. SSE connections and their timers read it without database queries. Browsers never create an upstream Jetstream connection, and cache refreshes continue without subscribers. `GET /api/word-buckets` accepts `windowSeconds` (`30`, `60`, `300`, `600`, `1800`, `3600`; default `30`) and `refreshIntervalMs` (`0`, `5000`, `10000`, `30000`, `60000`; default `0`). Invalid values return HTTP 400. Zero means delivery on each cache refresh; positive intervals send at most one result per interval, and only when the cache has refreshed since the previous delivery. A slow connection keeps only the newest pending result.

The first available cached snapshot is sent immediately on each connection, including after changing either setting. A connection made before the first successful cache refresh waits for that result. Subsequent timed deliveries begin one selected interval later. SSE data remains a JSON word/count map, e.g. `{"hello":12,"world":7}`. Counts are 64-bit sums. The upstream connection, cache worker, and downstream streams/timers stop on application shutdown.

Spring Boot initializes `schema.sql` before the repository is used. Repeated startups preserve existing counts and checkpoint progress.

## Semantic search (in progress)

Semantic search over the last 24 hours of posts is being built behind `skystream.search.enabled`, which is off by default. It embeds text with [embeddinggemma-300m](https://huggingface.co/onnx-community/embeddinggemma-300m-ONNX) through ONNX Runtime, in-process. Fetch the model (about 1.2 GB) once:

```sh
scripts/fetch-model.sh
```

The script writes the model's files, pinned to one revision, to `models/embeddinggemma-300m` (gitignored). Set `skystream.search.model-dir` if they live elsewhere. The embedder's tests check its vectors against the ones recorded in `src/test/resources/embedding-fixture.json`, and are skipped when the model hasn't been fetched. The model is released under the [Gemma Terms of Use](https://ai.google.dev/gemma/terms).

With search enabled, ingestion also saves English top-level posts with at least five words, once links are removed, and no longer than Bluesky allows (3,000 characters), to `search_post`, and posts older than 24 hours are deleted every 10 minutes. A background worker embeds waiting posts newest first, up to 256 at a time on a thread of its own, so in a burst fresh posts become searchable first and older ones catch up later or expire unembedded. If a batch fails, its posts are embedded one at a time, and any that fail on their own are marked `embedding_failed` and skipped from then on, so one bad post can't stall the rest. It logs the backlog every minute, as in `Embedding backlog: 12 posts, oldest seen 4s ago`, which measures how far embedding is behind ingestion.

Search calls Claude, so with search enabled the app needs an [Anthropic API key](https://console.anthropic.com/) in the `ANTHROPIC_API_KEY` environment variable, and doesn't start without one. The key is never logged. For example, with the key in a `.env` file (gitignored):

```sh
set -a; . ./.env; set +a
./mvnw spring-boot:run -Dspring-boot.run.arguments=--skystream.search.enabled=true
```

`GET /api/search?q=` first asks Claude Haiku (`claude-haiku-5-5`, thinking off) for three short hypothetical posts that would answer the query. It embeds the query and those posts, and returns the 10 embedded posts nearest their normalized mean by cosine similarity, most similar first. The posts let a query find matches by what they're about rather than the words they share: "washington state hikes" finds a post about Sahale Arm. Expansions are cached by query, ignoring case and spacing, for a day. If Haiku fails or takes longer than 5 seconds, the search uses the query alone. Results are a JSON array:

```json
[{"text": "…", "seenAt": "2026-10-18T19:04:11.52Z", "url": "https://bsky.app/profile/did:plc:…/post/3m…", "similarity": 0.61}]
```

`seenAt` is when Jetstream saw the post, and `url` its page on bsky.app. The query is trimmed and must be 1 to 200 characters, or the request returns HTTP 400. The endpoints exist only with search enabled.

`GET /api/search/stream?q=` does the same search, then has Haiku answer the query from the results, as server-sent events:
- `results`: the results, as above.
- `answer`: the answer's next piece of text, as `{"text": "…"}`, repeated as Haiku writes it. The answer is 2 to 4 sentences citing results by number, as in `[2]`, and says so when no result is relevant.
- `done` (`{}`) ends the stream, or `failed` (`{}`) if no answer could be written. With no results, there's no answer.

The posts are strangers' words, so the prompt passes them as data inside numbered `<post>` tags, escaped so a post can't close its tag, and tells Haiku never to follow instructions in them. A complete answer is cached by query and the list of results it was written from, so a repeated search whose results haven't changed reuses it. An answer ends with `failed` if Claude returns an error, refuses, stops short of a complete answer, or goes 10 seconds without new text. The frontend's search page, `search.html`, uses this endpoint.

PostgreSQL runs on the `pgvector/pgvector:pg17` image, which the Compose file and tests use. Switching an existing Compose volume from the earlier `postgres:17-alpine` image can cause collation-version warnings, so recreate the tables with the `clean-start` profile after switching.

## External database

Supply these Spring properties through configuration or environment variables:

```properties
spring.docker.compose.enabled=false
spring.r2dbc.url=r2dbc:postgresql://your-host:5432/your-database
spring.r2dbc.username=your-user
spring.r2dbc.password=your-password
```

The database needs the [pgvector](https://github.com/pgvector/pgvector) extension, version 0.7 or later, even with search disabled, because `schema.sql` creates the search table. Creating the extension needs a superuser, so run `CREATE EXTENSION vector;` once as one if the application's user isn't.

Set `skystream.jetstream.enabled=false` to run HTTP/database functionality without upstream ingestion. Frontend setup is described in [its README](src/main/frontend/README.md).
