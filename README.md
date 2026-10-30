# Skystream

Jetstream post commits become canonical five-second word-count buckets in PostgreSQL. The accumulator flushes when an event enters a newer bucket. The same counts are also kept per minute and per five minutes. After committed flushes, filtered-aggregate queries refresh a shared in-memory cache of nine durations: 30 seconds (default), 1 minute, 5 minutes, and 10 minutes from the five-second buckets, 30 minutes and 1 hour from the one-minute ones, and 3, 8, and 24 hours from the five-minute ones. SSE delivers the selected cached window at each visitor's refresh rate. Each browser independently saves both preferences.

## Run locally

Requires Java 21, Docker with Compose, and the frontend's pnpm setup.

From the project root:

```sh
./mvnw spring-boot:run
```

Spring Boot starts PostgreSQL from `compose.yaml`, on a random port only this machine can reach, discovers that port and the credentials, initializes the bucket schema, and connects to Jetstream on startup. The Compose volume retains data. The HTTP server defaults to port 8080.

For a clean start, stop any running instance of the app and run:

```sh
./mvnw spring-boot:run -Dspring-boot.run.profiles=clean-start
```

The `clean-start` profile drops the word count tables (`word_bucket`, `bucket_word`, and their `_1m` and `_5m` versions), `consumer_checkpoint`, and `search_post`, then recreates them using `schema.sql` before ingestion starts. This deletes all stored word counts, captured search posts, and checkpoint progress; Jetstream starts at the live tip. Use this command each time you want to reset the application tables.

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
- Retain the existing English/create and URL filtering rules. Stop words are loaded once from `src/main/resources/stopwords/english.txt`, containing the [CountWordsFree English list](https://countwordsfree.com/stopwords) and standalone digits `0`–`9`. Multi-digit numbers remain eligible. Words retain apostrophes between letters; curly apostrophes normalize to straight apostrophes before stop-word matching, so contractions such as `don't` are filtered as whole words. Surrounding quotes and other punctuation are separators. Only words using ASCII letters and numeric tokens using ASCII digits are counted. Posts whose `createdAt` is more than an hour before `witnessedAt` are skipped for both words and search: these are mostly an account's whole history sent again at once when it moves to another PDS, which can be thousands of posts in a second, or bots backdating what they post. A `createdAt` that isn't a valid timestamp doesn't skip the post. Posts their authors labeled `porn`, `sexual`, `nudity` or `graphic-media`, which Bluesky hides or puts behind a warning by default, are skipped the same way, since the pages are public. Labels applied by Bluesky's moderation service aren't in the post, so they aren't seen. All accepted commits advance sequence progress, including those without usable words.
- Skip events at or below the loaded checkpoint. Within a session, also skip events at or below the greatest already accepted sequence, preventing duplicates in the pending batch. This assumes ordered delivery from the same Jetstream instance.
- Accumulate word-count increments by bucket start. Older buckets can always receive contributions from new sequences; there is no closing threshold or lateness allowance.
- Once an event's bucket start exceeds the greatest bucket start already observed, flush all pending increments. Include the triggering event's increments and sequence in that flush. The first event establishes the initial maximum; it does not flush by itself.
- No local timer or completion hook flushes pending contributions. On disconnect they are discarded in memory and reconstructed from replay after the durable checkpoint.

For example, events 100 and 101 at `:01` and `:02`, followed by event 102 at `:05`, produce a batch containing the increments for both `:00` and `:05`, with sequence 102. A later event 103 at `:03` adds to the old `:00` bucket and is saved at the next forward boundary.

## Database and checkpoints

`word_bucket` uses `started_at` as its primary key. The end is always start plus five seconds; no run ID or end column is stored. `bucket_word` stores additive counts keyed by bucket start and word. `word_bucket_1m` and `bucket_word_1m` hold the same counts per minute, and `word_bucket_5m` and `bucket_word_5m` per five minutes. At daytime traffic, an hour then sums about 450,000 rows rather than 720,000, and 24 hours about 5 million rather than 14 million; the five-second tables keep 10 minutes.

The singleton `consumer_checkpoint` stores the latest persisted sequence and greatest observed bucket start. `WordBucketRepository.saveBatch` locks this row and writes all bucket increments, to every bucket size, and checkpoint progress in one transaction. When search is enabled, the same transaction saves the batch's captured posts and deletes to `search_post`. A repeated batch already covered by the checkpoint does nothing. A batch whose preceding sequence differs from the checkpoint is rejected rather than risking duplicate contributions.

Persistence is sequential with no prefetch. Transient database errors get up to three retries with backoff. The repository returns its result only after the transaction commits.

After persistence commits, `WordSnapshotService` notifies a cache worker for each bucket size. `WordBucketRepository.snapshots` reads a bucket size's longest window in one SQL statement and uses `SUM(count) FILTER (WHERE ...)` to calculate the shorter ones. Each range ends at the start of the bucket holding `maxBucketStart`, excluding that partially observed bucket even though its initial contributions are already saved: `[maxBucketStart - duration, maxBucketStart)` for windows up to 10 minutes, and the same rounded down to the minute or to five minutes for longer ones. So the five-second worker refreshes after every batch, and the others when a new minute or five-minute bucket starts; a slow 24-hour query doesn't hold up the short windows, and the hour isn't summed again every 5 seconds. Gaps without rows naturally contribute zero. Corrections to older buckets appear in refreshed windows that still contain them. Each window keeps only its 100 most frequent words, the most the browser draws, selected in the database and breaking ties at the cutoff alphabetically as the browser does. It also reports how many of its seconds have buckets, which is less than the window until the app has run that long, or after downtime. A one-minute or five-minute bucket counts in full even if the app collected only part of it, so longer windows can overstate this by up to a minute or five minutes.

Each worker publishes the immutable snapshots for its windows together only after the whole query succeeds. While it runs, visitors use the previous cached set. Transient query failures receive up to three retries; failed refreshes are logged and preserve the previous cache while ingestion continues. The next committed batch triggers another attempt. Each worker's queries are sequential and keep recent pending progress when they fall behind, so it does not build an unbounded backlog during catch-up. The cache starts empty on each application startup and is populated after a committed batch; existing database history is not loaded into it during startup.

Word counts are stored as `integer`. Databases from before this change have `bigint` counts, which `schema.sql` doesn't change, since it only creates missing tables. The app works with either, but sums `bigint` more slowly. To convert, stop the app first: while it runs, its cached snapshot queries would fail with `cached plan must not change result type` until it restarts. Then run these statements with psql, here for the Compose database, or reset with the `clean-start` profile:

```bash
docker exec skystream_postgres_local psql -U local_user -d skystream_db -c '
ALTER TABLE IF EXISTS bucket_word ALTER COLUMN count TYPE integer;
ALTER TABLE IF EXISTS bucket_word_1m ALTER COLUMN count TYPE integer;
ALTER TABLE IF EXISTS bucket_word_5m ALTER COLUMN count TYPE integer;'
```

Each table is rewritten under a lock. On a 4-core VPS, 3.6 million five-minute rows took 10 seconds, and the other two tables under 2 seconds together.

## Reconnects

`JetstreamConsumer` always uses the same configured Jetstream host. On error, normal upstream disconnect, or 30 seconds without a frame it reloads the checkpoint and reconnects with `cursor=latestSeq`. Silence that long means the connection died without closing; the time also runs while a batch saves, so a save that slow reconnects too, and the replay saves it again. The raw cursor is inclusive, so that event is received again and skipped. Each session gets a fresh accumulator initialized from durable progress.

The WebSocket client accepts frame payloads up to 1 MiB, allowing Jetstream events larger than Reactor Netty's default 64 KiB limit.

A new database has sequence zero and starts at the live tip. Resume depends on the server retaining the requested history; archive backfill for cursors outside the WebSocket lookback is not implemented. Do not change upstream instances or run multiple ingestion processes against the same checkpoint. There is one checkpoint for the current fixed filters and aggregation rules; changing those rules does not rebuild older counts.

The latest complete cache set stays in a shared replay sink. SSE connections and their timers read it without database queries. Browsers never create an upstream Jetstream connection, and cache refreshes continue without subscribers. `GET /api/word-buckets` accepts `windowSeconds` (`30`, `60`, `300`, `600`, `1800`, `3600`, `10800`, `28800`, `86400`; default `30`) and `refreshIntervalMs` (`0`, `5000`, `10000`, `30000`, `60000`; default `0`). Invalid values return HTTP 400. Zero means delivery on each cache refresh; positive intervals send at most one result per interval, and only when the cache has refreshed since the previous delivery. A slow connection keeps only the newest pending result.

The first available cached snapshot is sent immediately on each connection, including after changing either setting. A connection made before the first successful cache refresh waits for that result. Subsequent timed deliveries begin one selected interval later. SSE data is the window's word/count map and how many of its seconds have been collected, e.g. `{"words":{"hello":12,"world":7},"collectedSeconds":30}`. Counts are 64-bit sums. The upstream connection, cache workers, expiry schedule, and downstream streams/timers stop on application shutdown.

Spring Boot initializes `schema.sql` before the repository is used. Repeated startups preserve existing counts and checkpoint progress.

## Expiry

While ingestion is enabled (the default), `ExpiryService` deletes what the app no longer shows every 10 minutes: five-second buckets older than 10 minutes, one-minute buckets older than an hour, and five-minute buckets older than 24 hours, with their words, and search posts older than 24 hours. Word buckets are measured from the newest bucket, as the windows are, so catching up after downtime keeps what the windows show; search posts are measured from the clock. Buckets saved before the one-minute or five-minute tables existed aren't copied into them, so after upgrading, the windows they serve fill as the app runs.

## Semantic search (in progress)

Semantic search over the last 24 hours of posts is being built behind `skystream.search.enabled`, which is off by default. It embeds text with [embeddinggemma-300m](https://huggingface.co/onnx-community/embeddinggemma-300m-ONNX) through ONNX Runtime, in-process. Fetch the model (about 1.2 GB) once:

```sh
scripts/fetch-model.sh
```

The script writes the model's files, pinned to one revision, to `models/embeddinggemma-300m` (gitignored). Set `skystream.search.model-dir` if they live elsewhere. The embedder's tests check its vectors against the ones recorded in `src/test/resources/embedding-fixture.json`, and are skipped when the model hasn't been fetched. The model is released under the [Gemma Terms of Use](https://ai.google.dev/gemma/terms).

With search enabled, ingestion also saves English top-level posts, written no more than an hour before Jetstream saw them and not labeled as adult content (see Accumulation and flushing), with at least five words, once links are removed, and no longer than Bluesky allows (3,000 characters), to `search_post`, and posts older than 24 hours are deleted every 10 minutes (see Expiry). A background worker embeds waiting posts newest first, up to 256 at a time on a thread of its own, so in a burst fresh posts become searchable first and older ones catch up later or expire unembedded. If a batch fails, its posts are embedded one at a time, and any that fail on their own are marked `embedding_failed` and skipped from then on, so one bad post can't stall the rest. It reads the backlog at startup and every minute, which measures how far embedding is behind ingestion, logging it as in `Embedding backlog: 12 posts, oldest seen 4s ago` and reporting it as metrics (see Metrics).

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

## Metrics

The app serves Prometheus metrics at `http://127.0.0.1:8081/actuator/prometheus`. Actuator runs on its own port, which listens only on the local machine, and exposes nothing but `health` and `prometheus`. Spring Boot's built-in metrics cover the JVM, CPU, HTTP requests (`http_server_requests_seconds`, which for `/api/search/stream` times the whole stream, answer included) and the database connection pool (`r2dbc_pool_*`). The app adds:

| Metric | What it measures |
|---|---|
| `skystream_ingestion_lag_seconds` | How far behind now the newest saved 5-second bucket starts: up to 5 seconds plus Jetstream's own delay while keeping up, more while catching up. Before the first save, the time since startup. |
| `skystream_ingestion_save_seconds` | Saving each batch, per attempt. |
| `skystream_snapshots_refresh_seconds{tier}` | Refreshing each tier's word cloud windows, per attempt. `tier` is `five_seconds`, `one_minute` or `five_minutes`. |
| `skystream_expiry_seconds{data}`, `skystream_expiry_deleted_rows_total{data}` | Each expiry run, and the rows it deleted: `search_posts`, or `word_buckets`, counting bucket rows, whose words go with them. |
| `skystream_search_backlog_posts`, `skystream_search_backlog_age_seconds` | Posts waiting for an embedding, and how long before the reading the oldest was seen. Read at startup and every minute. |
| `skystream_search_embeddings_total{outcome}` | Posts `embedded`, or `failed` and skipped. Its rate is the embedding speed. |
| `skystream_search_query_seconds` | Searches, from the query to the results. |
| `skystream_search_expansion_seconds` | Haiku's query expansions, timed to the end even when the search stopped waiting after 5 seconds. Cached expansions aren't timed. |
| `skystream_search_answer_seconds` | Haiku's answers, from the call to the last text sent. Cached answers aren't timed. |

Timers have an `outcome` tag: `success`, `error` or `cancel`. The search timers also have buckets at 0.25 to 20 seconds (`_bucket`), so percentiles can be graphed. Tags take only values from a fixed set, never search text or post URIs, so the app produces about 170 series, built-in ones included.

### Sending metrics to Grafana Cloud

The Compose file's `monitoring` profile runs [Grafana Alloy](https://grafana.com/docs/alloy/), which scrapes once a minute and sends the metrics to Grafana Cloud:
- the app,
- the app's and PostgreSQL's processes: memory, including the embedding model's outside the JVM, and CPU,
- the host: CPU, memory, disk and network,
- PostgreSQL, through `postgres-exporter`: table and index sizes, dead rows and autovacuum runs.

The free tier allows 10,000 active series and keeps 14 days. This setup sends about 1,000–2,000: about 170 from the app, 480 from PostgreSQL, 55 from the processes, and the rest from the host, which grows with its CPUs and disks. Alloy uses the host's network to reach the app's local-only port, so it needs Docker Engine with the Compose plugin on Linux (WSL included), not Docker Desktop. Its status page is at `http://127.0.0.1:12345`, on that machine only.

1. Create a free Grafana Cloud stack. On its Prometheus details page, find the remote write URL and the username, and create an access policy token with the `metrics:write` scope. Add them to `.env`, and on a server make it readable only by you (`chmod 600 .env`):

   ```sh
   GRAFANA_CLOUD_PROMETHEUS_URL=https://…/api/prom/push
   GRAFANA_CLOUD_PROMETHEUS_USER=123456
   GRAFANA_CLOUD_TOKEN=glc_…
   ```

2. Start monitoring, which also starts the database, then start the app separately:

   ```sh
   docker compose --profile monitoring up -d
   ./mvnw spring-boot:run
   ```

   Compose runs the database and monitoring, not the app, and starting the app doesn't start monitoring. Start monitoring first: when the app starts the database itself, stopping the app stops all of Compose's services, monitoring included.

3. Create the dashboard and alerts, from any machine with curl 7.76 or later and jq. In the stack's Grafana, create a service account with the Admin role and a token for it, with a short expiry. The script reads its settings from the environment, not `.env`:

   ```sh
   export GRAFANA_URL=https://your-stack.grafana.net
   export GRAFANA_SERVICE_ACCOUNT_TOKEN=glsa_…
   export ALERT_HOST=your-server-hostname
   monitoring/publish.sh
   ```

   It creates or updates, in a Skystream folder, the dashboard from `monitoring/dashboard.json` and two alerts from `monitoring/alert-rules.json`.
   - **What the alerts watch:** only `ALERT_HOST`, the host whose app they watch, as listed under Host on the dashboard. That way a laptop still reporting can't hide the server going quiet.
   - **Metrics stopped** fires 5 minutes after the app stops answering scrapes, or about 10 after metrics stop arriving at all because Alloy or the server stopped. A series counts as gone only after 5 minutes without data.
   - **Ingestion behind** fires about 10 minutes after ingestion stops while the app still answers scrapes, or when it's still more than 5 minutes behind 5 minutes into catching up after downtime.
   - **Who it notifies:** whatever the default notification policy names. A new stack's may name an empty contact point, which notifies no one. Add a contact point you read (Alerting → Contact points) and set it on the default policy (Alerting → Notification policies).

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
