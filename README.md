# Skystream

Jetstream post commits become canonical five-second word-count buckets in PostgreSQL. The accumulator flushes when an event enters a newer bucket. After every committed flush, Java sends each SSE subscriber combined counts for their selected duration: 30 seconds (default), 1 minute, 5 minutes, 10 minutes, 30 minutes, or 1 hour. Each browser independently saves its snapshot duration and refresh preference.

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

The `clean-start` profile drops `bucket_word`, `word_bucket`, and `consumer_checkpoint`, then recreates them using `schema.sql` before ingestion starts. This deletes all stored word counts and checkpoint progress; Jetstream starts at the live tip. Use this command each time you want to reset the application tables.

In another terminal:

```sh
cd src/main/frontend
pnpm install
pnpm dev
```

Open Vite's printed URL without `?demo=1`. During continuous live traffic, expect a snapshot roughly every five seconds, showing the preceding selected duration. Publication is driven by stream progress, so gaps and catch-up replay can change the wall-clock cadence. The **Refresh** selector controls how often the browser displays received snapshots; **Snapshot** controls how much history they contain.

```sh
curl -N http://localhost:8080/api/word-buckets
curl -N 'http://localhost:8080/api/word-buckets?windowSeconds=3600'
./mvnw test
```

Tests use temporary PostgreSQL 17 containers, not the Compose database, and disable the real Jetstream connection. Database tests skip when Docker is unavailable.

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

The singleton `consumer_checkpoint` stores the latest persisted sequence, greatest observed bucket start, and successful flush count. `WordBucketRepository.saveBatch` locks this row and writes all bucket increments and checkpoint progress in one transaction. A repeated batch already covered by the checkpoint does nothing. A batch whose preceding sequence differs from the checkpoint is rejected rather than risking duplicate contributions.

Persistence is sequential with no prefetch. Transient database errors get up to three retries with backoff. The repository returns its result only after the transaction commits.

`WordSnapshotService` publishes committed checkpoint progress after every successful flush. Each SSE subscription queries `[maxBucketStart - selected duration, maxBucketStart)`, excluding the partially observed current bucket even though its initial contributions are already saved. Gaps without rows naturally contribute zero; they do not create additional flushes. Corrections to older buckets appear in the next snapshot if their timestamps remain within that snapshot's range; already published snapshots are not individually revised. If the database contains less history than requested, the snapshot includes the available buckets without waiting for a full window.

## Reconnects

`JetstreamConsumer` always uses the same configured Jetstream host. On error or normal upstream disconnect it reloads the checkpoint and reconnects with `cursor=latestSeq`. The raw cursor is inclusive, so that event is received again and skipped. Each session gets a fresh accumulator initialized from durable progress.

The WebSocket client accepts frame payloads up to 1 MiB, allowing Jetstream events larger than Reactor Netty's default 64 KiB limit.

A new database has sequence zero and starts at the live tip. Resume depends on the server retaining the requested history; archive backfill for cursors outside the WebSocket lookback is not implemented. Do not change upstream instances or run multiple ingestion processes against the same checkpoint. There is one checkpoint for the current fixed filters and aggregation rules; changing those rules does not rebuild older counts. Retention cleanup is not implemented.

The latest committed checkpoint stays in a shared replay sink for browser subscribers. Each subscriber queries counts for its own window; browsers never create an upstream Jetstream connection, and ingestion continues without subscribers. If a subscriber's query falls behind, it keeps only the newest pending checkpoint instead of queuing outdated windows. Snapshot query failures affect that subscriber's stream rather than ingestion. `GET /api/word-buckets?windowSeconds=30` accepts exactly `30`, `60`, `300`, `600`, `1800`, or `3600`; omitting the parameter defaults to `30`, and invalid values return HTTP 400. Changing the selection reconnects SSE and immediately queries the latest published checkpoint. Before the first flush after application startup, subscribers wait for committed progress. SSE data remains a JSON word/count map, e.g. `{"hello":12,"world":7}`. Counts are 64-bit sums. The upstream connection and downstream streams are stopped on application shutdown.

Spring Boot initializes `schema.sql` before the repository is used. Repeated startups preserve existing counts and checkpoint progress.

## External database

Supply these Spring properties through configuration or environment variables:

```properties
spring.docker.compose.enabled=false
spring.r2dbc.url=r2dbc:postgresql://your-host:5432/your-database
spring.r2dbc.username=your-user
spring.r2dbc.password=your-password
```

Set `skystream.jetstream.enabled=false` to run HTTP/database functionality without upstream ingestion. Frontend setup is described in [its README](src/main/frontend/README.md).
