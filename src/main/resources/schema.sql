CREATE TABLE IF NOT EXISTS word_bucket (
    started_at TIMESTAMPTZ CONSTRAINT word_bucket_started_at_pk PRIMARY KEY,
    CHECK (EXTRACT(EPOCH FROM started_at) % 5 = 0)
);

-- Counts are INTEGER, since a word's count in one bucket is far below 2^31. Summing integers keeps a plain 64-bit total
-- per word and window; summing bigints allocates a 128-bit one, which made the 24-hour query 3.1 s rather than 1.9 s.
CREATE TABLE IF NOT EXISTS bucket_word (
    bucket_started_at TIMESTAMPTZ NOT NULL REFERENCES word_bucket(started_at) ON DELETE CASCADE,
    word TEXT NOT NULL CHECK (length(trim(word)) > 0),
    count INTEGER NOT NULL CHECK (count > 0),
    CONSTRAINT bucket_word_started_at_word_pk PRIMARY KEY (bucket_started_at, word)
);

-- The same counts per minute, for the 30-minute and 1-hour windows.
CREATE TABLE IF NOT EXISTS word_bucket_1m (
    started_at TIMESTAMPTZ CONSTRAINT word_bucket_1m_started_at_pk PRIMARY KEY,
    CHECK (EXTRACT(EPOCH FROM started_at) % 60 = 0)
);

CREATE TABLE IF NOT EXISTS bucket_word_1m (
    bucket_started_at TIMESTAMPTZ NOT NULL REFERENCES word_bucket_1m(started_at) ON DELETE CASCADE,
    word TEXT NOT NULL CHECK (length(trim(word)) > 0),
    count INTEGER NOT NULL CHECK (count > 0),
    CONSTRAINT bucket_word_1m_started_at_word_pk PRIMARY KEY (bucket_started_at, word)
);

-- The same counts per 5 minutes, for windows longer than an hour.
CREATE TABLE IF NOT EXISTS word_bucket_5m (
    started_at TIMESTAMPTZ CONSTRAINT word_bucket_5m_started_at_pk PRIMARY KEY,
    CHECK (EXTRACT(EPOCH FROM started_at) % 300 = 0)
);

CREATE TABLE IF NOT EXISTS bucket_word_5m (
    bucket_started_at TIMESTAMPTZ NOT NULL REFERENCES word_bucket_5m(started_at) ON DELETE CASCADE,
    word TEXT NOT NULL CHECK (length(trim(word)) > 0),
    count INTEGER NOT NULL CHECK (count > 0),
    CONSTRAINT bucket_word_5m_started_at_word_pk PRIMARY KEY (bucket_started_at, word)
);

CREATE TABLE IF NOT EXISTS consumer_checkpoint (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    latest_seq BIGINT NOT NULL CHECK (latest_seq >= 0),
    max_bucket_started_at TIMESTAMPTZ
);

INSERT INTO consumer_checkpoint (id, latest_seq)
VALUES (1, 0) ON CONFLICT (id) DO NOTHING;

-- Semantic search. Posts are captured with each ingestion batch without an embedding; a worker fills it in later.
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS search_post (
    uri TEXT CONSTRAINT search_post_uri_pk PRIMARY KEY,
    text TEXT NOT NULL,
    -- Jetstream's time for the event, not the author-supplied createdAt.
    seen_at TIMESTAMPTZ NOT NULL,
    embedding HALFVEC(768),
    -- Set when the post fails to embed on its own, so it's never fetched again and can't block the posts behind it.
    embedding_failed BOOLEAN NOT NULL DEFAULT false
);

-- Rows without an embedding are skipped by the HNSW index. Those still waiting for one are found through the partial
-- index.
CREATE INDEX IF NOT EXISTS search_post_embedding_hnsw ON search_post USING hnsw (embedding halfvec_cosine_ops);
CREATE INDEX IF NOT EXISTS search_post_unembedded ON search_post (seen_at DESC)
    WHERE embedding IS NULL AND NOT embedding_failed;
-- Expiry deletes by age across all rows.
CREATE INDEX IF NOT EXISTS search_post_seen_at ON search_post (seen_at);
