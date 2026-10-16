CREATE TABLE IF NOT EXISTS word_bucket (
    started_at TIMESTAMPTZ CONSTRAINT word_bucket_started_at_pk PRIMARY KEY,
    CHECK (EXTRACT(EPOCH FROM started_at) % 5 = 0)
);

CREATE TABLE IF NOT EXISTS bucket_word (
    bucket_started_at TIMESTAMPTZ NOT NULL REFERENCES word_bucket(started_at) ON DELETE CASCADE,
    word TEXT NOT NULL CHECK (length(trim(word)) > 0),
    count BIGINT NOT NULL CHECK (count > 0),
    CONSTRAINT bucket_word_started_at_word_pk PRIMARY KEY (bucket_started_at, word)
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
    embedding HALFVEC(768)
);

-- Rows without an embedding are skipped by the HNSW index and found through the partial index.
CREATE INDEX IF NOT EXISTS search_post_embedding_hnsw ON search_post USING hnsw (embedding halfvec_cosine_ops);
CREATE INDEX IF NOT EXISTS search_post_unembedded ON search_post (seen_at DESC) WHERE embedding IS NULL;
-- Expiry deletes by age across all rows.
CREATE INDEX IF NOT EXISTS search_post_seen_at ON search_post (seen_at);
