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
