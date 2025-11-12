CREATE TABLE IF NOT EXISTS steamspy (
    appid   INT PRIMARY KEY,
    payload    JSONB NOT NULL,
    ingested_at    TIMESTAMPTZ DEFAULT NOW()
);
