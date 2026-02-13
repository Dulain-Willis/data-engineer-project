CREATE TABLE IF NOT EXISTS analytics.steamspy_silver
(
    appid Int32,
    name String,
    developer String,
    publisher String,
    score_rank String,
    positive Int32,
    negative Int32,
    userscore Int32,
    owners String,
    average_forever Int32,
    average_2weeks Int32,
    median_forever Int32,
    median_2weeks Int32,
    ccu Int32,
    price String,
    initialprice String,
    discount String,
    run_id String,
    ingestion_timestamp DateTime64(3),
    dt String
)
ENGINE = ReplacingMergeTree(ingestion_timestamp)
PARTITION BY dt
ORDER BY (appid, dt)
SETTINGS index_granularity = 8192;
