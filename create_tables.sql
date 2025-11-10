CREATE TABLE IF NOT EXISTS steamspy (
    appid INT PRIMARY KEY,
    name TEXT NOT NULL,
    publisher TEXT,
    score_rank INT,
    owners TEXT,
    average_forever INT,
    average_2weeks INT,
    median_forever INT,    
    median_2weeks INT,   
    ccu INT, 
    price INT,
    initialprice INT,
    discount INT,
    tags JSONB,
    languages TEXT[],
    genre TEXT[],
    ingested_at TIMESTAMPTZ DEFAULT NOW()
)
