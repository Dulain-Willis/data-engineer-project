{{
    config(
        materialized='view'
    )
}}

-- Hybrid ranking: Wilson score as a quality gate, raw positive volume as the rank.
-- Step 1: compute Wilson scores for all games (same as gold_top_games_wilson).
-- Step 2: keep only the top 500 by Wilson score (the "credibility pool").
-- Step 3: within that pool, rank by raw positive review count.
--
-- Result: games that are both genuinely well-received AND widely played.
-- Tweak wilson_pool_size to widen/narrow the quality gate.
WITH review_stats AS (
    SELECT
        appid,
        game_title,
        positive,
        negative,
        positive + negative AS total_reviews,
        if(positive + negative > 0, positive / (positive + negative), 0) AS approval_rate
    FROM {{ source('analytics', 'steamspy_silver_stg_games') }}
    WHERE positive + negative > 0
),

wilson AS (
    SELECT
        appid,
        game_title,
        positive,
        negative,
        total_reviews,
        round(approval_rate * 100, 1) AS approval_pct,
        round(
            (
                approval_rate
                + (1.96 * 1.96) / (2 * total_reviews)
                - 1.96 * sqrt(
                    (approval_rate * (1 - approval_rate) + (1.96 * 1.96) / (4 * total_reviews))
                    / total_reviews
                )
            )
            / (1 + (1.96 * 1.96) / total_reviews),
            4
        ) AS wilson_score
    FROM review_stats
),

-- Quality gate: top 500 games by Wilson score
wilson_pool AS (
    SELECT *
    FROM wilson
    ORDER BY wilson_score DESC
    LIMIT 500
)

SELECT
    row_number() OVER (ORDER BY positive DESC) AS rank,
    game_title,
    total_reviews,
    positive,
    negative,
    approval_pct,
    wilson_score
FROM wilson_pool
ORDER BY positive DESC
LIMIT 50
