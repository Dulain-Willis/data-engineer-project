{{
    config(
        materialized='table'
    )
}}

-- Wilson score lower bound for a binomial proportion (95% confidence, z=1.96)
-- Ranks games fairly by balancing approval ratio with review volume.
-- Games with very few reviews are penalised even if all reviews are positive.
WITH review_stats AS (
    SELECT
        appid,
        game_title,
        positive,
        negative,
        positive + negative                                          AS total_reviews,
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
        round(approval_rate * 100, 1)                              AS approval_pct,
        -- Wilson score lower bound (z = 1.96 for 95% CI)
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
        )                                                           AS wilson_score
    FROM review_stats
)

SELECT
    row_number() OVER (ORDER BY wilson_score DESC) AS rank,
    game_title,
    total_reviews,
    positive,
    negative,
    approval_pct,
    wilson_score
FROM wilson
ORDER BY wilson_score DESC
LIMIT 50
