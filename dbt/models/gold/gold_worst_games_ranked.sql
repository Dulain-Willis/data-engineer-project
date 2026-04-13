{{
    config(
        materialized='view'
    )
}}

-- Bottom 10 Steam games by raw negative review volume.
SELECT
    row_number() OVER (ORDER BY negative DESC) AS rank,
    game_title,
    positive,
    negative,
    positive + negative AS total_reviews,
    round(positive / (positive + negative) * 100, 1) AS approval_pct
FROM {{ source('analytics', 'steamspy_silver_stg_games') }}
WHERE positive + negative > 0
ORDER BY negative DESC
LIMIT 10
