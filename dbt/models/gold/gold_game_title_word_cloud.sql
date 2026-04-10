{{
    config(
        materialized='table'
    )
}}

{% set stop_words = ['the', 'of', 'and', 'a', 'an', 'in', 'to', 'is', 'for', 'on', 'with', 'at', 'by', 'from', 'or', 'be', 'as', 'it', 'my', 'no', 'ii'] %}

-- strip non-letters, split each title into one word per row
WITH words AS (
    SELECT
        arrayJoin( -- equivalent to UNNEST
            splitByNonAlpha(assumeNotNull(game_title)) -- CH-specific: splits on any non-letter character, skipping empty elements
        ) AS word
    FROM {{ source('analytics', 'steamspy_silver_stg_games') }}
    WHERE game_title IS NOT NULL
)

-- count word frequency across all titles, excluding single characters and stop words
SELECT
    word,
    count(*) AS word_count
FROM words
WHERE
    length(word) > 1
    AND word NOT IN ('{{ stop_words | join("', '") }}')
GROUP BY word
ORDER BY word_count DESC
