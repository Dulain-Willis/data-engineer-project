{{
    config(
        materialized='table'
    )
}}

-- Recreates the silver_int_developers Spark job logic in ClickHouse SQL.
-- Splits the comma-separated developer field from silver_stg_games into
-- individual deduplicated developer names, preserving business-name suffixes
-- (LLC, Inc., Ltd., Co., Corp.) so they remain attached to their parent name.
--
-- Approximates smart_split_comma by using a sentinel string to protect commas
-- before known business suffixes before splitting on remaining commas.
WITH protected AS (
    SELECT
        -- Pass 1: protect compound suffixes like "co., ltd."
        -- Pass 2: protect single suffixes like "llc", "inc", "ltd", etc.
        replaceRegexpAll(
            replaceRegexpAll(
                ifNull(developer, ''),
                ',\\s*(co\\.,\\s*ltd\\.?)',
                '|||\\1'
            ),
            ',\\s*(llc\\.?|inc\\.?|ltd\\.?|l\\.l\\.c\\.?|co\\.|corp\\.?)',
            '|||\\1'
        ) AS protected_str,
        dt
    FROM {{ source('analytics', 'steamspy_silver_stg_games') }}
    WHERE developer IS NOT NULL AND developer != ''
),

exploded AS (
    SELECT
        trim(replaceAll(dev_raw, '|||', ', ')) AS developer,
        dt
    FROM protected
    ARRAY JOIN splitByChar(',', protected_str) AS dev_raw
),

deduped AS (
    SELECT DISTINCT
        developer,
        dt
    FROM exploded
    WHERE developer != ''
)

SELECT
    rowNumberInAllBlocks() AS developer_id,
    developer,
    dt
FROM deduped
