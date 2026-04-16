{{
    config(
        materialized='table'
    )
}}

-- Recreates the silver_int_publishers Spark job logic in ClickHouse SQL.
-- Splits the comma-separated publisher field from silver_stg_games into
-- individual deduplicated publisher names, preserving business-name suffixes
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
                ifNull(publisher, ''),
                ',\\s*(co\\.,\\s*ltd\\.?)',
                '|||\\1'
            ),
            ',\\s*(llc\\.?|inc\\.?|ltd\\.?|l\\.l\\.c\\.?|co\\.|corp\\.?)',
            '|||\\1'
        ) AS protected_str,
        dt
    FROM {{ source('analytics', 'steamspy_silver_stg_games') }}
    WHERE publisher IS NOT NULL AND publisher != ''
),

exploded AS (
    SELECT
        trim(replaceAll(pub_raw, '|||', ', ')) AS publisher,
        dt
    FROM protected
    ARRAY JOIN splitByChar(',', protected_str) AS pub_raw
),

deduped AS (
    SELECT DISTINCT
        publisher,
        dt
    FROM exploded
    WHERE publisher != ''
)

SELECT
    rowNumberInAllBlocks() AS publisher_id,
    publisher,
    dt
FROM deduped
