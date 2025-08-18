{{
    config(
        alias='stg_steam_games',
        materialized='view'
    )
}}

WITH final AS (
    SELECT
        app_id,
        name,
        is_free,
        price::NUMBER(38,2) AS price,
        NULLIF(TO_DATE(release_date, 'MON DD, YYYY'), '') AS release_date,
        NULLIF(developer, '') AS developer,
        NULLIF(publisher, '') AS publisher,
        NULLIF(genres, '') AS genres,
        NULLIF(categories, '') AS categories,
        platforms,
        metacritic_score,
        recommendations,
        NULLIF(short_description, '') AS short_description,
        NULLIF(languages, '') AS languages
FROM {{ source('raw', 'raw_games') }}
)

SELECT *
FROM final
