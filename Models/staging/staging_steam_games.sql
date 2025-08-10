{{
  config(
    alias='staging_steam_games'
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

SELECT 
    f.app_id,
    f.name,
    f.is_free,
    f.price,
    f.release_date,
    f.developer,
    f.publisher,
    f.genres,
    f.categories,
    f.platforms,
    f.metacritic_score,
    f.recommendations,
    f.short_description,
    f.languages
FROM final AS f
