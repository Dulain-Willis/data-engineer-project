{{
    config(
        alias='int_ratings'
        materialized='view'
    )
}}

WITH final AS (
    SELECT 
        appid,
        title,
        metacritic_score,
        recommendations
    FROM {{ ref('staging_steam_games') }} 
)
