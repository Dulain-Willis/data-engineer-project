SELECT
    app_id,
    name,
    is_free,
    price,
    release_date,
    developer,
    publisher,
    genres,
    catergories,
    platforms,
    metacritic_score,
    recommendations,
    short_description,
    languages
FROM {{ source('raw', 'raw_games') }}