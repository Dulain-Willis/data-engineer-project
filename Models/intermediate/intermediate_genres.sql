{{
	config(
		alias='int_genres',
		materialized='view'
	)
}}

WITH base AS (
	SELECT
		app_id,
		split(
			regexp_replace(
				coalesce(genres, ''),
			'\\s*,\\s*', ','),
		',') AS split_genres
	FROM {{ ref('staging_steam_games') }}
),

flatten AS (
	SELECT
		app_id,
		NULLIF(TRIM(f.value)::varchar, '') AS genre_raw
	FROM base,
		lateral flatten(input => split_genres) AS f
),

final AS (
	SELECT
		app_id,
		NULLIF(genre_raw, '') AS genre
	FROM flatten
	WHERE NULLIF(genre_raw, '') IS NOT NULL
)

SELECT * FROM final

