{{
	config(
		alias='int_genres'
		materialized='view'
	)
}}

WITH base AS (
	SELECT
		app_id,
		split(
			regex_replace(
				coalesce(genres, ''),
			'\\s*,\\s*', ','),
		',') AS split_genres
	FROM {{ ref('staging_steam_games') }}
),

flatten AS (
	SELECT
		app_id,
		trim(f.value)::varchar
	FROM base
		lateral flatten(input => split_genres) AS f
)

