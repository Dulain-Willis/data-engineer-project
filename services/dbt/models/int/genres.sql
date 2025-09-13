{{
	config(
		alias='int_genres',
		materialized='view'
	)
}}

--select appid and genres from staging but make null rows blank, remove any spaces around commas, and 
--split the string into an array using a comma as the delimiter

WITH base AS (
	SELECT
		appid,
		SPLIT(
            REGEXP_REPLACE(
                COALESCE(genres, ''),
            '\\s*,\\s*', ','),
        ',') AS split_genres
	FROM {{ ref('staging_steam_games') }}
),

--select appid and genres from the previous CTE, flattening the split genres array and keeping only the
--values that are not null and not blank either

final AS (
	SELECT
		appid,
		LOWER(TRIM(f.value))::VARCHAR AS genres
	FROM base,
		LATERAL FLATTEN(input => split_genres) AS f
	WHERE f.value IS NOT NULL
		AND TRIM(f.value) <> '' 
)

SELECT * FROM final

