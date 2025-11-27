{{
	config(
		alias='int_categories',
		materialized='view'
	)
}}

--select appid and categories from staging but make null rows blank, remove any spaces around commas, and 
--split the string into an array using a comma as the delimiter

WITH base AS (
	SELECT
		appid,
		SPLIT(
            REGEXP_REPLACE(
                COALESCE(categories, ''),
            '\\s*,\\s*', ','),
        ',') AS split_categories
	FROM {{ ref('staging_steam_games') }}
),

--select appid and categories from the previous CTE, flattening the split categories array and keeping
--only the values that are not null and not blank either

final AS (
	SELECT
		appid,
		LOWER(TRIM(f.value::VARCHAR)) AS categories
	FROM base,
		LATERAL FLATTEN(input => split_categories) AS f
	WHERE f.value IS NOT NULL
		AND TRIM(f.value) <> ''
)

SELECT * FROM final
