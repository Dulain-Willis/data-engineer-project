{{
	config(
		alias='int_title_token',
		materialized='table'
	)
}}

WITH base AS (
    SELECT
        appid,	
        SPLIT(REGEXP_REPLACE(TRIM(title), '[^A-Za-z ]', ''), ' ') AS raw_title
    FROM {{ ref('staging_steam_games') }}
),

final AS (
    SELECT
        appid, 
        f.value::VARCHAR AS words 
    FROM base,
        LATERAL FLATTEN(input => raw_title) f
	WHERE f.value != ''
		AND LENGTH(f.value) > 2
		AND f.value NOT IN (
			SELECT word	
			FROM {{ ref('stop_words') }}
		)
)

SELECT * FROM final

