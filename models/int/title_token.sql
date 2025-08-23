{{
	config(
		alias='int_title_token',
		materialized='table'
	)
}}

WITH base AS (
    SELECT
        appid,	
        SPLIT(REGEXP_REPLACE(TRIM(name), '[^A-Za-z ]', ''), ' ') AS raw_title
    FROM {{ ref('staging_steam_games') }}
),

flatten AS (
    SELECT
        appid, 
        f.value::VARCHAR AS words 
    FROM base,
        LATERAL FLATTEN(input => raw_title) f
)

SELECT * FROM flatten
    
	
