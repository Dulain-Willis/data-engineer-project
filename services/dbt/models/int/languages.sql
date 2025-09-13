{{
	config(
		alias='int_languages',
		materialized='view'
	)
}}

WITH base AS (
	SELECT
        appid,
        SPLIT(
            REGEXP_REPLACE(
                COALESCE(languages, ''),
            '<.*?>|\\*|languages with full audio support', ''),
        ',')
        AS raw_languages
    FROM {{ ref('staging_steam_games') }}
),

final AS (
    SELECT
        b.appid,
        LOWER(TRIM(f.value::VARCHAR)) AS languages
    FROM base AS b,
        LATERAL FLATTEN(input => raw_languages) AS f
)

SELECT * FROM final
