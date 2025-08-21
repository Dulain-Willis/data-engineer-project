{{
	config(
		alias='int_platforms'
		materialized='view'
	)
}}

--select appid & then convert platforms into a json object (it was json but ingested as varchar so
--it's already in the correct format)

WITH base AS (
    SELECT
        appid,
        parse_json(platforms) AS platforms_json
    FROM raw.raw_games
),

--get the keys from platforms_json { "mac": false, "linux": true, "windows": true } and return an 
--array of keys ["mac","linux","windows"] then lateral flatten into one row per appid and platform. Now
--there's a table with a row for appid and every platform true or false, so filter with get saying
--for each row in by the flatten look up value inside the JSON object for this game, convert that value to a plain BOOLEAN, and keep the row if it’s true while discarding it if it’s false (or NULL).

final AS (
    SELECT
        b.appid,
        v.value::string AS platform
    FROM base b,
         lateral flatten(input => object_keys(platforms_json)) v
    WHERE GET(b.platforms_json, v.value)::BOOLEAN
)

SELECT *
FROM final;