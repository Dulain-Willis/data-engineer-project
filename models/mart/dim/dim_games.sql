{{
	config(
		alias='dim_games',
		materialized='view'
	)
}}

WITH final AS (
	SELECT 
		{{ dbt_utils.generate_surrogate_key(['appid','title']) }} AS games_sk,
		appid,
		title
	FROM {{ ref('staging_steam_games') }}
)

SELECT * FROM final
