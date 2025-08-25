{{
	config(
		alias='fact_words',
		materialized='view'
	)
}}

WITH final AS (
	SELECT 
		g.games_sk,
		w.words_sk,
		COUNT(*) AS word_cnt
	FROM {{ ref('title_token') }} t
	JOIN {{ ref('dim_games') }} g
		ON g.appid = t.appid
	JOIN {{ ref('dim_words') }} w
		ON w.words = t.words
	GROUP BY g.games_sk, w.words_sk
)

SELECT * FROM final
