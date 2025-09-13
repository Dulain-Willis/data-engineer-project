{{
	config(
		alias='agg_top_words',
		materialized='view'
	)
}}

WITH final AS (
	SELECT 
		d.words,
		f.word_cnt
	FROM {{ ref('fact_words') }} f
	JOIN {{ ref('dim_words') }} d
		ON d.words_sk = f.words_sk
ORDER BY f.word_cnt DESC
)

SELECT * FROM final

