{{
	config(
		alias='int_title_token',
		materialized='table'
	)
}}

SELECT
	TRIM(name)
	
