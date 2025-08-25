{{
    config(
        alias='dim_words',
        materialized='view'
    )
}}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['words']) }} AS words_sk,
    words 
FROM {{ ref('title_token') }} 
WHERE words != ''
