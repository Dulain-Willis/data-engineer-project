{{
    config(
        alias='dim_words',
        materialized='view'
    )
}}

SELECT 
    {{ dbt_utils.generate_surrogate_key(['words']) }} AS words,
    words 
FROM {{ ref('title_token') }} 
