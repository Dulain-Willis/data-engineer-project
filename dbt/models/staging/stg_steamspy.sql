{{ 
    config(
        materialized='view'
    ) 
}}

SELECT
    appid,
    name AS title,
    developer,
    publisher,
    positive,
    negative,
    userscore,
    owners,
    average_forever,
    average_2weeks,
    median_forever,
    median_2weeks,
    ccu,
    toDecimal64OrZero(price, 2) AS price,
    toDecimal64OrZero(initialprice, 2) AS initial_price,
    toInt32OrZero(discount) AS discount_pct,
    ingestion_timestamp,
    dt
FROM {{ source('silver', 'steamspy_silver') }}
