with source as (
    select * from {{ source('steamspy', 'steamspy_silver_stg_games') }}
),

renamed as (
    select
        -- ids
        appid as app_id,

        -- strings
        game_title,
        developer,
        publisher,
        score_rank,
        owners,
        price,
        initialprice as initial_price,
        discount,
        run_id,

        -- numerics
        positive,
        negative,
        userscore,
        average_forever as average_playtime_forever,
        average_2weeks as average_playtime_2weeks,
        median_forever as median_playtime_forever,
        median_2weeks as median_playtime_2weeks,
        ccu,

        -- timestamps
        ingestion_timestamp as ingested_at,

        -- dates
        dt as partition_date

    from source
)

select * from renamed
