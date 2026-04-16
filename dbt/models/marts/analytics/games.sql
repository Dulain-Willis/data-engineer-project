with games as (
    select * from {{ ref('stg_steamspy__games') }}
),

games_with_review_metrics as (
    select
        -- ids
        games.app_id,

        -- strings
        games.game_title,
        games.developer,
        games.publisher,
        games.score_rank,
        games.owners,
        games.price,
        games.initial_price,
        games.discount,

        -- numerics
        games.positive,
        games.negative,
        games.positive + games.negative as total_reviews,
        round(
            if(
                games.positive + games.negative > 0,
                games.positive / (games.positive + games.negative) * 100,
                0
            ),
            1
        ) as approval_pct,
        round(
            if(
                games.positive + games.negative > 0,
                (
                    (games.positive / (games.positive + games.negative))
                    + (1.96 * 1.96)
                    / (2 * (games.positive + games.negative))
                    - 1.96 * sqrt(
                        (
                            (games.positive / (games.positive + games.negative))
                            * (1 - (games.positive / (games.positive + games.negative)))
                            + (1.96 * 1.96)
                            / (4 * (games.positive + games.negative))
                        )
                        / (games.positive + games.negative)
                    )
                )
                / (1 + (1.96 * 1.96) / (games.positive + games.negative)),
                0
            ),
            4
        ) as wilson_score,
        games.userscore,
        games.average_playtime_forever,
        games.average_playtime_2weeks,
        games.median_playtime_forever,
        games.median_playtime_2weeks,
        games.ccu,

        -- timestamps
        games.ingested_at,

        -- dates
        games.partition_date

    from games
)

select * from games_with_review_metrics
