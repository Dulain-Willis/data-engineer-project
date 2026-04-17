with games as (
    select
        app_id,
        game_title,
        positive,
        negative,
        total_reviews,
        approval_pct
    from {{ ref('games') }}
    where total_reviews > 0
),

ranked_by_negative_volume as (
    select
        row_number() over (
            order by games.negative desc
        ) as rank,
        games.game_title,
        games.positive,
        games.negative,
        games.total_reviews,
        games.approval_pct
    from games
    order by games.negative desc
    limit 10
)

select * from ranked_by_negative_volume
