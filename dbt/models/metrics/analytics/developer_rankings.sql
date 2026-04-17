with developers as (
    select * from {{ ref('developers') }}
),

ranked_by_total_reviews as (
    select
        row_number() over (
            order by developers.total_reviews desc
        ) as rank,
        developers.developer,
        developers.num_games,
        developers.total_reviews,
        developers.total_positive,
        developers.total_negative,
        developers.avg_approval_pct
    from developers
    where developers.total_reviews > 0
    order by developers.total_reviews desc
    limit 20
)

select * from ranked_by_total_reviews
