with publishers as (
    select * from {{ ref('publishers') }}
),

ranked_by_total_reviews as (
    select
        row_number() over (
            order by publishers.total_reviews desc
        ) as rank,
        publishers.publisher,
        publishers.num_games,
        publishers.total_reviews,
        publishers.total_positive,
        publishers.total_negative,
        publishers.avg_approval_pct
    from publishers
    where publishers.total_reviews > 0
    order by publishers.total_reviews desc
    limit 20
)

select * from ranked_by_total_reviews
