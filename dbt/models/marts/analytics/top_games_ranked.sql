{# Hybrid ranking: Wilson score as a quality gate, raw positive volume as the rank. #}
{# Top 500 by Wilson score form the "credibility pool", then ranked by positive   #}
{# review count. Result: games both genuinely well-received AND widely played.    #}

with games as (
    select
        app_id,
        game_title,
        positive,
        negative,
        total_reviews,
        approval_pct,
        wilson_score
    from {{ ref('games') }}
    where total_reviews > 0
),

{# Quality gate: top 500 games by Wilson score #}
wilson_pool_filtered as (
    select *
    from games
    order by games.wilson_score desc
    limit 500
),

ranked_by_positive_volume as (
    select
        row_number() over (
            order by wilson_pool_filtered.positive desc
        ) as rank,
        wilson_pool_filtered.game_title,
        wilson_pool_filtered.total_reviews,
        wilson_pool_filtered.positive,
        wilson_pool_filtered.negative,
        wilson_pool_filtered.approval_pct,
        wilson_pool_filtered.wilson_score
    from wilson_pool_filtered
    order by wilson_pool_filtered.positive desc
    limit 50
)

select * from ranked_by_positive_volume
