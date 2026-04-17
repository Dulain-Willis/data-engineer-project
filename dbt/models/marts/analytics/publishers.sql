with publishers as (
    select distinct
        publisher,
        partition_date
    from {{ ref('int_games_publishers_exploded') }}
),

publisher_games as (
    select * from {{ ref('int_games_publishers_exploded') }}
),

publisher_game_metrics as (
    select
        publisher_games.publisher,
        publisher_games.partition_date,
        count(distinct publisher_games.app_id) as num_games,
        sum(publisher_games.positive + publisher_games.negative) as total_reviews,
        sum(publisher_games.positive) as total_positive,
        sum(publisher_games.negative) as total_negative,
        round(
            if(
                sum(publisher_games.positive + publisher_games.negative) > 0,
                sum(publisher_games.positive)
                / sum(publisher_games.positive + publisher_games.negative) * 100,
                0
            ),
            1
        ) as avg_approval_pct
    from publisher_games
    group by 1, 2
),

publishers_with_game_metrics as (
    select
        publishers.publisher,
        publisher_game_metrics.num_games,
        publisher_game_metrics.total_reviews,
        publisher_game_metrics.total_positive,
        publisher_game_metrics.total_negative,
        publisher_game_metrics.avg_approval_pct,
        publishers.partition_date
    from publishers
    left join publisher_game_metrics
        on publishers.publisher = publisher_game_metrics.publisher
        and publishers.partition_date = publisher_game_metrics.partition_date
)

select * from publishers_with_game_metrics
