with developers as (
    select distinct
        developer,
        partition_date
    from {{ ref('int_games_developers_exploded') }}
),

developer_games as (
    select * from {{ ref('int_games_developers_exploded') }}
),

developer_game_metrics as (
    select
        developer_games.developer,
        developer_games.partition_date,
        count(distinct developer_games.app_id) as num_games,
        sum(developer_games.positive + developer_games.negative) as total_reviews,
        sum(developer_games.positive) as total_positive,
        sum(developer_games.negative) as total_negative,
        round(
            if(
                sum(developer_games.positive + developer_games.negative) > 0,
                sum(developer_games.positive)
                / sum(developer_games.positive + developer_games.negative) * 100,
                0
            ),
            1
        ) as avg_approval_pct
    from developer_games
    group by 1, 2
),

developers_with_game_metrics as (
    select
        developers.developer,
        developer_game_metrics.num_games,
        developer_game_metrics.total_reviews,
        developer_game_metrics.total_positive,
        developer_game_metrics.total_negative,
        developer_game_metrics.avg_approval_pct,
        developers.partition_date
    from developers
    left join developer_game_metrics
        on developers.developer = developer_game_metrics.developer
        and developers.partition_date = developer_game_metrics.partition_date
)

select * from developers_with_game_metrics
