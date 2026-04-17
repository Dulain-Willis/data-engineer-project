{% set stop_words = ['the', 'of', 'and', 'a', 'an', 'in', 'to', 'is', 'for', 'on', 'with', 'at', 'by', 'from', 'or', 'be', 'as', 'it', 'my', 'no', 'ii'] %}

with games as (
    select
        game_title
    from {{ ref('stg_steamspy__games') }}
    where game_title is not null
),

titles_split_to_words as (
    select
        arrayJoin(
            splitByNonAlpha(assumeNotNull(games.game_title))
        ) as word
    from games
),

words_counted as (
    select
        titles_split_to_words.word,
        count(*) as word_count
    from titles_split_to_words
    where
        length(titles_split_to_words.word) > 1
        and titles_split_to_words.word not in (
            '{{ stop_words | join("', '") }}'
        )
    group by 1
    order by word_count desc
)

select * from words_counted
