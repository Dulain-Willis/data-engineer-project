{# Splits comma-separated publisher field into individual deduplicated rows. #}
{# Preserves business-name suffixes (LLC, Inc., Ltd., Co., Corp.) by using  #}
{# a sentinel string to protect commas before known suffixes.              #}

with games as (
    select
        app_id,
        publisher,
        positive,
        negative,
        partition_date
    from {{ ref('stg_steamspy__games') }}
    where publisher is not null and publisher != ''
),

publishers_suffixes_protected as (
    select
        games.app_id,
        replaceRegexpAll(
            replaceRegexpAll(
                ifNull(games.publisher, ''),
                ',\\s*(co\\.,\\s*ltd\\.?)',
                '|||\\1'
            ),
            ',\\s*(llc\\.?|inc\\.?|ltd\\.?|l\\.l\\.c\\.?|co\\.|corp\\.?)',
            '|||\\1'
        ) as protected_str,
        games.positive,
        games.negative,
        games.partition_date
    from games
),

publishers_exploded as (
    select
        publishers_suffixes_protected.app_id,
        trim(
            replaceAll(pub_raw, '|||', ', ')
        ) as publisher,
        publishers_suffixes_protected.positive,
        publishers_suffixes_protected.negative,
        publishers_suffixes_protected.partition_date
    from publishers_suffixes_protected
    array join splitByChar(
        ',', publishers_suffixes_protected.protected_str
    ) as pub_raw
),

publishers_deduped as (
    select distinct
        publishers_exploded.app_id,
        publishers_exploded.publisher,
        publishers_exploded.positive,
        publishers_exploded.negative,
        publishers_exploded.partition_date
    from publishers_exploded
    where publishers_exploded.publisher != ''
)

select * from publishers_deduped
