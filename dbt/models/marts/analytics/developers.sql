{# Splits comma-separated developer field into individual deduplicated rows. #}
{# Preserves business-name suffixes (LLC, Inc., Ltd., Co., Corp.) by using  #}
{# a sentinel string to protect commas before known suffixes.              #}

with games as (
    select
        developer,
        partition_date
    from {{ ref('stg_steamspy__games') }}
    where developer is not null and developer != ''
),

developers_suffixes_protected as (
    select
        replaceRegexpAll(
            replaceRegexpAll(
                ifNull(games.developer, ''),
                ',\\s*(co\\.,\\s*ltd\\.?)',
                '|||\\1'
            ),
            ',\\s*(llc\\.?|inc\\.?|ltd\\.?|l\\.l\\.c\\.?|co\\.|corp\\.?)',
            '|||\\1'
        ) as protected_str,
        games.partition_date
    from games
),

developers_exploded as (
    select
        trim(
            replaceAll(dev_raw, '|||', ', ')
        ) as developer,
        developers_suffixes_protected.partition_date
    from developers_suffixes_protected
    array join splitByChar(
        ',', developers_suffixes_protected.protected_str
    ) as dev_raw
),

developers_deduped as (
    select distinct
        developers_exploded.developer,
        developers_exploded.partition_date
    from developers_exploded
    where developers_exploded.developer != ''
)

select
    rowNumberInAllBlocks() as developer_id,
    developers_deduped.developer,
    developers_deduped.partition_date
from developers_deduped
