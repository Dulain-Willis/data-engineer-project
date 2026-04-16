# dbt Style Guide

**Source articles (read in this order):**
1. [Guide Overview](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview?version=1.12)
2. [Staging](https://docs.getdbt.com/best-practices/how-we-structure/2-staging?version=1.12)
3. [Intermediate](https://docs.getdbt.com/best-practices/how-we-structure/3-intermediate?version=1.12)
4. [Marts](https://docs.getdbt.com/best-practices/how-we-structure/4-marts?version=1.12)
5. [The Rest of the Project](https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project?version=1.12)
6. [How We Style Our dbt Projects (overview)](https://docs.getdbt.com/best-practices/how-we-style/0-how-we-style-our-dbt-projects?version=1.12)
7. [How We Style Our dbt Models](https://docs.getdbt.com/best-practices/how-we-style/1-how-we-style-our-dbt-models?version=1.12)
8. [How We Style Our SQL](https://docs.getdbt.com/best-practices/how-we-style/2-how-we-style-our-sql?version=1.12)
9. [How We Style Our Jinja](https://docs.getdbt.com/best-practices/how-we-style/4-how-we-style-our-jinja?version=1.12)
10. [How We Style Our YAML](https://docs.getdbt.com/best-practices/how-we-style/5-how-we-style-our-yaml?version=1.12)

---

## Part 1 — Project Structure

### 1.1 Core Philosophy

The goal of project structure is to move data **from source-conformed to business-conformed** — transforming external system layouts into business-aligned definitions. Consistent structure reduces decision fatigue so team bandwidth can be spent on complex, unique problems rather than conventions.

The three primary transformation layers, in order:

| Layer | Metaphor | Purpose |
|-------|----------|---------|
| Staging | Atoms | Modular building blocks from raw source data |
| Intermediate | Molecules | Purpose-built transformation steps |
| Marts | Cells | Rich, business-entity representations for end users |

### 1.2 Full Reference Folder Tree

```
jaffle_shop/
├── analyses/
├── macros/
│   ├── _macros.yml
│   └── cents_to_dollars.sql
├── models/
│   ├── intermediate/
│   │   └── finance/
│   │       ├── _int_finance__models.yml
│   │       └── int_payments_pivoted_to_orders.sql
│   ├── marts/
│   │   ├── finance/
│   │   │   ├── _finance__models.yml
│   │   │   ├── orders.sql
│   │   │   └── payments.sql
│   │   └── marketing/
│   │       ├── _marketing__models.yml
│   │       └── customers.sql
│   ├── staging/
│   │   ├── jaffle_shop/
│   │   │   ├── _jaffle_shop__docs.md
│   │   │   ├── _jaffle_shop__models.yml
│   │   │   ├── _jaffle_shop__sources.yml
│   │   │   ├── base/
│   │   │   │   ├── base_jaffle_shop__customers.sql
│   │   │   │   └── base_jaffle_shop__deleted_customers.sql
│   │   │   ├── stg_jaffle_shop__customers.sql
│   │   │   └── stg_jaffle_shop__orders.sql
│   │   └── stripe/
│   │       ├── _stripe__models.yml
│   │       ├── _stripe__sources.yml
│   │       └── stg_stripe__payments.sql
│   └── utilities/
│       └── all_dates.sql
├── seeds/
│   └── employees.csv
├── snapshots/
└── tests/
    └── assert_positive_value_for_total_amount.sql
```

### 1.3 dbt_project.yml — Cascade Configs

Set materializations and custom schemas at the directory level so individual models don't need to repeat config blocks. This is the DRY approach.

```yaml
# dbt_project.yml
models:
  jaffle_shop:
    staging:
      +materialized: view
    intermediate:
      +materialized: ephemeral
    marts:
      +materialized: table
      finance:
        +schema: finance
      marketing:
        +schema: marketing
```

---

## Part 2 — Staging Layer

### 2.1 Purpose

Staging is the **1-to-1 entry point** for every source table. One source table = one staging model. Its job is to clean and standardise raw source-conformed data into reusable atoms. It does NOT combine or aggregate data.

**DRY test for staging:** If a transformation is needed in every downstream model and its absence would require repeating code, it belongs in staging.

### 2.2 Folder Organisation

Organise staging subdirectories **by source system**, not by loader tool or business function.

```
✅  models/staging/jaffle_shop/    (source system)
✅  models/staging/stripe/         (source system)
❌  models/staging/fivetran/       (loader — too broad)
❌  models/staging/marketing/      (business function — creates overlap/conflicting definitions)
```

### 2.3 Naming Convention

Pattern: `stg_[source]__[entity]s.sql`

- Double underscore (`__`) visually separates source system from entity name
- Entity names are plural (`customers`, `orders`, not `customer`, `order`)
- ✅ `stg_jaffle_shop__orders.sql`
- ❌ `stg_orders.sql` (missing source context)

### 2.4 Materialization

Always `view`. Downstream models get fresh data; no warehouse space is wasted on intermediate artefacts.

```yaml
models:
  jaffle_shop:
    staging:
      +materialized: view
```

### 2.5 Model Pattern — Two-CTE Structure

Every staging model follows this exact pattern:

```sql
-- stg_stripe__payments.sql
with source as (
    select * from {{ source('stripe', 'payment') }}
),

renamed as (
    select
        -- ids
        id as payment_id,
        orderid as order_id,

        -- strings
        paymentmethod as payment_method,
        case
            when payment_method in ('stripe', 'paypal', 'credit_card', 'gift_card') then 'credit'
            else 'cash'
        end as payment_type,
        status,

        -- numerics
        amount as amount_cents,
        amount / 100.0 as amount,

        -- booleans
        case
            when status = 'successful' then true
            else false
        end as is_completed_payment,

        -- dates
        date_trunc('day', created) as created_date,

        -- timestamps
        created::timestamp_ltz as created_at

    from source
)

select * from renamed
```

### 2.6 Allowed vs Forbidden Transformations in Staging

| Transformation | Allowed | Reason |
|---|---|---|
| Renaming columns | ✅ | Core purpose of staging |
| Type casting | ✅ | Core purpose of staging |
| Basic computation (cents → dollars) | ✅ | Non-lossy, always needed downstream |
| Conditional categorisation | ✅ | Non-lossy, always needed downstream |
| Joins | ❌ | Creates duplicated computation and confusing grain ripple |
| Aggregations | ❌ | Changes the table grain, loses source-level access downstream |

### 2.7 Base Models (When Joins Are Unavoidable)

When a join is required to produce a clean staging concept (e.g., soft deletes tracked in a separate table), place the pre-join models in a `base/` subdirectory within the source system folder. Base models do non-joining transformations only; the staging model performs the join.

```sql
-- base/base_jaffle_shop__customers.sql
with source as (
    select * from {{ source('jaffle_shop', 'customers') }}
),

customers as (
    select
        id as customer_id,
        first_name,
        last_name
    from source
)

select * from customers
```

```sql
-- base/base_jaffle_shop__deleted_customers.sql
with source as (
    select * from {{ source('jaffle_shop', 'customer_deletes') }}
),

deleted_customers as (
    select
        id as customer_id,
        deleted as deleted_at
    from source
)

select * from deleted_customers
```

```sql
-- stg_jaffle_shop__customers.sql
with customers as (
    select * from {{ ref('base_jaffle_shop__customers') }}
),

deleted_customers as (
    select * from {{ ref('base_jaffle_shop__deleted_customers') }}
),

join_and_mark_deleted_customers as (
    select
        customers.*,
        case
            when deleted_customers.deleted_at is not null then true
            else false
        end as is_deleted
    from customers
    left join deleted_customers on customers.customer_id = deleted_customers.customer_id
)

select * from join_and_mark_deleted_customers
```

**Valid use cases for base models:**
- Joining delete/tombstone tables to mark or filter deleted records
- Unioning identically-structured data from multiple source systems before staging

---

## Part 3 — Intermediate Layer

### 3.1 Purpose

Intermediate models are **purpose-built transformation steps** that bridge staging atoms and mart cells. They exist to simplify mart models — they should not be exposed to end users.

### 3.2 Folder Organisation

Organise by **business concern** (not source system — that was staging's job).

```
models/intermediate/
└── finance/
    ├── _int_finance__models.yml
    └── int_payments_pivoted_to_orders.sql
```

### 3.3 Naming Convention

Pattern: `int_[entity]s_[verb]s.sql`

- Drop the double underscore (no longer source-system-level)
- Use action-oriented verbs that describe the transformation
- The name should be self-documenting

| Example filename | What it signals |
|---|---|
| `int_payments_pivoted_to_orders.sql` | Payments pivoted to order grain |
| `int_users_aggregated_to_campaigns.sql` | Users rolled up to campaign level |
| `int_orders_fanned_out_by_quantity.sql` | Orders expanded by item quantity |
| `int_events_funnel_created.sql` | Funnel logic applied to events |

**Exception:** Keep double underscore when still operating at source-system level: `int_shopify__orders_summed.sql`.

### 3.4 Materialization

Two valid options:

| Option | Pros | Cons |
|---|---|---|
| `ephemeral` (default) | Warehouse stays clean, no extra objects | Harder to debug; errors may surface at mart level |
| `view` in custom schema | Queryable for debugging, still warehouse-efficient | Adds objects to warehouse |

For development troubleshooting, temporarily build chains as `table` to surface errors at the correct DAG node.

```yaml
models:
  jaffle_shop:
    intermediate:
      +materialized: ephemeral
```

### 3.5 Example Model

```sql
-- int_payments_pivoted_to_orders.sql
{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

with payments as (
    select * from {{ ref('stg_stripe__payments') }}
),

pivot_and_aggregate_payments_to_order_grain as (
    select
        order_id,
        {% for payment_method in payment_methods -%}
            sum(
                case
                    when payment_method = '{{ payment_method }}' and
                         status = 'success'
                    then amount
                    else 0
                end
            ) as {{ payment_method }}_amount,
        {%- endfor %}
        sum(case when status = 'success' then amount end) as total_amount
    from payments
    group by 1
)

select * from pivot_and_aggregate_payments_to_order_grain
```

### 3.6 DAG Design Rules

**Narrow the DAG, widen the tables.** As transformations progress toward business-conformed, the DAG should look like an arrowhead pointing right — many narrow inputs converging into fewer, wider outputs.

- ✅ **Several arrows going INTO a model** — good, expected
- ❌ **Several arrows coming OUT of a model** — red flag; suggests the model is doing too much or is being misused as a staging model

**Common intermediate use cases:**
1. **Structural simplification** — combine 4–6 staging models so a mart only needs 2 joins instead of 10
2. **Re-graining** — fan out or collapse rows to the correct grain (e.g., orders → order items using quantity)
3. **Isolating complex logic** — extract difficult transformations so they can be tested and documented in isolation

---

## Part 4 — Marts Layer

### 4.1 Purpose

Marts are **entity-grained tables** for end-user consumption. Each mart represents one business concept at its natural grain (orders, customers, territories, click events). They are the final, polished output.

> "Unlike a traditional Kimball star schema, in modern data warehousing — where storage is cheap and compute is expensive — we'll happily borrow and add any and all data from other concepts that are relevant to answering questions about the mart's core entity."

### 4.2 Folder Organisation

Organise by **department or business area**.

```
models/marts/
├── finance/
│   ├── _finance__models.yml
│   ├── orders.sql
│   └── payments.sql
└── marketing/
    ├── _marketing__models.yml
    └── customers.sql
```

### 4.3 Naming Convention

Name files by the **entity in plain English**, plural: `customers.sql`, `orders.sql`.

- ❌ `finance_orders.sql` / `marketing_orders.sql` — building the same concept differently for different teams is an anti-pattern
- ✅ Exception: clearly distinct concepts are fine (`tax_revenue.sql` vs `revenue.sql`)
- ❌ Time-based rollup names (`user_orders_per_day`) — those belong in the metrics layer

### 4.4 Materialization

Always `table` or `incremental`. Marts are queried by end users; they must be pre-computed.

**Materialization progression rule:**
1. Start with `view`
2. Move to `table` when queries become slow
3. Move to `incremental` when builds become slow

### 4.5 Design Rules

- Build **wide and denormalized** — give end users everything they need about an entity in one table
- Avoid more than **4–5 joins** in one mart — if you need more, use intermediate models to pre-combine concepts
- **Marts are entity-grained**: include data from related concepts, but the row grain stays fixed on the primary entity
- **Do not** use semantic rollups (e.g., `orders_per_day`) in marts — those are metrics
- When referencing another mart in a mart, be intentional about grain changes

### 4.6 Semantic Layer Note

- **Without Semantic Layer** → embrace aggressive denormalization
- **With Semantic Layer (MetricFlow)** → prefer normalized mart structures; MetricFlow handles joins at query time

### 4.7 Example Models

```sql
-- orders.sql
with orders as (
    select * from {{ ref('stg_jaffle_shop__orders') }}
),

order_payments as (
    select * from {{ ref('int_payments_pivoted_to_orders') }}
),

orders_and_order_payments_joined as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        coalesce(order_payments.total_amount, 0) as amount,
        coalesce(order_payments.gift_card_amount, 0) as gift_card_amount
    from orders
    left join order_payments on orders.order_id = order_payments.order_id
)

select * from orders_and_order_payments_joined
```

```sql
-- customers.sql
with customers as (
    select * from {{ ref('stg_jaffle_shop__customers') }}
),

orders as (
    select * from {{ ref('orders') }}
),

customer_orders as (
    select
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value
    from orders
    group by 1
),

customers_and_customer_orders_joined as (
    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
        customer_orders.lifetime_value
    from customers
    left join customer_orders on customers.customer_id = customer_orders.customer_id
)

select * from customers_and_customer_orders_joined
```

---

## Part 5 — YAML Configuration

### 5.1 One YAML File Per Folder (Recommended)

Create `_[directory]__models.yml` per directory. The leading underscore sorts the file to the top of every folder.

```
models/staging/jaffle_shop/
├── _jaffle_shop__models.yml      ← model definitions for this folder
├── _jaffle_shop__sources.yml     ← source definitions for this folder
├── _jaffle_shop__docs.md         ← doc blocks for this folder
├── stg_jaffle_shop__customers.sql
└── stg_jaffle_shop__orders.sql
```

**What to avoid:**
- Single monolith YAML: hard to locate specific configs as project grows
- One file per model: creates management overhead

### 5.2 Other Project Folders

| Folder | Use For | Do NOT Use For |
|---|---|---|
| `seeds/` | Lookup tables that don't exist in source systems (zip codes → states, UTM → campaigns) | Loading source data — dbt is not a data loading tool |
| `analyses/` | Jinja-templated audit queries (e.g., audit helper package); version-controlled but not built into the warehouse | — |
| `tests/` | Singular (integration-style) tests that verify how multiple models interact | Replacing generic column tests |
| `snapshots/` | Type 2 slowly changing dimension records from destructively updated sources | — |
| `macros/` | DRY-ing up repeated transformation logic | — |
| `macros/_macros.yml` | Documenting macro purpose and arguments | — |

### 5.3 Groups

Groups restrict access to private models and enable intentional cross-team collaboration.

```yaml
# Defined in any .yml file under a `groups:` key
groups:
  - name: finance
    owner:
      email: finance-team@company.com
```

### 5.4 When to Split Projects (dbt Mesh)

**Valid reasons to split:**
- Business groups or departments with distinct governance needs
- Data security requirements
- Project size exceeding ~1000 models

**Invalid reasons:**
- ML vs reporting use cases (they should share the same marts/metrics)

---

## Part 6 — Model Naming & Field Conventions

### 6.1 Model Names

- Models must be **plural**: `customers`, `orders`, `products`
- Use `snake_case` for schemas, tables, and columns — no dots in model names
  - ✅ `stg_jaffle_shop__orders`
  - ❌ `stg.jaffle_shop.orders` (dots require quoting; confuses database.schema.object path)
- No abbreviations — `customer` not `cust`, `order` not `ord`
- No reserved words as column names
- Version suffixes: `customers_v1`, `customers_v2`

### 6.2 Primary Keys

- Every model must have a primary key
- Format: `<object>_id` — `customer_id`, `order_id`, `payment_id`
- Type: **string** (not integer) — prevents issues across platforms
- Must be consistent across all models referencing the same entity

### 6.3 Column Naming by Type

| Type | Pattern | Example | Notes |
|---|---|---|---|
| Primary key | `<object>_id` | `order_id` | String type |
| Foreign key | `<referenced_object>_id` | `customer_id` | Must match PK name in referenced model |
| Boolean | `is_<state>` or `has_<thing>` | `is_deleted`, `has_discount` | — |
| Timestamp | `<event>_at` | `created_at`, `deleted_at` | UTC; non-UTC gets suffix: `created_at_pt` |
| Date | `<event>_date` | `created_date`, `order_date` | — |
| Event tense | past tense | `created`, `updated`, `deleted` | — |
| Price/revenue | decimal currency | `amount`, `price` | Non-decimal gets suffix: `price_in_cents` |

### 6.4 Column Ordering Within a Model

Always group columns in this order:
1. IDs (primary key first, then foreign keys)
2. Strings
3. Numerics
4. Booleans
5. Dates
6. Timestamps

### 6.5 Terminology

Use **business terminology**, not source system terminology. If the business calls them "customers", use `customer_id` — not `user_id` even if the source table has a `user_id` column.

---

## Part 7 — SQL Style

### 7.1 Basics

- Use **SQLFluff** to enforce style automatically (configure via `.sqlfluff`; ignore `target/`, `dbt_packages/`, `macros/` in `.sqlfluffignore`)
- 4-space indentation
- Lines ≤ 80 characters
- All field names, keywords, and function names **lowercase**
- Explicit `as` keyword for all aliases (field and table)
- **Trailing commas** (comma at the end of a line, not the start)
- Use Jinja comments `{# comment #}` for comments that should not appear in compiled SQL

### 7.2 CTE Structure

Every model follows this CTE pattern:

1. **Import CTEs at the top** — all `{{ ref('...') }}` and `{{ source('...') }}` calls go here, named after their source table, and filtered/column-pruned as tightly as possible
2. **Functional CTEs** — each performs one logical unit of work; name them verbosely to describe the transformation
3. **Final line** — `select * from <final_cte>` — this makes it trivial to audit intermediate steps and materialize at any point

```sql
with my_data as (
    select
        field_1,
        field_2,
        field_3,
        cancellation_date,
        expiration_date,
        start_date
    from {{ ref('my_data') }}
),

some_cte as (
    select
        id,
        field_4,
        field_5
    from {{ ref('some_cte') }}
),

some_cte_agg as (
    select
        id,
        sum(field_4) as total_field_4,
        max(field_5) as max_field_5
    from some_cte
    group by 1
),

joined as (
    select
        my_data.field_1,
        my_data.field_2,
        my_data.field_3,
        -- use line breaks to visually separate calculations into blocks
        case
            when my_data.cancellation_date is null
                and my_data.expiration_date is not null
                then expiration_date
            when my_data.cancellation_date is null
                then my_data.start_date + 7
            else my_data.cancellation_date
        end as cancellation_date,
        some_cte_agg.total_field_4,
        some_cte_agg.max_field_5
    from my_data
    left join some_cte_agg
        on my_data.id = some_cte_agg.id
    where my_data.field_1 = 'abc' and
        (
            my_data.field_2 = 'def' or
            my_data.field_2 = 'ghi'
        )
    having count(*) > 1
)

select * from joined
```

**Import CTE with filtering:**
```sql
with orders as (
    select
        order_id,
        customer_id,
        order_total,
        order_date
    from {{ ref('orders') }}
    where order_date >= '2020-01-01'
)
```

**CTE comment syntax:**
```sql
with events as (
    ...
),

{# This CTE filters to only successful events #}
filtered_events as (
    ...
)

select * from filtered_events
```

### 7.3 Aggregations & Grouping

- Fields should be stated **before** aggregates and window functions
- Aggregate **as early as possible** on the smallest dataset before joining — improves performance significantly
- Group/order by **number**, not column name: `group by 1, 2` not `group by customer_id, order_date`

### 7.4 Joins

- **Always prefix column names** with the table name when joining two or more tables; omit prefix when selecting from a single table
- Write **explicit join types**: `inner join`, `left join`, `cross join` — never bare `join`
- **Avoid table alias initialisms**: use `customers` not `c`; use `order_payments` not `op`
- Prefer `union all` over `union` unless deduplication is explicitly required
- Always move **left to right**: left table is the base, right table is joined in. If you need a `right join`, swap which table is in `from` vs `join`

### 7.5 Model Config Block

In-model config should be at the top, formatted with one argument per line:

```sql
{{
    config(
        materialized = 'table',
        sort = 'id',
        dist = 'id'
    )
}}
```

If the config applies to an entire directory, put it in `dbt_project.yml` instead — not in every model.

---

## Part 8 — Jinja Style

### 8.1 Four Rules

1. **Delimiter spacing** — always space inside delimiters
   - ✅ `{{ this }}`, `{% if condition %}`
   - ❌ `{{this}}`, `{%if condition%}`

2. **Newlines for logical blocks** — separate Jinja blocks visually with blank lines

3. **4-space indentation inside Jinja blocks** — visually communicate that code is wrapped

4. **Don't obsess over whitespace control** — readable project code matters more than perfectly formatted compiled output; the `-%}` / `{%-` whitespace controls are optional

### 8.2 Macro Example

```jinja
{% macro make_cool(uncool_id) %}
    do_cool_thing({{ uncool_id }})
{% endmacro %}
```

### 8.3 Conditional Logic Example

```sql
select
    entity_id,
    entity_type,

    {% if this %}
        {{ that }},
    {% else %}
        {{ the_other_thing }},
    {% endif %}

    {{ make_cool('uncool_id') }} as cool_id
```

### 8.4 Loop Example (from intermediate models)

```sql
{%- set payment_methods = ['bank_transfer', 'credit_card', 'coupon', 'gift_card'] -%}

select
    order_id,
    {% for payment_method in payment_methods -%}
        sum(
            case
                when payment_method = '{{ payment_method }}' and status = 'success'
                then amount
                else 0
            end
        ) as {{ payment_method }}_amount,
    {%- endfor %}
    sum(case when status = 'success' then amount end) as total_amount
from payments
group by 1
```

---

## Part 9 — YAML Style

### 9.1 Rules

- 2-space indentation (not 4)
- List items must be indented
- Lines ≤ 80 characters
- Single-entry list items can be strings but explicit lists are preferred
  - `'select': 'other_user'` is valid
  - `'select': ['other_user']` is preferred
- Separate dictionary list items with blank lines where appropriate
- Use the dbt JSON schema with a compatible IDE for validation
- Use Prettier for automatic formatting

### 9.2 Full YAML Example

```yaml
models:
  - name: events
    columns:
      - name: event_id
        description: This is a unique identifier for the event
        data_tests:
          - unique
          - not_null

      - name: event_time
        description: "When the event occurred in UTC (eg. 2018-01-01 12:00:00)"
        data_tests:
          - not_null

      - name: user_id
        description: The ID of the user who recorded the event
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('users')
                field: id
```

### 9.3 YAML File Naming

All YAML files use a leading underscore so they sort to the top of directories:

| File | Purpose |
|---|---|
| `_[dir]__models.yml` | Model descriptions, column docs, tests |
| `_[dir]__sources.yml` | Source declarations |
| `_[dir]__docs.md` | Long-form doc blocks |
| `_macros.yml` | Macro argument documentation |

---

## Part 10 — Quick-Reference Rules (LLM Decision Table)

Use this table when deciding how to handle a specific scenario.

| Scenario | Rule |
|---|---|
| Where does a new source table enter the DAG? | Create one staging model: `stg_[source]__[entity]s.sql` |
| What materialization for staging? | Always `view` |
| What materialization for intermediate? | `ephemeral` (default) or `view` in custom schema |
| What materialization for marts? | `table` or `incremental` |
| A staging model needs a join | Use `base/` models; staging model joins base models |
| Should staging aggregate? | Never — aggregation changes the grain |
| CTE naming in functional CTEs | Verbose and action-oriented (`events_joined_to_users`, not `user_events`) |
| Where do `{{ ref() }}` calls go? | Import CTEs at the top of the file only |
| Last line of every model | `select * from <final_cte>` |
| Column aliases | Always use explicit `as` |
| Join type | Always explicit: `inner join`, `left join`, never bare `join` |
| Column name in multi-table query | Always prefix with table name |
| Group by / order by | Use numeric position (`group by 1, 2`) |
| Boolean column names | Must start with `is_` or `has_` |
| Timestamp column names | `<event>_at`, UTC, e.g. `created_at` |
| Date column names | `<event>_date`, e.g. `order_date` |
| Primary key type | String |
| Primary key format | `<object>_id` |
| Two departments want different versions of the same entity | Single model — splitting by team is an anti-pattern |
| CTE duplicated across multiple models | Extract to intermediate model |
| More than 4–5 joins in a mart | Refactor into intermediate model(s) first |
| Many arrows exiting one model | Red flag — redesign the model |
| Seeds use case | Lookup tables not in source systems only (zip codes, UTM params) |
| Tests folder use case | Integration-style tests across multiple models |
| Macros folder | Always include `_macros.yml` with argument docs |
| YAML indentation | 2 spaces |
| SQL indentation | 4 spaces |
| Jinja delimiter spacing | Always space inside: `{{ x }}` not `{{x}}` |
| Config applies to whole directory | Put in `dbt_project.yml`, not per-model config blocks |

---

## Part 11 — Overriding This Guide

> "The most important thing is not to follow this style guide; it's to make your style guide and follow it."
> "Overall, consistency is more important than any of these specific conventions."

When deviating from these patterns, document the decision. The guide is a starting point — adapt it to the team's needs and enforce it with SQLFluff, Prettier, and YAML schema validation rather than code review comments.
