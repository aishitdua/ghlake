{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='actor_id') }}

with batch as (
    select actor_id, min(dt) as first_seen, max(dt) as last_seen
    from {{ ref('contributor_events') }}
    group by actor_id
)
{% if is_incremental() %}
select
    b.actor_id,
    least(b.first_seen, coalesce(t.first_seen, b.first_seen)) as first_seen,
    greatest(b.last_seen, coalesce(t.last_seen, b.last_seen)) as last_seen
from batch b
left join {{ this }} t using (actor_id)
{% else %}
select * from batch
{% endif %}
