{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='dt') }}

select dt, hour, event_type, count(*) as events, count(distinct actor_id) as actors
from {{ source('silver', 'events') }}
group by all
