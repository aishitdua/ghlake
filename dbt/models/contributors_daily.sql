{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='dt') }}

select dt, count(distinct actor_id) as active
from {{ ref('contributor_events') }}
group by dt
