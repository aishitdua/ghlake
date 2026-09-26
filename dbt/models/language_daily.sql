{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='dt') }}

-- GH Archive payloads only carry a repo language on forks
select dt, language, count(*) as forks
from {{ source('silver', 'events') }}
where event_type = 'ForkEvent' and language is not null
group by all
