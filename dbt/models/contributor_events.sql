{{ config(materialized='ephemeral') }}

select dt, actor_id
from {{ source('silver', 'events') }}
where event_type in ('PushEvent', 'PullRequestEvent', 'IssuesEvent', 'PullRequestReviewEvent')
    and actor_login not like '%[bot]'
