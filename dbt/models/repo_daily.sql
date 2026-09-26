{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='dt') }}

select
    dt,
    repo_id,
    arg_max(repo_name, created_at) as repo_name,
    count(*) as events,
    count(*) filter (event_type = 'PushEvent') as pushes,
    count(*) filter (event_type = 'WatchEvent') as stars,
    count(*) filter (event_type = 'ForkEvent') as forks,
    count(*) filter (event_type = 'PullRequestEvent' and action = 'opened') as prs_opened,
    count(*) filter (event_type = 'IssuesEvent' and action = 'opened') as issues_opened,
    count(distinct actor_id) as actors
from {{ source('silver', 'events') }}
-- a few upstream ForkEvents arrive with an empty repo object
where repo_id is not null
group by dt, repo_id
-- ponytail: drops long-tail repos to keep gold small; lower the bar if a use needs them
having stars + forks + prs_opened + issues_opened > 0 or events >= 10
