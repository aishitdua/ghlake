with latest as (select max(dt) as d from {{ ref('repo_daily') }})
select
    repo_id,
    arg_max(repo_name, dt) as repo_name,
    coalesce(sum(stars) filter (dt > d - 7), 0) as stars_7d,
    coalesce(sum(stars) filter (dt <= d - 7), 0) as stars_prev_7d,
    round(stars_7d / 7, 2) as stars_per_day
from {{ ref('repo_daily') }}, latest
where dt > d - 14
group by repo_id
having stars_7d > 0
order by stars_7d desc
limit 500
