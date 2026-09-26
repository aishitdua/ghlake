with latest as (
    select max(dt) as d from (
        select dt from {{ ref('event_type_hourly') }} group by dt having count(distinct hour) = 24
    )
),
today as (
    select repo_id, repo_name, stars from {{ ref('repo_daily') }}, latest where dt = d
),
before as (
    select repo_id, sum(stars) / 7 as avg_stars
    from {{ ref('repo_daily') }}, latest
    where dt between d - 7 and d - 1
    group by repo_id
)
select
    (select d from latest) as dt,
    t.repo_id,
    t.repo_name,
    t.stars,
    round(coalesce(b.avg_stars, 0), 2) as avg_stars_prev_7d,
    round(t.stars - coalesce(b.avg_stars, 0), 2) as delta
from today t
left join before b using (repo_id)
order by delta desc
limit 100
