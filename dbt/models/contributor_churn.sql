with first_seen as (
    select first_seen as dt, count(*) as n from {{ ref('contributor_seen') }} group by 1
),
last_seen as (
    select last_seen as dt, count(*) as n from {{ ref('contributor_seen') }} group by 1
)
select
    d.dt,
    d.active,
    coalesce(f.n, 0) as first_time,
    d.active - coalesce(f.n, 0) as returned,
    -- only final once a week has passed with no further activity
    case when d.dt <= (select max(dt) from {{ ref('contributors_daily') }}) - 7
        then coalesce(l.n, 0) end as churned
from {{ ref('contributors_daily') }} d
left join first_seen f using (dt)
left join last_seen l using (dt)
