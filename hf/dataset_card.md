---
license: cc-by-4.0
pretty_name: ghlake
configs:
  - config_name: repo_daily
    data_files: gold/repo_daily/*/*.parquet
  - config_name: event_type_hourly
    data_files: gold/event_type_hourly.parquet
  - config_name: run_log
    data_files: gold/run_log.parquet
---

# ghlake

Every public GitHub event from [GH Archive](https://www.gharchive.org), loaded hourly by
[github.com/aishitdua/ghlake](https://github.com/aishitdua/ghlake).

- `silver/dt=YYYY-MM-DD/hour=HH/events.parquet`: one row per event, deduplicated on `event_id`.
- `gold/*.parquet`: daily and hourly aggregates. `gold/stats.json` has the running totals.

```sql
select * from 'hf://datasets/aishitdua/ghlake/gold/star_velocity.parquet' limit 10;

-- silver: glob one day at a time; a glob over every hour folder hits the Hub's API rate limit
select event_type, count(*) from 'hf://datasets/aishitdua/ghlake/silver/dt=2026-09-25/*/*.parquet' group by 1;
```
