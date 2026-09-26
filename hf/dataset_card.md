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

- `silver/dt=YYYY-MM-DD/hour=H/events.parquet`: one row per event, deduplicated on `event_id`.
- `gold/*.parquet`: daily and hourly aggregates. `gold/stats.json` has the running totals.

`catalog.duckdb` holds views (`silver.events`, `gold.*`) over the Parquet files, rebuilt every run:

```sql
attach 'hf://datasets/aishitdua/ghlake/catalog.duckdb' as ghlake;
select repo_name, stars_7d from ghlake.gold.star_velocity limit 10;
-- filter silver on dt (and hour) so DuckDB reads only those files
select event_type, count(*) from ghlake.silver.events where dt = '2026-09-25' group by 1;
```
