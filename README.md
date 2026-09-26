# ghlake

An hourly lakehouse over [GH Archive](https://www.gharchive.org), the public record of every GitHub event. Runs on its own on GitHub Actions, costs nothing.

- **Dashboard:** https://huggingface.co/spaces/aishitdua/ghlake
- **Dataset:** https://huggingface.co/datasets/aishitdua/ghlake
- **Pipeline runs:** https://github.com/aishitdua/ghlake/actions/workflows/hourly.yml

[![hourly](https://github.com/aishitdua/ghlake/actions/workflows/hourly.yml/badge.svg)](https://github.com/aishitdua/ghlake/actions/workflows/hourly.yml)
![events](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Faishitdua%2Fghlake%2Fresolve%2Fmain%2Fgold%2Fstats.json&query=%24.events_fmt&label=events%20processed)
![hours](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Faishitdua%2Fghlake%2Fresolve%2Fmain%2Fgold%2Fstats.json&query=%24.hours_loaded&label=hours%20loaded)
![newest](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Faishitdua%2Fghlake%2Fresolve%2Fmain%2Fgold%2Fstats.json&query=%24.newest_hour&label=newest%20hour)
![last run](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fhuggingface.co%2Fdatasets%2Faishitdua%2Fghlake%2Fresolve%2Fmain%2Fgold%2Fstats.json&query=%24.last_run&label=last%20run)

Query it from anywhere with DuckDB, no key needed. The dataset publishes a small catalog of views over its Parquet files:

```sql
attach 'hf://datasets/aishitdua/ghlake/catalog.duckdb' as ghlake;
select repo_name, stars_7d from ghlake.gold.star_velocity limit 10;
select event_type, count(*) from ghlake.silver.events where dt = '2026-09-25' group by 1;
```

## How it works

```
data.gharchive.org/YYYY-MM-DD-H.json.gz      (one file per hour)
  │ ghlake/ingest.py: download, check size + gzip CRC, count rows
  ├─► bronze: raw .json.gz, kept 7 days as an Actions artifact
  ├─► silver: silver/dt=…/hour=…/events.parquet, one row per event_id (DuckDB)
  │ dbt-duckdb (dbt/models): incremental, recomputes only the days touched
  ├─► gold: repo_daily, event_type_hourly, language_daily, star_velocity,
  │         top_movers, contributor_churn, run_log
  │ ghlake/publish.py: push silver + gold + dbt state to the HF dataset,
  │   then catalog.duckdb: views silver.events, gold.* over those files
  └─► static HF Space: DuckDB-WASM in the browser reads gold from the dataset
```

Every hour at :23, [hourly.yml](.github/workflows/hourly.yml) pulls the dbt state and the silver days it will touch from the dataset, loads any of the last 6 hours not yet loaded, runs `dbt build` (models plus grain, not-null and a 3-hour freshness test), and pushes the result. A concurrency group keeps runs from overlapping. Re-running an hour replaces its partition. To backfill, run the workflow by hand with a start and end hour.

Language comes only from fork events, because GH Archive payloads no longer carry a repo language anywhere else. Contributors are non-bot actors who push, open PRs or issues, or review; "first time" means first seen since the lake started, "returned" means seen before.

The HF dataset doubles as the warehouse: gold is plain Parquet that DuckDB, pandas or Spark can read over HTTPS. The dashboard is a static page that runs DuckDB-WASM in your browser against those files, so there is no server to pay for.

## Run it locally

Needs [uv](https://docs.astral.sh/uv/).

```
make demo                      # loads yesterday 12:00 UTC, builds gold, serves the dashboard
make demo HOUR=2026-09-20T08   # any other hour
make check                     # ruff + pytest
```

Everything lands in `data/`. Backfill a range with `uv run python -m ghlake.ingest backfill 2026-09-20T00 2026-09-20T23`.
