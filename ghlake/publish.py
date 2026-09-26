import argparse
import json
import os
import shutil
import sys
from datetime import date, timedelta
from pathlib import Path

import duckdb
from huggingface_hub import HfApi, hf_hub_download, snapshot_download
from huggingface_hub.errors import EntryNotFoundError, RepositoryNotFoundError

from ghlake.ingest import DATA, STATE, parse_hour

REPO = os.environ.get("GHLAKE_HF_REPO", "aishitdua/ghlake")
GOLD_TABLES = [
    "event_type_hourly",
    "language_daily",
    "contributors_daily",
    "contributor_churn",
    "star_velocity",
    "top_movers",
    "run_log",
]
LATEST_RUNS = """
select * from run_log
qualify row_number() over (partition by hour order by logged_at desc) = 1
"""


def pull(start: date, end: date, data: Path = DATA) -> None:
    try:
        hf_hub_download(REPO, STATE, repo_type="dataset", local_dir=data)
    except (EntryNotFoundError, RepositoryNotFoundError):
        print("no remote state yet, starting fresh")
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    snapshot_download(
        REPO,
        repo_type="dataset",
        local_dir=data,
        allow_patterns=[f"silver/dt={d}/*" for d in days],
    )


def export(data: Path = DATA) -> dict:
    gold = data / "gold"
    shutil.rmtree(gold, ignore_errors=True)
    (gold / "repo_daily").mkdir(parents=True)
    con = duckdb.connect(str(data / STATE), read_only=True)
    for t in GOLD_TABLES:
        con.execute(f"copy {t} to '{gold}/{t}.parquet' (format parquet, compression zstd)")
    con.execute(
        f"copy (from repo_daily where dt >= current_date - 30) to '{gold}/repo_daily_30d.parquet' "
        "(format parquet, compression zstd)"
    )
    # only days present locally changed this run; older partitions stay as they are on the hub
    for d in sorted(p.name.removeprefix("dt=") for p in (data / "silver").glob("dt=*")):
        out = gold / "repo_daily" / f"dt={d}"
        out.mkdir()
        con.execute(
            f"copy (select * exclude (dt) from repo_daily where dt = '{d}') "
            f"to '{out}/data.parquet' (format parquet, compression zstd)"
        )
    events, hours, newest, last_run, status = con.sql(f"""
        with r as ({LATEST_RUNS})
        select
            coalesce(sum(rows_out) filter (status = 'ok'), 0),
            count(*) filter (status = 'ok'),
            strftime(max(hour) filter (status = 'ok'), '%Y-%m-%d %H:00 UTC'),
            strftime(max(logged_at), '%Y-%m-%d %H:%M UTC'),
            arg_max(status, logged_at)
        from r
    """).fetchone()
    con.close()
    stats = {
        "events": events,
        "events_fmt": f"{events:,}",
        "hours_loaded": hours,
        "newest_hour": newest,
        "last_run": last_run,
        "last_status": status,
    }
    (gold / "stats.json").write_text(json.dumps(stats, indent=1))
    return stats


def push(data: Path = DATA) -> None:
    stats = export(data)
    print(stats)
    HfApi().upload_folder(
        repo_id=REPO,
        repo_type="dataset",
        folder_path=data,
        allow_patterns=["silver/**", "gold/**", STATE],
        commit_message=f"load through {stats['newest_hour']}",
    )


def deploy() -> None:
    api = HfApi()
    api.create_repo(REPO, repo_type="dataset", exist_ok=True)
    api.upload_file(
        path_or_fileobj="hf/dataset_card.md",
        path_in_repo="README.md",
        repo_id=REPO,
        repo_type="dataset",
    )
    api.create_repo(REPO, repo_type="space", space_sdk="static", exist_ok=True)
    api.upload_folder(repo_id=REPO, repo_type="space", folder_path="app")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ghlake.publish")
    sub = p.add_subparsers(dest="cmd", required=True)
    pl = sub.add_parser("pull", help="fetch state and the silver days a run will touch")
    pl.add_argument("start", type=parse_hour)
    pl.add_argument("end", type=parse_hour)
    sub.add_parser("export", help="write gold parquet and stats.json locally")
    sub.add_parser("push", help="export, then upload silver, gold and state")
    sub.add_parser("deploy", help="create the dataset and space, push the card and the app")
    a = p.parse_args(argv)
    if a.cmd == "pull":
        pull(a.start.date(), a.end.date())
    elif a.cmd == "export":
        print(export())
    elif a.cmd == "push":
        push()
    else:
        deploy()
    return 0


if __name__ == "__main__":
    sys.exit(main())
