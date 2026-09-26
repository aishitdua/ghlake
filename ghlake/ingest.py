import argparse
import gzip
import shutil
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import duckdb
import requests
import urllib3.util.connection

# data.gharchive.org stalls ~2 min on hosts with broken IPv6 routes
urllib3.util.connection.HAS_IPV6 = False

DATA = Path("data")
STATE = "state/ghlake.duckdb"
URL = "https://data.gharchive.org/{:%Y-%m-%d}-{}.json.gz"
BRONZE_DAYS = 7

RUN_LOG_DDL = """
create table if not exists run_log (
    hour timestamp, rows_in bigint, rows_out bigint, seconds double,
    status varchar, logged_at timestamp
)"""

SILVER_SQL = """
copy (
    select
        cast(id as bigint) as event_id,
        type as event_type,
        cast(created_at as timestamp) as created_at,
        actor.id as actor_id,
        actor.login as actor_login,
        repo.id as repo_id,
        repo.name as repo_name,
        org.login as org_login,
        payload->>'$.action' as action,
        payload->>'$.forkee.language' as language
    from read_json(?, format = 'newline_delimited', columns = {
        id: 'varchar', type: 'varchar', created_at: 'varchar',
        actor: 'struct(id bigint, login varchar)',
        repo: 'struct(id bigint, name varchar)',
        org: 'struct(login varchar)', payload: 'json'
    })
    qualify row_number() over (partition by id order by created_at) = 1
) to '{out}' (format parquet, compression zstd)
"""


class Missing(Exception):
    pass


def download(hour: datetime, bronze: Path) -> tuple[Path, int]:
    bronze.mkdir(parents=True, exist_ok=True)
    path = bronze / f"{hour:%Y-%m-%d}-{hour.hour}.json.gz"
    url = URL.format(hour, hour.hour)
    for attempt in range(3):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                if r.status_code == 404:
                    raise Missing(url)
                r.raise_for_status()
                expected = int(r.headers.get("Content-Length", -1))
                with open(path, "wb") as f:
                    f.writelines(r.iter_content(1 << 20))
            if expected >= 0 and path.stat().st_size != expected:
                raise OSError(f"size {path.stat().st_size} != {expected}")
            with gzip.open(path, "rb") as f:
                return path, sum(1 for _ in f)
        except (OSError, EOFError, requests.RequestException):
            if attempt == 2:
                raise
            time.sleep(5 * (attempt + 1))


def silver_path(data: Path, hour: datetime) -> Path:
    return data / "silver" / f"dt={hour:%Y-%m-%d}" / f"hour={hour.hour}" / "events.parquet"


def ingest_hour(con: duckdb.DuckDBPyConnection, hour: datetime, data: Path = DATA) -> str:
    start = time.monotonic()
    rows_in = rows_out = 0
    try:
        gz, rows_in = download(hour, data / "bronze")
        out = silver_path(data, hour)
        shutil.rmtree(out.parent, ignore_errors=True)
        out.parent.mkdir(parents=True)
        con.execute(SILVER_SQL.replace("{out}", str(out)), [str(gz)])
        rows_out = con.sql(f"select count(*) from '{out}'").fetchone()[0]
        status = "ok"
    except Missing:
        status = "missing"
    except (OSError, EOFError, ValueError, requests.RequestException, duckdb.Error) as e:
        print(f"{hour:%Y-%m-%dT%H} failed: {e!r}", file=sys.stderr)
        status = "failed"
    secs = round(time.monotonic() - start, 2)
    now = datetime.now(UTC).replace(tzinfo=None)
    row = [hour.replace(tzinfo=None), rows_in, rows_out, secs, status, now]
    con.execute("insert into run_log values (?, ?, ?, ?, ?, ?)", row)
    print(f"{hour:%Y-%m-%dT%H} {status} in={rows_in} out={rows_out} {secs}s", flush=True)
    return status


def loaded_hours(con: duckdb.DuckDBPyConnection) -> set[datetime]:
    rows = con.sql("select distinct hour from run_log where status = 'ok'").fetchall()
    return {r[0] for r in rows}


def hours_between(start: datetime, end: datetime) -> list[datetime]:
    n = int((end - start).total_seconds() // 3600)
    return [start + timedelta(hours=i) for i in range(n + 1)]


def prune_bronze(bronze: Path, now: datetime) -> None:
    cutoff = now.timestamp() - BRONZE_DAYS * 86400
    for f in bronze.glob("*.json.gz"):
        if f.stat().st_mtime < cutoff:
            f.unlink()


def run(hours: list[datetime], skip_loaded: bool, data: Path = DATA) -> int:
    (data / STATE).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(data / STATE))
    con.execute(RUN_LOG_DDL)
    done = loaded_hours(con) if skip_loaded else set()
    statuses = [ingest_hour(con, h, data) for h in hours if h.replace(tzinfo=None) not in done]
    con.close()
    prune_bronze(data / "bronze", datetime.now(UTC))
    return 1 if "failed" in statuses else 0


def parse_hour(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H").replace(tzinfo=UTC)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="ghlake.ingest")
    sub = p.add_subparsers(dest="cmd", required=True)
    one = sub.add_parser("hour", help="ingest one hour, replacing its partition")
    one.add_argument("hour", type=parse_hour, help="YYYY-MM-DDTHH (UTC)")
    bf = sub.add_parser("backfill", help="ingest hours in a range not yet loaded")
    bf.add_argument("start", type=parse_hour)
    bf.add_argument("end", type=parse_hour)
    last = sub.add_parser("recent", help="fill gaps in the last N complete hours")
    last.add_argument("--hours", type=int, default=6)
    a = p.parse_args(argv)

    if a.cmd == "hour":
        return run([a.hour], skip_loaded=False)
    if a.cmd == "backfill":
        return run(hours_between(a.start, a.end), skip_loaded=True)
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
    return run(hours_between(end - timedelta(hours=a.hours - 1), end), skip_loaded=True)


if __name__ == "__main__":
    sys.exit(main())
