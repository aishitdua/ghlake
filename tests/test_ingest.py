import gzip
import json
from datetime import UTC, datetime

import duckdb

from ghlake import ingest

HOUR = datetime(2026, 1, 2, 3, tzinfo=UTC)


def event(i, repo=True):
    return {
        "id": str(i),
        "type": "WatchEvent",
        "actor": {"id": 7, "login": "a"},
        "repo": {"id": 9, "name": "o/r"} if repo else {},
        "payload": {"action": "started"},
        "public": True,
        "created_at": "2026-01-02T03:04:05Z",
    }


def fake_download(tmp_path, events):
    gz = tmp_path / "in.json.gz"
    with gzip.open(gz, "wt") as f:
        f.writelines(json.dumps(e) + "\n" for e in events)
    return lambda hour, bronze: (gz, len(events))


def test_hour_is_deduped_and_idempotent(tmp_path, monkeypatch):
    events = [event(1), event(1), event(2, repo=False)]
    monkeypatch.setattr(ingest, "download", fake_download(tmp_path, events))
    for _ in range(2):
        assert ingest.run([HOUR], skip_loaded=False, data=tmp_path) == 0

    files = list((tmp_path / "silver").rglob("*.parquet"))
    assert [f.relative_to(tmp_path).as_posix() for f in files] == [
        "silver/dt=2026-01-02/hour=3/events.parquet"
    ]
    rows = duckdb.sql(f"select event_id, repo_id from '{files[0]}' order by 1").fetchall()
    assert rows == [(1, 9), (2, None)]

    con = duckdb.connect(str(tmp_path / ingest.STATE))
    assert con.sql("select rows_in, rows_out, status from run_log").fetchall() == [
        (3, 2, "ok"),
        (3, 2, "ok"),
    ]


def test_backfill_skips_loaded_and_logs_missing(tmp_path, monkeypatch):
    def missing(hour, bronze):
        raise ingest.Missing("404")

    monkeypatch.setattr(ingest, "download", fake_download(tmp_path, [event(1)]))
    ingest.run([HOUR], skip_loaded=True, data=tmp_path)
    monkeypatch.setattr(ingest, "download", missing)
    hours = ingest.hours_between(HOUR, HOUR.replace(hour=4))
    assert ingest.run(hours, skip_loaded=True, data=tmp_path) == 0

    con = duckdb.connect(str(tmp_path / ingest.STATE))
    assert con.sql("select hour(hour), status from run_log order by logged_at").fetchall() == [
        (3, "ok"),
        (4, "missing"),
    ]
