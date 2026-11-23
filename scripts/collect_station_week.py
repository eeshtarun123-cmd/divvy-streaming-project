"""Capture a bounded week of station snapshots under the local scheduler."""
import argparse
import fcntl
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from ingest_gbfs_live import GBFS_FEEDS, fetch_gbfs_snapshots
from pipeline_common import BASE_DIR, RAW_DIR

INTERVAL = 300
SLOTS = 7 * 24 * 60 * 60 // INTERVAL
DAY_SLOTS = 24 * 60 * 60 // INTERVAL


def timestamp(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def tick(state, start, scheduler_file):
    current = int(datetime.now(timezone.utc).timestamp())
    slot = (current - start) // INTERVAL
    if slot < 0:
        return
    end = min(slot, SLOTS)
    state.executemany(
        "INSERT OR IGNORE INTO polls VALUES (?, ?, 'missed', ?, NULL)",
        ((i, timestamp(start + i * INTERVAL), 'No poll recorded in interval')
         for i in range(end)),
    )
    state.commit()

    batch = min(slot // DAY_SLOTS, 7)
    loaded = state.execute(
        "SELECT 1 FROM loads WHERE batch = ? AND success = 1", (batch,)
    ).fetchone()
    last_attempt = state.execute(
        "SELECT max(attempted_at) FROM loads WHERE batch = ?", (batch,)
    ).fetchone()[0]
    load_due = not loaded and (last_attempt is None or current - last_attempt >= 1800)
    if slot < SLOTS and not state.execute(
        'SELECT 1 FROM polls WHERE slot = ?', (slot,)
    ).fetchone():
        detail = ''
        snapshot_path = None
        try:
            with tempfile.TemporaryDirectory(dir=RAW_DIR) as scratch:
                paths = fetch_gbfs_snapshots(
                    Path(scratch), feed_names=('station_status',)
                )
                status = next(path for path in paths if path.parent.name == 'station_status')
                payload = json.loads(status.read_text())
                feed = payload['raw_response']
                if not feed['data']['stations']:
                    raise ValueError('Empty station status feed')
                captured = datetime.fromisoformat(payload['ingested_at_utc']).timestamp()
                if abs(captured - int(feed['last_updated'])) > INTERVAL:
                    raise ValueError('Station feed timestamp is more than five minutes from capture time')
                for path in paths:
                    destination = RAW_DIR / path.parent.name / path.name
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    path.replace(destination)
                snapshot_path = str(RAW_DIR / 'station_status' / status.name)
        except Exception as exc:
            detail = str(exc)
        state.execute(
            'INSERT INTO polls VALUES (?, ?, ?, ?, ?)',
            (slot, timestamp(start + slot * INTERVAL),
             'captured' if snapshot_path else 'missed', detail, snapshot_path),
        )
        state.commit()
        print(f'Slot {slot + 1}/{SLOTS}: {snapshot_path or detail}', flush=True)

    if load_due:
        with (BASE_DIR / 'logs' / 'station_week_loads.log').open('a') as log:
            print(f'\nBatch {batch} at {timestamp(current)}', file=log, flush=True)
            try:
                # The five-minute samples are recorded independently of these feeds.
                fetch_gbfs_snapshots(
                    feed_names=tuple(name for name in GBFS_FEEDS if name != 'station_status')
                )
            except Exception as exc:
                print(f'Companion feed refresh failed: {exc}', file=log, flush=True)
            result = subprocess.run(
                [sys.executable, 'scripts/run_pipeline.py'],
                cwd=BASE_DIR, stdout=log, stderr=subprocess.STDOUT,
                env={**os.environ, "DIVVY_SCHEDULED_LOAD": "1"},
            )
        state.execute('INSERT INTO loads VALUES (?, ?, ?)', (batch, current, result.returncode == 0))
        state.commit()
        loaded = result.returncode == 0
        print(f'Warehouse batch {batch}: exit {result.returncode}', flush=True)

    if slot >= SLOTS and loaded:
        print('Collection complete:', state.execute(
            'SELECT status, count(*) FROM polls GROUP BY status'
        ).fetchall(), flush=True)
        scheduler_file.unlink(missing_ok=True)
        subprocess.run(['/bin/launchctl', 'bootout', f'gui/{os.getuid()}/com.divvy.station-week'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', type=int, required=True)
    parser.add_argument('--scheduler-file', type=Path, required=True)
    args = parser.parse_args()
    with (BASE_DIR / 'logs' / 'station_week.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return
        with sqlite3.connect(BASE_DIR / 'logs' / f'station_week_{args.start}.sqlite3') as state:
            state.execute('CREATE TABLE IF NOT EXISTS polls(slot INTEGER PRIMARY KEY, scheduled_utc TEXT, status TEXT, detail TEXT, snapshot_path TEXT)')
            state.execute('CREATE TABLE IF NOT EXISTS loads(batch INTEGER, attempted_at INTEGER, success INTEGER)')
            tick(state, args.start, args.scheduler_file)


if __name__ == '__main__':
    main()
