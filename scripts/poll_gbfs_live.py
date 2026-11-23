import argparse
import time
from datetime import datetime, timedelta, timezone

from ingest_gbfs_live import fetch_gbfs_snapshots
from pipeline_common import RAW_DIR, configure_logging


def poll_gbfs_live(interval_seconds: int = 60, duration_minutes: int = 30):
    logger = configure_logging()
    started_at = datetime.now(timezone.utc)
    ends_at = started_at + timedelta(minutes=duration_minutes)
    poll_count = 0

    logger.info(
        "Starting GBFS polling interval_seconds=%s duration_minutes=%s",
        interval_seconds,
        duration_minutes,
    )

    while datetime.now(timezone.utc) < ends_at:
        poll_started_at = time.perf_counter()
        try:
            written_paths = fetch_gbfs_snapshots(RAW_DIR)
            poll_count += 1
            logger.info("GBFS poll complete poll_count=%s files=%s", poll_count, len(written_paths))
        except Exception:
            logger.exception("GBFS poll failed")

        elapsed = time.perf_counter() - poll_started_at
        sleep_seconds = max(interval_seconds - elapsed, 0)
        if datetime.now(timezone.utc) + timedelta(seconds=sleep_seconds) > ends_at:
            break
        time.sleep(sleep_seconds)

    logger.info("Finished GBFS polling poll_count=%s", poll_count)
    return {"rows_affected": poll_count, "message": f"Completed {poll_count} GBFS polls"}


def main():
    parser = argparse.ArgumentParser(description="Poll GBFS live APIs and save raw JSON snapshots only.")
    parser.add_argument("--interval-seconds", type=int, default=60)
    parser.add_argument("--duration-minutes", type=int, default=30)
    args = parser.parse_args()
    poll_gbfs_live(interval_seconds=args.interval_seconds, duration_minutes=args.duration_minutes)


if __name__ == "__main__":
    main()
