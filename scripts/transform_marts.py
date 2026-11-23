import argparse
from pathlib import Path

from pipeline_common import BASE_DIR, configure_logging, get_connection, make_run_id, record_pipeline_event, run_sql_file, utc_now_naive


MART_SQL_DIR = BASE_DIR / "sql" / "marts"


def run_mart_transforms(sql_dir: Path = MART_SQL_DIR):
    logger = configure_logging()
    con = get_connection()
    try:
        sql_files = sorted(sql_dir.glob("*.sql"))
        if not sql_files:
            raise FileNotFoundError(f"No mart SQL files found in {sql_dir}")

        con.execute("BEGIN")
        try:
            for sql_file in sql_files:
                logger.info("Running mart SQL: %s", sql_file)
                run_sql_file(con, sql_file)
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

        table_count = con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'mart'
              AND table_type = 'BASE TABLE'
            """
        ).fetchone()[0]
    finally:
        con.close()
    return {"rows_affected": table_count, "message": f"Ran {len(sql_files)} mart SQL files"}


def main():
    parser = argparse.ArgumentParser(description="Run mart transformations.")
    parser.add_argument("--sql-dir", type=Path, default=MART_SQL_DIR)
    args = parser.parse_args()

    logger = configure_logging()
    run_id = make_run_id()
    con = get_connection()
    started_at = utc_now_naive()
    try:
        result = run_mart_transforms(args.sql_dir)
        record_pipeline_event(
            con,
            run_id,
            "transform_marts",
            "mart",
            str(args.sql_dir),
            "mart.*",
            "SUCCESS",
            rows_affected=result["rows_affected"],
            started_at_utc=started_at,
            ended_at_utc=utc_now_naive(),
            message=result["message"],
        )
    except Exception as exc:
        logger.exception("Mart transforms failed")
        record_pipeline_event(
            con,
            run_id,
            "transform_marts",
            "mart",
            str(args.sql_dir),
            "mart.*",
            "FAILED",
            started_at_utc=started_at,
            ended_at_utc=utc_now_naive(),
            error_message=str(exc),
        )
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
