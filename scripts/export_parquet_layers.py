import argparse
from pathlib import Path

from pipeline_common import PROCESSED_DIR, configure_logging, get_connection, quote_ident


def export_table_to_parquet(schema_name: str, table_name: str, output_root: Path = PROCESSED_DIR):
    logger = configure_logging()
    output_dir = output_root / schema_name
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{table_name}.parquet"
    qualified = f"{quote_ident(schema_name)}.{quote_ident(table_name)}"
    con = get_connection()
    try:
        logger.info("Exporting %s to %s", qualified, target)
        con.execute(
            f"""
            COPY (SELECT * FROM {qualified})
            TO ?
            (FORMAT PARQUET, COMPRESSION ZSTD)
            """,
            [str(target)],
        )
    finally:
        con.close()
    return {"rows_affected": 1, "message": f"Exported {qualified} to Parquet"}


def export_schema_to_parquet(schema_name: str, output_root: Path = PROCESSED_DIR):
    logger = configure_logging()
    output_dir = output_root / schema_name
    output_dir.mkdir(parents=True, exist_ok=True)
    con = get_connection()
    try:
        tables = con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = ?
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            [schema_name],
        ).fetchall()

        exported = 0
        for (table_name,) in tables:
            target = output_dir / f"{table_name}.parquet"
            qualified = f"{quote_ident(schema_name)}.{quote_ident(table_name)}"
            logger.info("Exporting %s to %s", qualified, target)
            con.execute(
                f"""
                COPY (SELECT * FROM {qualified})
                TO ?
                (FORMAT PARQUET, COMPRESSION ZSTD)
                """,
                [str(target)],
            )
            exported += 1
    finally:
        con.close()
    return {"rows_affected": exported, "message": f"Exported {exported} {schema_name} tables to Parquet"}


def export_all_layers(output_root: Path = PROCESSED_DIR):
    total = 0
    messages = []
    for schema_name in ["raw", "staging", "mart"]:
        result = export_schema_to_parquet(schema_name, output_root)
        total += result["rows_affected"]
        messages.append(result["message"])
    return {"rows_affected": total, "message": " | ".join(messages)}


def main():
    parser = argparse.ArgumentParser(description="Export DuckDB raw/staging/mart tables as Parquet files.")
    parser.add_argument("--output-root", type=Path, default=PROCESSED_DIR)
    parser.add_argument("--schema", choices=["raw", "staging", "mart", "all"], default="all")
    args = parser.parse_args()

    if args.schema == "all":
        export_all_layers(args.output_root)
    else:
        export_schema_to_parquet(args.schema, args.output_root)


if __name__ == "__main__":
    main()
