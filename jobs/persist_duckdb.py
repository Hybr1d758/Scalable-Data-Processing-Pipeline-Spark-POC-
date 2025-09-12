import argparse
from pathlib import Path
from typing import List

import duckdb


DEFAULT_TABLES = {
    "routed": "data/processed/routed/parquet/*.parquet",
    "market_trends": "data/processed/aggregated/market_trends/parquet/*.parquet",
    "property_type_size": "data/processed/aggregated/property_type_size/parquet/*.parquet",
    "lot_size_influence": "data/processed/aggregated/lot_size_influence/parquet/*.parquet",
    "broker_performance": "data/processed/aggregated/broker_performance/parquet/*.parquet",
    "status_analysis": "data/processed/aggregated/status_analysis/parquet/*.parquet",
    "time_yearly": "data/processed/aggregated/time_yearly/parquet/*.parquet",
}


def refresh_tables(db_path: str, tables: List[str]) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(db_path)
    try:
        for t in tables:
            if t not in DEFAULT_TABLES:
                print(f"Skipping unknown table '{t}'. Known: {list(DEFAULT_TABLES.keys())}")
                continue
            pattern = DEFAULT_TABLES[t]
            sql = f"""
                CREATE OR REPLACE TABLE {t} AS
                SELECT * FROM parquet_scan('{pattern}')
            """
            print(f"Refreshing table '{t}' from {pattern}")
            con.execute(sql)
        con.commit()
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Persist Parquet outputs into a DuckDB file.")
    parser.add_argument("--db", type=str, default="data/warehouse.duckdb", help="Path to DuckDB file to create or update.")
    parser.add_argument("--tables", type=str, default="", help="Comma-separated list of tables to refresh.")
    parser.add_argument("--refresh-all", action="store_true", help="Refresh all known tables.")
    args = parser.parse_args()

    if args.refresh_all:
        tables = list(DEFAULT_TABLES.keys())
    else:
        tables = [t.strip() for t in args.tables.split(",") if t.strip()]
        if not tables:
            print("No tables specified; use --refresh-all or --tables ...")
            return

    refresh_tables(args.db, tables)
    print(f"Updated DuckDB at: {args.db}")


if __name__ == "__main__":
    main()


