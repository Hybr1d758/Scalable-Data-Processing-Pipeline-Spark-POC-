import argparse
import glob
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession

input_csv_path = '/Users/edwardjr/Downloads/upwork /Engineering/Scalable-Data-Processing-Pipeline-Spark-POC-/realtor-data.zip.csv'

def resolve_input_csv_path(explicit_input_path: Optional[str]) -> str:
    """Return the CSV path to read.

    Priority:
    1) Use the explicitly provided --input path if present.
    2) Otherwise, pick the first CSV under input_csv_path.

    Raises a FileNotFoundError with guidance if no CSV is found.
    """
    if explicit_input_path:
        return explicit_input_path

    project_root = Path(__file__).resolve().parents[1]
    default_glob = input_csv_path
    matches = sorted(glob.glob(str(default_glob)))
    if not matches:
        raise FileNotFoundError(
            f"No CSV files found under default path: {default_glob}. "
            f"Provide --input /absolute/path/to/file.csv"
        )
    return matches[0]


def create_spark(app_name: str) -> SparkSession:
    """Create (or reuse) a SparkSession with the given app name.

    Using getOrCreate() lets this play nicely whether you run via python or spark-submit.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def read_csv(
    spark: SparkSession,
    input_path: str,
    header: bool,
    infer_schema: bool,
    delimiter: str,
) -> DataFrame:
    """Read a CSV into a DataFrame with common options.

    - header: whether the first row contains column names
    - infer_schema: enable Spark's type inference (helpful for quick POCs)
    - delimiter: CSV separator (default comma)
    """
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .option("delimiter", delimiter)
        .csv(input_path)
    )


def main() -> None:
    # Parse simple CLI arguments so this script is easy to re-run with variations
    parser = argparse.ArgumentParser(description="Ingest a single CSV file with Spark.")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to a CSV file to ingest. If omitted, the first CSV under input_csv_path is used.",
    )
    parser.add_argument(
        "--take",
        type=int,
        default=100_000,
        help="Number of rows to keep using DataFrame.limit(). Use -1 to keep all rows.",
    )
    parser.add_argument(
        "--delimiter",
        type=str,
        default=",",
        help="CSV delimiter (default: ',').",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Set if the CSV has no header row.",
    )
    parser.add_argument(
        "--no-infer-schema",
        action="store_true",
        help="Disable schema inference (treat all columns as strings).",
    )

    args = parser.parse_args()

    # Resolve an input file (explicit path or first CSV under input_csv_path)
    input_csv_path = resolve_input_csv_path(args.input)
    # Create a Spark session
    spark = create_spark(app_name="IngestSingleCSV")

    print(f"Reading CSV: {input_csv_path}")
    # Read the CSV with the chosen options
    dataframe = read_csv(
        spark=spark,
        input_path=input_csv_path,
        header=not args.no_header,
        infer_schema=not args.no_infer_schema,
        delimiter=args.delimiter,
    )

    # Optionally limit to a smaller slice for quick local runs (e.g., 100k rows)
    if args.take is not None and args.take > 0:
        limited_dataframe = dataframe.limit(args.take)
    else:
        limited_dataframe = dataframe

    print("\nSchema:")
    limited_dataframe.printSchema()

    # Count rows after optional limiting to verify record volume
    total_rows = limited_dataframe.count()
    print(f"\nRow count (post-limit if applied): {total_rows}")

    # Show a few example records so you can visually confirm parsing
    print("\nSample rows:")
    for row in limited_dataframe.take(5):
        print(row)

    # Cleanly stop the Spark session (flushes logs and releases resources)
    spark.stop()


if __name__ == "__main__":
    main()


