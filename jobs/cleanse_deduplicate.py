import argparse
import glob
from pathlib import Path
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def resolve_input_paths(explicit_input_glob: Optional[str]) -> List[str]:
    if explicit_input_glob:
        return sorted(glob.glob(explicit_input_glob))

    project_root = Path(__file__).resolve().parents[1]
    default_glob = project_root / "data" / "raw" / "usa-real-estate" / "*.csv"
    matches = sorted(glob.glob(str(default_glob)))
    if not matches:
        raise FileNotFoundError(
            f"No CSV files found under default path: {default_glob}. "
            f"Provide --input '/absolute/or/relative/glob/*.csv' and --keys key1,key2"
        )
    return matches


def read_csvs(
    spark: SparkSession,
    paths: List[str],
    header: bool,
    infer_schema: bool,
    delimiter: str,
) -> DataFrame:
    return (
        spark.read
        .option("header", str(header).lower())
        .option("inferSchema", str(infer_schema).lower())
        .option("delimiter", delimiter)
        .csv(paths)
    )


def trim_string_columns(df: DataFrame) -> DataFrame:
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    if not string_cols:
        return df
    return df.select(
        *[F.trim(F.col(c)).alias(c) if c in string_cols else F.col(c) for c in df.columns]
    )


def deduplicate_by_join(df: DataFrame, key_columns: List[str]) -> DataFrame:
    if not key_columns:
        raise ValueError("--keys must be provided as a comma-separated list")

    # Assign a synthetic id per row for deterministic selection within groups
    df_with_id = df.withColumn("__row_id", F.monotonically_increasing_id())

    # Compute the single representative id per key group
    winners = (
        df_with_id
        .groupBy(*[F.col(k) for k in key_columns])
        .agg(F.min(F.col("__row_id")).alias("__row_id"))
    )

    # Join back to keep exactly one row per key
    deduped = df_with_id.join(winners, on=key_columns + ["__row_id"], how="inner")
    return deduped.drop("__row_id")


def normalize_string_columns(
    df: DataFrame,
    lowercase: bool,
    normalize_spaces: bool,
    strip_punct: bool,
) -> DataFrame:
    """Apply optional, basic text normalization to all string columns.

    - lowercase: convert to lower case
    - normalize_spaces: collapse multiple spaces to one, and trim
    - strip_punct: remove non-word, non-space characters (keeps letters, digits, underscore, space)
    """
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    if not string_cols:
        return df

    def transform(col):
        x = col
        if lowercase:
            x = F.lower(x)
        if normalize_spaces:
            x = F.regexp_replace(x, r"\s+", " ")
            x = F.trim(x)
        if strip_punct:
            x = F.regexp_replace(x, r"[^\w\s]", "")
        return x

    return df.select(
        *[transform(F.col(c)).alias(c) if c in string_cols else F.col(c) for c in df.columns]
    )


def detect_sensible_keys(df: DataFrame) -> Tuple[List[str], str]:
    """Try to auto-detect reasonable deduplication keys based on common real-estate columns.

    Returns (keys, strategy_description).
    """
    columns_lower = {c.lower(): c for c in df.columns}

    # Priority 1: parcel/unique property identifiers
    candidates = [
        "parcel_number", "parcelnumber", "parcelid", "apn", "property_id", "mlsnumber", "mls_id", "id"
    ]
    for cand in candidates:
        if cand in columns_lower:
            return [columns_lower[cand]], f"auto: unique id '{columns_lower[cand]}'"

    # Priority 2: address + zipcode
    if "address" in columns_lower and "zipcode" in columns_lower:
        return [columns_lower["address"], columns_lower["zipcode"]], "auto: address + zipcode"

    # Priority 3: address + city + state
    if all(x in columns_lower for x in ["address", "city", "state"]):
        return [columns_lower["address"], columns_lower["city"], columns_lower["state"]], "auto: address + city + state"

    raise ValueError("Could not auto-detect sensible keys. Please provide --keys col1,col2 explicitly.")


def write_outputs(df: DataFrame, output_root: Path, coalesce: Optional[int]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    target_parquet = str(output_root / "parquet")
    target_csv = str(output_root / "csv")

    to_write = df
    if coalesce is not None and coalesce > 0:
        to_write = to_write.coalesce(coalesce)

    to_write.write.mode("overwrite").parquet(target_parquet)
    to_write.write.mode("overwrite").option("header", "true").csv(target_csv)


def main() -> None:
    parser = argparse.ArgumentParser(description="Cleanse & Deduplicate using JOIN-based method.")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Glob or path to CSVs (e.g., 'data/raw/usa-real-estate/*.csv'). Defaults to that location if omitted.",
    )
    parser.add_argument(
        "--keys",
        type=str,
        required=False,
        help="Comma-separated key columns to identify duplicates (e.g., 'parcel_number,address').",
    )
    parser.add_argument(
        "--auto-keys",
        action="store_true",
        help="Attempt to auto-detect sensible keys if --keys is not provided.",
    )
    parser.add_argument(
        "--take",
        type=int,
        default=100_000,
        help="Limit rows for local runs. Use -1 to process all.",
    )
    parser.add_argument(
        "--delimiter",
        type=str,
        default=",",
        help="CSV delimiter (default ',').",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Set if CSV has no header row.",
    )
    parser.add_argument(
        "--no-infer-schema",
        action="store_true",
        help="Disable schema inference (read all as strings).",
    )
    parser.add_argument(
        "--no-trim",
        action="store_true",
        help="Disable trimming of leading/trailing whitespace for string columns.",
    )
    parser.add_argument(
        "--lowercase",
        action="store_true",
        help="Lowercase all string columns before deduplication.",
    )
    parser.add_argument(
        "--normalize-spaces",
        action="store_true",
        help="Collapse multiple spaces to one and trim string columns.",
    )
    parser.add_argument(
        "--strip-punct",
        action="store_true",
        help="Remove punctuation from string columns (keeps letters, digits, underscore, space).",
    )
    parser.add_argument(
        "--write-output",
        action="store_true",
        help="If set, writes deduplicated output to data/processed/cleanse_dedup/{parquet,csv}.",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=1,
        help="Coalesce partitions before write (for small local outputs). Use -1 to skip.",
    )

    args = parser.parse_args()

    spark = create_spark(app_name="CleanseDeduplicate")

    # Resolve inputs and read
    input_paths = resolve_input_paths(args.input)
    df = read_csvs(
        spark=spark,
        paths=input_paths,
        header=not args.no_header,
        infer_schema=not args.no_infer_schema,
        delimiter=args.delimiter,
    )

    if args.take is not None and args.take > 0:
        df = df.limit(args.take)

    print("\nInput schema:")
    df.printSchema()
    print(f"Input count (post-limit if applied): {df.count()}")

    # Cleanse: trim strings unless disabled
    if not args.no_trim:
        df = trim_string_columns(df)

    # Optional normalization for better matching
    if args.lowercase or args.normalize_spaces or args.strip_punct:
        df = normalize_string_columns(
            df,
            lowercase=args.lowercase,
            normalize_spaces=args.normalize_spaces,
            strip_punct=args.strip_punct,
        )

    # Deduplicate using JOIN-based approach
    key_columns: List[str] = []
    if args.keys:
        key_columns = [c.strip() for c in args.keys.split(",") if c.strip()]
    elif args.auto_keys:
        key_columns, strategy = detect_sensible_keys(df)
        print(f"Auto-detected keys: {key_columns} ({strategy})")
    else:
        raise ValueError("--keys is required unless --auto-keys is provided.")
    deduped = deduplicate_by_join(df, key_columns)

    print("\nDeduplicated schema:")
    deduped.printSchema()
    print(f"Deduplicated count: {deduped.count()}")

    # Optionally write outputs for downstream steps
    if args.write_output:
        project_root = Path(__file__).resolve().parents[1]
        output_root = project_root / "data" / "processed" / "cleanse_dedup"
        coalesce = args.coalesce if args.coalesce is not None and args.coalesce > 0 else None
        write_outputs(deduped, output_root, coalesce)
        print(f"\nWrote outputs to: {output_root}")

    spark.stop()


if __name__ == "__main__":
    main()


