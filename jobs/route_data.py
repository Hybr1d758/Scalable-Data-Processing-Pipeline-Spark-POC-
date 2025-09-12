import argparse
import glob
from pathlib import Path
from typing import List, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def resolve_input_paths(explicit_input: Optional[str]) -> List[str]:
    if explicit_input:
        p = Path(explicit_input)
        if p.is_dir():
            return [str(p)]
        return sorted(glob.glob(str(p)))

    project_root = Path(__file__).resolve().parents[1]
    # Default to the deduplicated parquet output from step 2
    default_dir = project_root / "data" / "processed" / "cleanse_dedup" / "parquet"
    if default_dir.exists():
        return [str(default_dir)]

    # Fallback to raw CSVs if step 2 wasn't run yet
    default_glob = project_root / "data" / "raw" / "usa-real-estate" / "*.csv"
    matches = sorted(glob.glob(str(default_glob)))
    if not matches:
        raise FileNotFoundError(
            f"No inputs found. Provide --input path or run step 2 to generate {default_dir}"
        )
    return matches


def read_inputs(spark: SparkSession, inputs: List[str]) -> DataFrame:
    if len(inputs) == 1 and Path(inputs[0]).is_dir():
        # Assume parquet directory
        return spark.read.parquet(inputs[0])
    # Otherwise, treat as CSVs
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputs)
    )


def _coalesce_existing(df: DataFrame, candidate_names: Sequence[str]) -> F.Column:
    existing = [name for name in candidate_names if name in df.columns]
    if not existing:
        return F.lit(None)
    return F.coalesce(*[F.col(name) for name in existing])


def apply_rules_preset_basic(df: DataFrame) -> DataFrame:
    # Handle differing column names across datasets
    price_col = _coalesce_existing(df, ["price", "list_price", "sold_price"]).cast("double")
    beds_col = _coalesce_existing(df, ["beds", "bedrooms", "bed"]).cast("int")
    sqft_col = _coalesce_existing(df, ["sqft", "sqft_living", "living_area", "house_size"]).cast("double")

    segment = (
        F.when(price_col > F.lit(1_000_000), F.lit("luxury"))
        .when((beds_col >= F.lit(3)) & (sqft_col >= F.lit(2000)), F.lit("family"))
        .otherwise(F.lit("standard"))
    )

    return df.withColumn("segment", segment)


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
    parser = argparse.ArgumentParser(description="Route data by deriving a segment column from decision rules.")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to inputs (parquet dir from step 2 or CSV glob). Defaults to step 2 parquet output if present.",
    )
    parser.add_argument(
        "--rules",
        type=str,
        default="preset_basic",
        choices=["preset_basic"],
        help="Which set of rules to apply (currently only 'preset_basic').",
    )
    parser.add_argument(
        "--take",
        type=int,
        default=100_000,
        help="Limit rows for local runs. Use -1 to process all.",
    )
    parser.add_argument(
        "--write-output",
        action="store_true",
        help="Write outputs under data/processed/routed/{parquet,csv}.",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=1,
        help="Coalesce partitions before write (for small local outputs). Use -1 to skip.",
    )

    args = parser.parse_args()

    spark = create_spark(app_name="RouteData")
    inputs = resolve_input_paths(args.input)
    df = read_inputs(spark, inputs)

    if args.take is not None and args.take > 0:
        df = df.limit(args.take)

    print("\nInput schema:")
    df.printSchema()

    if args.rules == "preset_basic":
        routed = apply_rules_preset_basic(df)
    else:
        raise ValueError(f"Unsupported rules preset: {args.rules}")

    print("\nRouted schema:")
    routed.printSchema()
    print(f"Routed count: {routed.count()}")

    if args.write_output:
        project_root = Path(__file__).resolve().parents[1]
        output_root = project_root / "data" / "processed" / "routed"
        coalesce = args.coalesce if args.coalesce is not None and args.coalesce > 0 else None
        write_outputs(routed, output_root, coalesce)
        print(f"\nWrote outputs to: {output_root}")

    spark.stop()


if __name__ == "__main__":
    main()


