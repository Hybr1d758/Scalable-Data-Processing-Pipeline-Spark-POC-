import argparse
import glob
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def create_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )


def resolve_inputs(explicit_input: Optional[str]) -> List[str]:
    if explicit_input:
        p = Path(explicit_input)
        if p.is_dir():
            return [str(p)]
        return sorted(glob.glob(str(p)))

    project_root = Path(__file__).resolve().parents[1]
    default_dir = project_root / "data" / "processed" / "routed" / "parquet"
    if default_dir.exists():
        return [str(default_dir)]
    default_dir2 = project_root / "data" / "processed" / "cleanse_dedup" / "parquet"
    if default_dir2.exists():
        return [str(default_dir2)]

    default_glob = project_root / "data" / "raw" / "usa-real-estate" / "*.csv"
    matches = sorted(glob.glob(str(default_glob)))
    if not matches:
        raise FileNotFoundError(
            f"No inputs found. Provide --input or generate processed data first."
        )
    return matches


def read_dataset(spark: SparkSession, inputs: List[str]) -> DataFrame:
    if len(inputs) == 1 and Path(inputs[0]).is_dir():
        # assume parquet directory
        return spark.read.parquet(inputs[0])
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputs)
    )


def parse_metrics(metrics_arg: str) -> List[Tuple[str, str]]:
    """Parse metrics specification like "price:sum,price:max,house_size:avg"."""
    if not metrics_arg:
        raise ValueError("--metrics is required; format col:agg[,col:agg]")
    pairs: List[Tuple[str, str]] = []
    for part in metrics_arg.split(","):
        part = part.strip()
        if not part:
            continue
        if ":" not in part:
            raise ValueError(f"Invalid metric segment '{part}'. Use col:agg format.")
        col, agg = part.split(":", 1)
        col = col.strip()
        agg = agg.strip().lower()
        if not col or not agg:
            raise ValueError(f"Invalid metric segment '{part}'. Empty col/agg.")
        if agg not in {"sum", "min", "max", "avg", "mean", "count"}:
            raise ValueError(f"Unsupported aggregation '{agg}' for '{col}'.")
        pairs.append((col, "avg" if agg == "mean" else agg))
    if not pairs:
        raise ValueError("No valid metrics parsed from --metrics.")
    return pairs


def build_aggregations(df: DataFrame, metrics: Sequence[Tuple[str, str]]) -> List[F.Column]:
    agg_exprs: List[F.Column] = []
    for col, agg in metrics:
        c = F.col(col).cast("double") if col in df.columns else F.lit(None)
        alias = f"{col}_{agg}"
        if agg == "sum":
            agg_exprs.append(F.sum(c).alias(alias))
        elif agg == "min":
            agg_exprs.append(F.min(c).alias(alias))
        elif agg == "max":
            agg_exprs.append(F.max(c).alias(alias))
        elif agg == "avg":
            agg_exprs.append(F.avg(c).alias(alias))
        elif agg == "count":
            agg_exprs.append(F.count(c).alias(alias))
        else:
            raise ValueError(f"Unsupported aggregation: {agg}")
    return agg_exprs


def _coalesce_existing(df: DataFrame, candidate_names: Sequence[str]) -> F.Column:
    existing = [name for name in candidate_names if name in df.columns]
    if not existing:
        return F.lit(None)
    return F.coalesce(*[F.col(name) for name in existing])


def _get_price_col(df: DataFrame) -> F.Column:
    return _coalesce_existing(df, ["price", "list_price", "sold_price"]).cast("double")


def _get_house_size_col(df: DataFrame) -> F.Column:
    return _coalesce_existing(df, ["house_size", "sqft", "sqft_living", "living_area"]).cast("double")


def _get_bed_col(df: DataFrame) -> F.Column:
    return _coalesce_existing(df, ["bed", "beds", "bedrooms"]).cast("int")


def _get_bath_col(df: DataFrame) -> F.Column:
    return _coalesce_existing(df, ["bath", "baths", "bathrooms"]).cast("int")


def _get_acre_lot_col(df: DataFrame) -> F.Column:
    return _coalesce_existing(df, ["acre_lot", "lot_size_acres", "acres"]).cast("double")


def _get_dim_col(df: DataFrame, name: str) -> F.Column:
    return F.col(name) if name in df.columns else F.lit(None).alias(name)


def _parse_year_from_date(df: DataFrame, date_col_name: str = "prev_sold_date") -> F.Column:
    # Try a few common formats; fall back to null
    candidates = [
        F.to_date(F.col(date_col_name), "yyyy-MM-dd"),
        F.to_date(F.col(date_col_name), "MM/dd/yyyy"),
        F.to_date(F.col(date_col_name), "M/d/yyyy"),
        F.to_date(F.col(date_col_name), "yyyy/MM/dd"),
    ] if date_col_name in df.columns else []
    if not candidates:
        return F.lit(None)
    parsed = candidates[0]
    for c in candidates[1:]:
        parsed = F.coalesce(parsed, c)
    return F.year(parsed)


def run_preset_market_trends(df: DataFrame) -> DataFrame:
    price = _get_price_col(df)
    house = _get_house_size_col(df)
    city = _get_dim_col(df, "city")
    state = _get_dim_col(df, "state")
    return (
        df.groupBy(city.alias("city"), state.alias("state"))
        .agg(
            F.avg(price).alias("avg_price"),
            F.percentile_approx(price, 0.5, 100).alias("median_price"),
            F.count(F.lit(1)).alias("num_properties"),
            F.avg(house).alias("avg_house_size"),
        )
    )


def run_preset_property_type_size(df: DataFrame) -> DataFrame:
    price = _get_price_col(df)
    bed = _get_bed_col(df)
    bath = _get_bath_col(df)
    return (
        df.groupBy(bed.alias("bed"), bath.alias("bath"))
        .agg(
            F.avg(price).alias("avg_price"),
            F.min(price).alias("min_price"),
            F.max(price).alias("max_price"),
            F.count(F.lit(1)).alias("num_properties"),
        )
    )


def run_preset_lot_size_influence(df: DataFrame) -> DataFrame:
    price = _get_price_col(df)
    house = _get_house_size_col(df)
    acres = _get_acre_lot_col(df)
    # Bucket: <0.25 small, 0.25-1 medium, >1 large
    lot_bucket = (
        F.when(acres < F.lit(0.25), F.lit("small"))
        .when((acres >= F.lit(0.25)) & (acres <= F.lit(1.0)), F.lit("medium"))
        .when(acres > F.lit(1.0), F.lit("large"))
        .otherwise(F.lit("unknown"))
    )
    return (
        df.groupBy(lot_bucket.alias("lot_bucket"))
        .agg(
            F.avg(price).alias("avg_price"),
            F.avg(house).alias("avg_house_size"),
        )
    )


def run_preset_broker_performance(df: DataFrame) -> DataFrame:
    price = _get_price_col(df)
    broker = _get_dim_col(df, "brokered_by")
    return (
        df.groupBy(broker.alias("brokered_by"))
        .agg(
            F.count(F.lit(1)).alias("num_listings"),
            F.avg(price).alias("avg_price"),
            F.sum(price).alias("total_listing_value"),
        )
    )


def run_preset_status_analysis(df: DataFrame) -> DataFrame:
    price = _get_price_col(df)
    house = _get_house_size_col(df)
    status = _get_dim_col(df, "status")
    return (
        df.groupBy(status.alias("status"))
        .agg(
            F.count(F.lit(1)).alias("count"),
            F.avg(price).alias("avg_price"),
            F.avg(house).alias("avg_house_size"),
        )
    )


def run_preset_time_yearly(df: DataFrame) -> DataFrame:
    price = _get_price_col(df)
    year = _parse_year_from_date(df, "prev_sold_date")
    return (
        df.groupBy(year.alias("year"))
        .agg(
            F.avg(price).alias("avg_price"),
            F.count(F.lit(1)).alias("num_transactions"),
        )
    )


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
    parser = argparse.ArgumentParser(description="Aggregate metrics by group columns or run predefined presets.")
    parser.add_argument(
        "--input",
        type=str,
        default=None,
        help="Path to input (parquet dir or CSV glob). Defaults to routed parquet, then deduped parquet.",
    )
    parser.add_argument(
        "--preset",
        type=str,
        choices=[
            "market_trends",
            "property_type_size",
            "lot_size_influence",
            "broker_performance",
            "status_analysis",
            "time_yearly",
        ],
        help="Run a predefined aggregation preset matching common analyses.",
    )
    parser.add_argument(
        "--run-all-presets",
        action="store_true",
        help="Run all predefined presets and write each to its own subdirectory.",
    )
    parser.add_argument(
        "--group-by",
        type=str,
        required=False,
        help="Comma-separated grouping columns (e.g., 'segment,state').",
    )
    parser.add_argument(
        "--metrics",
        type=str,
        required=False,
        help="Comma-separated metrics in col:agg format (agg in sum|min|max|avg|mean|count).",
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
        help="Write outputs under data/processed/aggregated/{parquet,csv}.",
    )
    parser.add_argument(
        "--coalesce",
        type=int,
        default=1,
        help="Coalesce partitions before write (for small local outputs). Use -1 to skip.",
    )

    args = parser.parse_args()

    spark = create_spark(app_name="AggregateMetrics")
    inputs = resolve_inputs(args.input)
    df = read_dataset(spark, inputs)

    if args.take is not None and args.take > 0:
        df = df.limit(args.take)

    print("\nInput schema:")
    df.printSchema()

    # Preset mode(s)
    if args.run_all_presets or args.preset:
        results = []
        if args.run_all_presets:
            presets = [
                ("market_trends", run_preset_market_trends(df)),
                ("property_type_size", run_preset_property_type_size(df)),
                ("lot_size_influence", run_preset_lot_size_influence(df)),
                ("broker_performance", run_preset_broker_performance(df)),
                ("status_analysis", run_preset_status_analysis(df)),
                ("time_yearly", run_preset_time_yearly(df)),
            ]
        else:
            name = args.preset
            if name == "market_trends":
                presets = [(name, run_preset_market_trends(df))]
            elif name == "property_type_size":
                presets = [(name, run_preset_property_type_size(df))]
            elif name == "lot_size_influence":
                presets = [(name, run_preset_lot_size_influence(df))]
            elif name == "broker_performance":
                presets = [(name, run_preset_broker_performance(df))]
            elif name == "status_analysis":
                presets = [(name, run_preset_status_analysis(df))]
            elif name == "time_yearly":
                presets = [(name, run_preset_time_yearly(df))]
            else:
                raise ValueError(f"Unknown preset: {name}")

        for preset_name, preset_df in presets:
            print(f"\nPreset '{preset_name}' schema:")
            preset_df.printSchema()
            print(f"Preset '{preset_name}' groups: {preset_df.count()}")
            if args.write_output:
                project_root = Path(__file__).resolve().parents[1]
                base_root = project_root / "data" / "processed" / "aggregated" / preset_name
                coalesce = args.coalesce if args.coalesce is not None and args.coalesce > 0 else None
                write_outputs(preset_df, base_root, coalesce)
                print(f"Wrote preset '{preset_name}' outputs to: {base_root}")
        # Done with presets
        spark.stop()
        return

    # Generic mode with --group-by and --metrics
    if not args.group_by or not args.metrics:
        raise ValueError("Provide --group-by and --metrics, or use --preset/--run-all-presets.")
    group_cols = [c.strip() for c in args.group_by.split(",") if c.strip()]
    metrics = parse_metrics(args.metrics)
    agg_exprs = build_aggregations(df, metrics)
    grouped = df.groupBy(*[F.col(c) for c in group_cols]).agg(*agg_exprs)
    print("\nAggregated schema:")
    grouped.printSchema()
    print(f"Aggregated count (groups): {grouped.count()}")

    if args.write_output:
        project_root = Path(__file__).resolve().parents[1]
        output_root = project_root / "data" / "processed" / "aggregated"
        coalesce = args.coalesce if args.coalesce is not None and args.coalesce > 0 else None
        write_outputs(grouped, output_root, coalesce)
        print(f"\nWrote outputs to: {output_root}")

    spark.stop()


if __name__ == "__main__":
    main()


