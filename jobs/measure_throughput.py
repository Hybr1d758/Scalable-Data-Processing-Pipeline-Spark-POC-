import argparse
import glob
import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


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
    # Prefer routed parquet if available, else deduped parquet, else raw CSVs
    routed_dir = project_root / "data" / "processed" / "routed" / "parquet"
    if routed_dir.exists():
        return [str(routed_dir)]
    dedup_dir = project_root / "data" / "processed" / "cleanse_dedup" / "parquet"
    if dedup_dir.exists():
        return [str(dedup_dir)]
    raw_glob = project_root / "data" / "raw" / "usa-real-estate" / "*.csv"
    matches = sorted(glob.glob(str(raw_glob)))
    if not matches:
        raise FileNotFoundError(
            f"No inputs found. Provide --input or generate processed data first."
        )
    return matches


def read_dataset(spark: SparkSession, inputs: List[str]) -> DataFrame:
    if len(inputs) == 1 and Path(inputs[0]).is_dir():
        # assume parquet dir
        return spark.read.parquet(inputs[0])
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputs)
    )


def detect_sensible_keys(df: DataFrame) -> List[str]:
    columns_lower = {c.lower(): c for c in df.columns}
    for cand in ["parcel_number", "parcelnumber", "parcelid", "apn", "property_id", "mlsnumber", "mls_id", "id"]:
        if cand in columns_lower:
            return [columns_lower[cand]]
    if "address" in columns_lower and "zipcode" in columns_lower:
        return [columns_lower["address"], columns_lower["zipcode"]]
    for combo in (("street", "city", "state", "zip_code"), ("address", "city", "state", "zip")):
        if all(x in columns_lower for x in combo):
            return [columns_lower[x] for x in combo]
    # fallback: no dedup keys
    return []


def deduplicate_by_join(df: DataFrame, key_columns: Sequence[str]) -> DataFrame:
    if not key_columns:
        return df
    df_with_id = df.withColumn("__row_id", F.monotonically_increasing_id())
    winners = df_with_id.groupBy(*[F.col(k) for k in key_columns]).agg(F.min("__row_id").alias("__row_id"))
    return df_with_id.join(winners, on=list(key_columns) + ["__row_id"], how="inner").drop("__row_id")


def _coalesce_existing(df: DataFrame, candidate_names: Sequence[str]) -> F.Column:
    existing = [name for name in candidate_names if name in df.columns]
    if not existing:
        return F.lit(None)
    return F.coalesce(*[F.col(name) for name in existing])


def apply_routing_basic(df: DataFrame) -> DataFrame:
    price_col = _coalesce_existing(df, ["price", "list_price", "sold_price"]).cast("double")
    beds_col = _coalesce_existing(df, ["beds", "bedrooms", "bed"]).cast("int")
    sqft_col = _coalesce_existing(df, ["sqft", "sqft_living", "living_area", "house_size"]).cast("double")
    segment = (
        F.when(price_col > F.lit(1_000_000), F.lit("luxury"))
        .when((beds_col >= F.lit(3)) & (sqft_col >= F.lit(2000)), F.lit("family"))
        .otherwise(F.lit("standard"))
    )
    return df.withColumn("segment", segment)


def aggregate_sample(df: DataFrame) -> DataFrame:
    price = _coalesce_existing(df, ["price", "list_price", "sold_price"]).cast("double")
    state = F.col("state") if "state" in df.columns else F.lit(None).alias("state")
    segment = F.col("segment") if "segment" in df.columns else F.lit(None).alias("segment")
    return df.groupBy(segment.alias("segment"), state.alias("state")).agg(F.avg(price).alias("avg_price"), F.count(F.lit(1)).alias("count"))


def measure_stage(records: int, fn, *args, **kwargs) -> Tuple[float, int]:
    start = time.time()
    result = fn(*args, **kwargs)
    # Force materialization with a count when result is a DataFrame
    if isinstance(result, DataFrame):
        out_count = result.count()
    else:
        out_count = records
    elapsed = time.time() - start
    return elapsed, out_count


def write_metrics(metrics: List[Dict[str, object]], output_root: Path) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    csv_path = output_root / "throughput.csv"
    json_path = output_root / "throughput.json"
    # Write CSV
    if metrics:
        headers = list(metrics[0].keys())
        lines = [",".join(headers)]
        for m in metrics:
            lines.append(",".join(str(m[h]) for h in headers))
        csv_path.write_text("\n".join(lines))
    # Write JSON
    json_path.write_text(json.dumps(metrics, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description="Measure throughput (records/sec) across pipeline stages and sizes.")
    parser.add_argument("--input", type=str, default=None, help="Input path (parquet dir or CSV glob). Defaults to routed, then deduped, then raw.")
    parser.add_argument("--sizes", type=str, default="10000,100000", help="Comma-separated sizes to test; use -1 for all rows.")
    parser.add_argument("--keys", type=str, default=None, help="Keys for dedup (comma-separated). If omitted, tries to auto-detect.")
    parser.add_argument("--write-output", action="store_true", help="Write metrics to data/metrics/throughput.{csv,json}.")

    args = parser.parse_args()

    spark = create_spark(app_name="MeasureThroughput")
    inputs = resolve_inputs(args.input)
    base_df = read_dataset(spark, inputs)

    total_input = base_df.count()
    print(f"Total input rows available: {total_input}")

    keys: List[str] = []
    if args.keys:
        keys = [c.strip() for c in args.keys.split(",") if c.strip()]
    else:
        try:
            keys = detect_sensible_keys(base_df)
            if keys:
                print(f"Auto-detected keys for dedup: {keys}")
            else:
                print("No dedup keys detected; dedup stage will be skipped.")
        except Exception:
            keys = []
            print("Key detection failed; skipping dedup stage.")

    sizes: List[int] = []
    for s in args.sizes.split(","):
        s = s.strip()
        if not s:
            continue
        sizes.append(int(s))

    metrics: List[Dict[str, object]] = []
    for sz in sizes:
        if sz is None or sz <= 0:
            df = base_df
            size_label = "all"
            in_count = total_input
        else:
            df = base_df.limit(sz)
            in_count = df.count()
            size_label = str(sz)

        # Cache sample df to stabilize timings across stages
        df = df.cache()

        # Stage: read (already read, but time a recount as proxy)
        t_read_start = time.time()
        _ = df.count()
        t_read = time.time() - t_read_start
        metrics.append({"size": size_label, "stage": "read", "seconds": round(t_read, 4), "records": in_count, "rps": round(in_count / t_read if t_read > 0 else 0.0, 2)})

        # Stage: dedup (if keys)
        if keys:
            t_dedup, out_count = measure_stage(in_count, deduplicate_by_join, df, keys)
            metrics.append({"size": size_label, "stage": "dedup", "seconds": round(t_dedup, 4), "records": out_count, "rps": round(in_count / t_dedup if t_dedup > 0 else 0.0, 2)})
            routed_input = deduplicate_by_join(df, keys)
        else:
            routed_input = df

        # Stage: route
        t_route, out_count = measure_stage(in_count, apply_routing_basic, routed_input)
        metrics.append({"size": size_label, "stage": "route", "seconds": round(t_route, 4), "records": out_count, "rps": round(in_count / t_route if t_route > 0 else 0.0, 2)})
        routed_df = apply_routing_basic(routed_input)

        # Stage: aggregate (simple)
        t_agg_start = time.time()
        agg_df = aggregate_sample(routed_df)
        _ = agg_df.count()
        t_agg = time.time() - t_agg_start
        metrics.append({"size": size_label, "stage": "aggregate", "seconds": round(t_agg, 4), "records": out_count, "rps": round(in_count / t_agg if t_agg > 0 else 0.0, 2)})

    for m in metrics:
        print(m)

    if args.write_output:
        project_root = Path(__file__).resolve().parents[1]
        out_dir = project_root / "data" / "metrics"
        write_metrics(metrics, out_dir)
        print(f"\nWrote metrics to: {out_dir}")

    spark.stop()


if __name__ == "__main__":
    main()


