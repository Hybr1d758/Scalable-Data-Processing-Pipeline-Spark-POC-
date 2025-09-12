import argparse
import glob
import json
import time
from pathlib import Path
from typing import Dict, List, Optional

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
    routed_dir = project_root / "data" / "processed" / "routed" / "parquet"
    if routed_dir.exists():
        return [str(routed_dir)]
    dedup_dir = project_root / "data" / "processed" / "cleanse_dedup" / "parquet"
    if dedup_dir.exists():
        return [str(dedup_dir)]
    raw_glob = project_root / "data" / "raw" / "usa-real-estate" / "*.csv"
    matches = sorted(glob.glob(str(raw_glob)))
    if not matches:
        raise FileNotFoundError("No inputs found. Provide --input or generate processed data first.")
    return matches


def read_dataset(spark: SparkSession, inputs: List[str]) -> DataFrame:
    if len(inputs) == 1 and Path(inputs[0]).is_dir():
        return spark.read.parquet(inputs[0])
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputs)
    )


def compute_quality(
    df: DataFrame,
    required_cols: List[str],
    unique_keys: List[str],
    numeric_min: Dict[str, float],
    numeric_max: Dict[str, float],
    max_null_frac: Dict[str, float],
) -> Dict[str, object]:
    total = df.count()

    checks: List[Dict[str, object]] = []

    # Required fields completeness
    for col in required_cols:
        if col not in df.columns:
            checks.append({
                "check": f"required_present:{col}",
                "status": "fail",
                "observed": "missing_column",
                "threshold": None,
                "details": {}
            })
            continue
        nulls = df.filter(F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")).count()
        frac = nulls / total if total else 0.0
        checks.append({
            "check": f"required_non_null:{col}",
            "status": "pass" if nulls == 0 else "fail",
            "observed": {"nulls": nulls, "fraction": round(frac, 6)},
            "threshold": {"nulls": 0},
            "details": {}
        })

    # Null rate thresholds
    for col, max_frac in max_null_frac.items():
        if col not in df.columns:
            continue
        nulls = df.filter(F.col(col).isNull()).count()
        frac = nulls / total if total else 0.0
        checks.append({
            "check": f"null_rate:{col}",
            "status": "pass" if frac <= max_frac else "fail",
            "observed": {"nulls": nulls, "fraction": round(frac, 6)},
            "threshold": {"max_fraction": max_frac},
            "details": {}
        })

    # Numeric ranges
    for col, vmin in numeric_min.items():
        if col in df.columns:
            cnt = df.filter((~F.col(col).isNull()) & (F.col(col).cast("double") < F.lit(vmin))).count()
            checks.append({
                "check": f"min:{col}",
                "status": "pass" if cnt == 0 else "fail",
                "observed": {"out_of_range": cnt},
                "threshold": {"min": vmin},
                "details": {}
            })
    for col, vmax in numeric_max.items():
        if col in df.columns:
            cnt = df.filter((~F.col(col).isNull()) & (F.col(col).cast("double") > F.lit(vmax))).count()
            checks.append({
                "check": f"max:{col}",
                "status": "pass" if cnt == 0 else "fail",
                "observed": {"out_of_range": cnt},
                "threshold": {"max": vmax},
                "details": {}
            })

    # Uniqueness of keys
    if unique_keys:
        if all(k in df.columns for k in unique_keys):
            dup_cnt = df.groupBy(*[F.col(k) for k in unique_keys]).count().filter(F.col("count") > 1).count()
            checks.append({
                "check": f"unique_keys:{','.join(unique_keys)}",
                "status": "pass" if dup_cnt == 0 else "fail",
                "observed": {"duplicate_key_groups": dup_cnt},
                "threshold": {"duplicates": 0},
                "details": {}
            })

    # Rollup summary
    summary = {
        "total_rows": total,
        "passed": sum(1 for c in checks if c["status"] == "pass"),
        "failed": sum(1 for c in checks if c["status"] == "fail"),
    }

    return {"summary": summary, "checks": checks}


def write_report(report: Dict[str, object], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # JSON
    (out_dir / "report.json").write_text(json.dumps(report, indent=2))
    # CSV (flat)
    lines = ["check,status,observed,threshold"]
    for c in report["checks"]:
        lines.append(
            f"{c['check']},{c['status']}," +
            f"{json.dumps(c['observed'])},{json.dumps(c['threshold'])}"
        )
    (out_dir / "report.csv").write_text("\n".join(lines))


def main() -> None:
    parser = argparse.ArgumentParser(description="Data quality checks over routed/deduped outputs.")
    parser.add_argument("--input", type=str, default=None, help="Input path (parquet dir or CSV glob). Defaults to routed parquet.")
    parser.add_argument("--required-cols", type=str, default="street,city,state,zip_code,price", help="Comma-separated required columns.")
    parser.add_argument("--unique-keys", type=str, default="street,city,state,zip_code", help="Comma-separated columns that must be unique together.")
    parser.add_argument("--min", dest="min_spec", type=str, default="price:0,house_size:0,acre_lot:0", help="col:min,col:min...")
    parser.add_argument("--max", dest="max_spec", type=str, default="", help="col:max,col:max...")
    parser.add_argument("--max-null-frac", type=str, default="status:0.2,brokered_by:0.5", help="col:frac,col:frac...")
    parser.add_argument("--take", type=int, default=100_000, help="Limit rows for local runs. Use -1 to process all.")
    parser.add_argument("--write-output", action="store_true", help="Write report under data/quality/<ts>/.")
    parser.add_argument("--print-all", action="store_true", help="Print all checks with status in the console output.")

    args = parser.parse_args()

    # Parse specs
    required_cols = [c.strip() for c in (args.required_cols or "").split(",") if c.strip()]
    unique_keys = [c.strip() for c in (args.unique_keys or "").split(",") if c.strip()]
    numeric_min: Dict[str, float] = {}
    numeric_max: Dict[str, float] = {}
    max_null_frac: Dict[str, float] = {}

    if args.min_spec:
        for part in args.min_spec.split(","):
            if ":" in part:
                col, val = part.split(":", 1)
                numeric_min[col.strip()] = float(val)
    if args.max_spec:
        for part in args.max_spec.split(","):
            if ":" in part:
                col, val = part.split(":", 1)
                numeric_max[col.strip()] = float(val)
    if args.max_null_frac:
        for part in args.max_null_frac.split(","):
            if ":" in part:
                col, val = part.split(":", 1)
                max_null_frac[col.strip()] = float(val)

    spark = create_spark(app_name="DataQuality")
    inputs = resolve_inputs(args.input)
    df = read_dataset(spark, inputs)
    if args.take is not None and args.take > 0:
        df = df.limit(args.take)

    print("\nInput schema:")
    df.printSchema()

    report = compute_quality(df, required_cols, unique_keys, numeric_min, numeric_max, max_null_frac)

    print("\nQuality summary:")
    print(json.dumps(report["summary"], indent=2))

    # Always show failing checks with details
    failing = [c for c in report["checks"] if c["status"] == "fail"]
    if failing:
        print("\nFailing checks:")
        for c in failing:
            print(f"- {c['check']} -> observed={c['observed']} threshold={c['threshold']}")
    else:
        print("\nFailing checks: none")

    # Optionally show all checks
    if args.print_all:
        print("\nAll checks:")
        for c in report["checks"]:
            print(f"- {c['check']}: {c['status']} | observed={c['observed']} | threshold={c['threshold']}")

    if args.write_output:
        project_root = Path(__file__).resolve().parents[1]
        out_dir = project_root / "data" / "quality" / str(int(time.time()))
        write_report(report, out_dir)
        print(f"\nWrote quality report to: {out_dir}")

    spark.stop()


if __name__ == "__main__":
    main()


