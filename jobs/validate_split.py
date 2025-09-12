import argparse
import glob
from pathlib import Path
from typing import List, Optional, Sequence

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
        return spark.read.parquet(inputs[0])
    return (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(inputs)
    )


def is_string_column(df: DataFrame, column_name: str) -> bool:
    try:
        field = next(f for f in df.schema.fields if f.name == column_name)
        return isinstance(field.dataType, StringType)
    except StopIteration:
        return False


def build_validation_columns(
    df: DataFrame,
    required_cols: Sequence[str],
    min_price: Optional[float],
    max_price: Optional[float],
    zip_col: Optional[str],
    zip_len: Optional[int],
    zip_numeric: bool,
    nonneg_cols: Sequence[str],
    int_cols: Sequence[str],
) -> DataFrame:
    errors = []

    # Required columns present and non-empty (strings) / non-null (others)
    for col in required_cols:
        if col not in df.columns:
            errors.append(F.lit(f"missing_column:{col}"))
            continue
        if is_string_column(df, col):
            cond = F.col(col).isNull() | (F.trim(F.col(col)) == F.lit(""))
        else:
            cond = F.col(col).isNull()
        errors.append(F.when(cond, F.lit(f"required_empty:{col}")).otherwise(F.lit(None)))

    # Price range checks
    if "price" in df.columns:
        price = F.col("price").cast("double")
        if min_price is not None:
            errors.append(F.when((~price.isNull()) & (price < F.lit(min_price)), F.lit("price_below_min")).otherwise(F.lit(None)))
        if max_price is not None:
            errors.append(F.when((~price.isNull()) & (price > F.lit(max_price)), F.lit("price_above_max")).otherwise(F.lit(None)))

    # Non-negative numeric columns
    for col in nonneg_cols:
        if col in df.columns:
            c = F.col(col).cast("double")
            errors.append(F.when((~c.isNull()) & (c < F.lit(0)), F.lit(f"negative:{col}")).otherwise(F.lit(None)))

    # Integer columns
    for col in int_cols:
        if col in df.columns:
            c = F.col(col)
            # if string, allow digits-only; if numeric, check fractional part
            err = F.when(c.isNull(), F.lit(None))
            if is_string_column(df, col):
                err = F.when(~F.regexp_replace(c, r"\d", "").eqNullSafe(""), F.lit(f"not_integer:{col}")).otherwise(F.lit(None))
            else:
                err = F.when((c.cast("double") % F.lit(1) != F.lit(0)), F.lit(f"not_integer:{col}")).otherwise(F.lit(None))
            errors.append(err)

    # Zip code checks
    if zip_col and zip_col in df.columns and (zip_len is not None or zip_numeric):
        z = F.col(zip_col).cast("string")
        z_digits = F.regexp_replace(z, r"[^0-9]", "") if zip_numeric else z
        if zip_numeric:
            errors.append(F.when(~F.regexp_replace(z, r"[^0-9]", "").eqNullSafe(F.regexp_replace(z, r"[^0-9]", "")), F.lit("zip_non_numeric")).otherwise(F.lit(None)))
        if zip_len is not None:
            errors.append(F.when(F.length(F.coalesce(z_digits, F.lit(""))) != F.lit(int(zip_len)), F.lit("zip_wrong_length")).otherwise(F.lit(None)))

    # Aggregate non-null errors into an array
    error_array = F.array(*errors) if errors else F.array()
    cleaned_errors = F.expr("filter(errors, x -> x is not null)").alias("validation_errors")
    df_with_errors = df.select("*", error_array.alias("errors")).withColumn("validation_errors", cleaned_errors).drop("errors")
    return df_with_errors.withColumn("is_valid", (F.size(F.col("validation_errors")) == F.lit(0)))


def write_outputs(df_valid: DataFrame, df_invalid: DataFrame, output_root: Path, coalesce: Optional[int]) -> None:
    output_root.mkdir(parents=True, exist_ok=True)
    valid_root = output_root / "valid"
    invalid_root = output_root / "invalid"
    valid_root.mkdir(parents=True, exist_ok=True)
    invalid_root.mkdir(parents=True, exist_ok=True)

    def write(df: DataFrame, root: Path):
        to_write = df
        if coalesce is not None and coalesce > 0:
            to_write = to_write.coalesce(coalesce)
        to_write.write.mode("overwrite").parquet(str(root / "parquet"))
        to_write.write.mode("overwrite").option("header", "true").csv(str(root / "csv"))

    write(df_valid, valid_root)
    write(df_invalid, invalid_root)


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate records and split into valid/invalid outputs.")
    parser.add_argument("--input", type=str, default=None, help="Input path (parquet dir or CSV glob). Defaults to routed parquet if present.")
    parser.add_argument("--required-cols", type=str, default="street,city,state,zip_code,price", help="Comma-separated required columns.")
    parser.add_argument("--min-price", type=float, default=0.0, help="Minimum allowed price (inclusive). Use None to disable.")
    parser.add_argument("--max-price", type=float, default=None, help="Maximum allowed price (inclusive).")
    parser.add_argument("--zip-col", type=str, default="zip_code", help="Zip/postal code column name.")
    parser.add_argument("--zip-len", type=int, default=5, help="Required numeric length for zip when --zip-numeric is set.")
    parser.add_argument("--zip-numeric", action="store_true", help="Require zip to be numeric (digits only) and of --zip-len length.")
    parser.add_argument("--nonneg-cols", type=str, default="price,house_size,acre_lot", help="Comma-separated numeric columns that must be non-negative.")
    parser.add_argument("--int-cols", type=str, default="bed,bath", help="Comma-separated columns that must be integers.")
    parser.add_argument("--take", type=int, default=100_000, help="Limit rows for local runs. Use -1 to process all.")
    parser.add_argument("--write-output", action="store_true", help="Write outputs under data/processed/validated/{valid,invalid}/{parquet,csv}.")
    parser.add_argument("--coalesce", type=int, default=1, help="Coalesce partitions before write (for small local outputs). Use -1 to skip.")

    args = parser.parse_args()

    spark = create_spark(app_name="ValidateSplit")
    inputs = resolve_inputs(args.input)
    df = read_dataset(spark, inputs)
    if args.take is not None and args.take > 0:
        df = df.limit(args.take)

    print("\nInput schema:")
    df.printSchema()

    required_cols = [c.strip() for c in (args.required_cols or "").split(",") if c.strip()]
    nonneg_cols = [c.strip() for c in (args.nonneg_cols or "").split(",") if c.strip()]
    int_cols = [c.strip() for c in (args.int_cols or "").split(",") if c.strip()]

    validated = build_validation_columns(
        df,
        required_cols=required_cols,
        min_price=args.min_price,
        max_price=args.max_price,
        zip_col=args.zip_col,
        zip_len=args.zip_len,
        zip_numeric=args.zip_numeric,
        nonneg_cols=nonneg_cols,
        int_cols=int_cols,
    )

    total = validated.count()
    valid_df = validated.filter(F.col("is_valid"))
    invalid_df = validated.filter(~F.col("is_valid"))
    valid_count = valid_df.count()
    invalid_count = invalid_df.count()
    print(f"\nValidation results: total={total}, valid={valid_count}, invalid={invalid_count}")

    print("\nSample invalid rows (up to 5):")
    for row in invalid_df.select("validation_errors", *[c for c in validated.columns if c not in {"validation_errors", "is_valid"}]).take(5):
        print(row)

    if args.write_output:
        project_root = Path(__file__).resolve().parents[1]
        output_root = project_root / "data" / "processed" / "validated"
        coalesce = args.coalesce if args.coalesce is not None and args.coalesce > 0 else None
        write_outputs(valid_df, invalid_df, output_root, coalesce)
        print(f"\nWrote outputs to: {output_root}/valid and {output_root}/invalid")

    spark.stop()


if __name__ == "__main__":
    main()


