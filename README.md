## Scalable Data Processing Pipeline — Spark POC

A practical, incremental Spark pipeline you can run locally first, then scale up. We start simple (ingest one CSV ~100k rows), then add cleansing, routing, enrichment, aggregations, validation, throughput measurement, and persistence.

### Pipeline stages
- **Ingest one CSV (local)**: quick validation of schema, sample, and counts
- **Cleanse & Deduplicate**: remove duplicates using Spark joins/keys
- **Route Data**: add a derived column based on decision rules
- **Enrich Data**: join with a small lookup table
- **Aggregate**: groupBy with sum/min/max
- **Validate & Split**: separate invalid records
- **Measure Throughput**: compare records/sec on small vs large datasets
- **Persist Output**: write results as CSV + Parquet

We will keep this README up to date as each stage is added.

---

### Repo structure (key paths)
- `jobs/` — Spark jobs/scripts
  - `jobs/ingest_single_csv.py` — Step 1: read a single CSV, print schema/count/sample
- `data/` — local data (not committed)
  - `data/raw/usa-real-estate/` — place your input CSV(s) here
  - `data/processed/` — outputs for later steps

Note: The project path on this machine contains a space. Wrap paths in quotes when using the shell.

Project root (example):
```
"/Users/edwardjr/Downloads/upwork /Engineering/Scalable-Data-Processing-Pipeline-Spark-POC-"
```

---

### Prerequisites
- Python 3.9+
- Java/JRE installed (required by Spark)
- Install PySpark:
```bash
pip3 install pyspark
```

Optional — Kaggle CLI (for programmatic downloads later):
```bash
pip3 install kaggle
# Place API token at ~/.kaggle/kaggle.json and run: chmod 600 ~/.kaggle/kaggle.json
```

---

### Data placement
Put your CSV file(s) here so scripts can auto-discover them:
```
data/raw/usa-real-estate/
```
Alternatively, you can pass an explicit `--input` path to any script.

---

### Step 1 — Ingest a single CSV (local)
Reads one CSV, optionally limits to ~100k rows, then prints schema, row count, and a small sample.

From the project root:
```bash
python3 "jobs/ingest_single_csv.py" --take 100000
# or specify a file explicitly
python3 "jobs/ingest_single_csv.py" --input "/absolute/path/to/file.csv" --take 100000
```

Notes:
- The script prints the schema because it calls `printSchema()`.
- `inferSchema` controls types: by default it infers data types; add `--no-infer-schema` to read all columns as strings.

---

### Roadmap (as we implement)
1) Cleanse & Deduplicate
   - Remove duplicate records using Spark joins/keys
2) Route Data
   - Add a derived routing column based on decision rules
3) Enrich Data
   - Join with a small lookup table (static CSV or in-memory)
4) Aggregate
   - `groupBy` selected dimensions; compute `sum/min/max` metrics
5) Validate & Split
   - Flag invalid records and write to a separate output
6) Measure Throughput
   - Compare records/sec across small and larger input sizes
7) Persist Output
   - Write final results to CSV and Parquet (with partitioning options)

---

### Troubleshooting
- If Spark fails to start, confirm Java is installed and on PATH
- If no CSV is found, place files under `data/raw/usa-real-estate/` or provide `--input /absolute/path.csv`
- On macOS, always quote paths that contain spaces
