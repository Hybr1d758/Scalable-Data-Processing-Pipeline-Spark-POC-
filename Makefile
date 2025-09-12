.PHONY: setup run-ingest run-dedupe run-route run-aggregate run-validate run-throughput run-duckdb run-quality docker-build docker-run

setup:
	python3 -m pip install -r requirements.txt

run-ingest:
	python3 jobs/ingest_single_csv.py --take 100000

run-dedupe:
	python3 jobs/cleanse_deduplicate.py --keys street,city,state,zip_code --lowercase --normalize-spaces --strip-punct --take 100000 --write-output

run-route:
	python3 jobs/route_data.py --rules preset_basic --take 100000 --write-output

run-aggregate:
	python3 jobs/aggregate_metrics.py --input data/processed/routed/parquet --run-all-presets --write-output

run-validate:
	python3 jobs/validate_split.py --required-cols street,city,state,zip_code,price --min-price 0 --zip-col zip_code --zip-len 5 --zip-numeric --zip-normalize --nonneg-cols price,house_size,acre_lot --int-cols bed,bath --take 100000 --write-output

run-throughput:
	python3 jobs/measure_throughput.py --keys street,city,state,zip_code --sizes 10000,100000 --write-output

run-duckdb:
	python3 jobs/persist_duckdb.py --db data/warehouse.duckdb --refresh-all

run-quality:
	python3 jobs/data_quality.py --write-output

docker-build:
	docker build -t spark-poc-jobs:latest .

docker-run:
	docker compose up --build
