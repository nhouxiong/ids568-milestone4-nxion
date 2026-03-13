# Milestone 4 - Distributed Feature Engineering

## Requirements
- Docker Desktop

## Setup
```bash
git clone https://github.com/nhouxiong/ids568-milestone4-nxion
cd ids568-milestone4-nxion
docker-compose up -d
```

Verify all 3 containers are running:
```bash
docker ps
```

You should see `spark-master`, `spark-worker-1`, and `spark-worker-2` all with 
status `Up`. The Spark Master UI will be available at http://localhost:8080 with 
2 workers registered.

![Spark Master UI](spark_master_ui.png)

## Reproduce Results

### 1. Generate 10M row dataset
```bash
docker exec spark-master python3 /opt/spark/apps/generate_data.py \
  --rows 10000000 --seed 42 --output /opt/spark/apps/data/
```

Expected output:
```
Generating 10,000,000 rows with seed=42
  Chunk 1/10: 1,000,000 rows
  ...
  Chunk 10/10: 1,000,000 rows
Verification hash: 12014c7a4e5ba920
```

### 2. Run local baseline (1 worker)
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master local[1] \
  /opt/spark/apps/pipeline.py \
  --input /opt/spark/apps/data/ \
  --output /opt/spark/apps/output_local/ \
  --mode local \
  --partitions 10
```

Expected output:
```
===== METRICS =====
  runtime_seconds: 17.9
  partitions_used: 10
===================
```

### 3. Run distributed (2 workers)
```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/apps/pipeline.py \
  --input /opt/spark/apps/data/ \
  --output /opt/spark/apps/output_dist/ \
  --mode distributed \
  --partitions 20
```

Expected output:
```
===== METRICS =====
  runtime_seconds: 26.19
  partitions_used: 20
===================
```

## Verify Reproducibility
```bash
docker exec spark-master python3 /opt/spark/apps/generate_data.py \
  --rows 100 --seed 42 --output /opt/spark/apps/run1/

docker exec spark-master python3 /opt/spark/apps/generate_data.py \
  --rows 100 --seed 42 --output /opt/spark/apps/run2/
```

Both runs should produce the same verification hash, confirming deterministic output.

## Seeds Used
- Data generation: `--seed 42`

## Tear Down
```bash
docker-compose down
```