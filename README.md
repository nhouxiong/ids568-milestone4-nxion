# Milestone 4 - Distributed Feature Engineering

## Requirements
- Python 3.9+
- Java 11+ (required for PySpark)
- See requirements.txt

## Setup
pip install -r requirements.txt

## Reproduce Results

### 1. Generate Data
python generate_data.py --rows 10000000 --seed 42 --output data/

### 2. Run Local Baseline
python pipeline.py --input data/ --output out_local/ --mode local --workers 1

### 3. Run Distributed
python pipeline.py --input data/ --output out_dist/ --mode distributed --workers 4

## Verify Reproducibility
python generate_data.py --rows 100 --seed 42 --output run1/
python generate_data.py --rows 100 --seed 42 --output run2/
diff -r run1/ run2/  # Should show no differences
