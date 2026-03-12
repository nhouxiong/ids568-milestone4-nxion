#!/usr/bin/env python3
"""
Synthetic data generator for Milestone 4.

Usage:
    python generate_data.py --rows 10000000 --seed 42 --output data/synthetic/
    python generate_data.py --rows 1000000 --skew 0.5 --partitions 50

This script generates reproducible synthetic data for distributed processing.
Running with the same arguments always produces identical output.
"""

import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import os
import hashlib
import sys

def compute_data_hash(df: pd.DataFrame) -> str:
    """Compute hash of DataFrame for verification."""
    content = pd.util.hash_pandas_object(df, index=True).values
    return hashlib.sha256(content.tobytes()).hexdigest()[:16]

def generate_data(args) -> None:
    """Main data generation function."""
    np.random.seed(args.seed)
    
    print(f"Generating {args.rows:,} rows with seed={args.seed}")
    
    # Configuration
    n_users = min(args.rows // 100, 100000)  # Scale users with data size
    user_ids = [f"user_{i:06d}" for i in range(n_users)]
    categories = ["food", "transport", "shopping", "utilities", "entertainment"]
    
    # Apply skew if specified
    if args.skew > 0:
        weights = np.array([(i + 1) ** (-args.skew * 2) for i in range(n_users)])
        weights /= weights.sum()
    else:
        weights = None
    
    # Generate data in chunks (memory-efficient)
    chunk_size = min(1_000_000, args.rows)
    n_chunks = (args.rows + chunk_size - 1) // chunk_size
    
    os.makedirs(args.output, exist_ok=True)
    
    for chunk_id in range(n_chunks):
        chunk_rows = min(chunk_size, args.rows - chunk_id * chunk_size)
        
        # Deterministic seed per chunk
        np.random.seed(args.seed + chunk_id)
        
        chunk_data = {
            "user_id": np.random.choice(user_ids, size=chunk_rows, p=weights),
            "amount": np.random.exponential(scale=50, size=chunk_rows).round(2),
            "timestamp": [
                datetime(2024, 1, 1) + timedelta(
                    seconds=int(s) + chunk_id * chunk_size
                )
                for s in np.random.uniform(0, 86400, size=chunk_rows)
            ],
            "category": np.random.choice(categories, size=chunk_rows),
        }
        
        df = pd.DataFrame(chunk_data)
        
        if args.partitions > 1:
            # Write as partitioned parquet
            partition_file = os.path.join(
                args.output, 
                f"part-{chunk_id:05d}.parquet"
            )
        else:
            partition_file = os.path.join(args.output, "data.parquet")
        
        df.to_parquet(partition_file, index=False)
        print(f"  Chunk {chunk_id + 1}/{n_chunks}: {chunk_rows:,} rows")
    
    # Compute verification hash
    sample_df = pd.read_parquet(args.output)
    data_hash = compute_data_hash(sample_df.head(10000))
    
    # Write metadata for reproducibility verification
    metadata = {
        "rows": args.rows,
        "seed": args.seed,
        "skew": args.skew,
        "partitions": n_chunks,
        "verification_hash": data_hash,
        "generated_at": datetime.now().isoformat(),
    }
    
    import json
    with open(os.path.join(args.output, "metadata.json"), "w") as f:
        json.dump(metadata, f, indent=2)
    
    print(f"\nComplete! Output: {args.output}")
    print(f"Verification hash: {data_hash}")
    print("Re-run with same args to verify reproducibility.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate synthetic data")
    parser.add_argument("--rows", type=int, default=10_000_000,
                        help="Number of rows to generate")
    parser.add_argument("--seed", type=int, default=42,
                        help="Random seed for reproducibility")
    parser.add_argument("--skew", type=float, default=0.0,
                        help="Skew factor (0=uniform, 1=extreme)")
    parser.add_argument("--partitions", type=int, default=1,
                        help="Number of output partitions")
    parser.add_argument("--output", type=str, default="data/synthetic/",
                        help="Output directory")
    
    args = parser.parse_args()
    generate_data(args)