## Performance Comparison

### Environment Configuration

| Environment | Workers | Memory per Worker | Total Cores |
|-------------|---------|-------------------|-------------|
| Local (pandas) | 1 | 16GB | 8 |
| Spark | 4 | 4GB | 16 |
| Ray | 4 | 4GB | 16 |

### Execution Results

| Environment | Load Time | Transform Time | Write Time | Total Time |
|-------------|-----------|----------------|------------|------------|
| Local | X.XXs | X.XXs | X.XXs | X.XXs |
| Spark (4 exec) | X.XXs | X.XXs | X.XXs | X.XXs |

### Scaling Analysis

[Include scaling plot here]

### Bottleneck Analysis

1. **Local:** Memory-bound during groupBy aggregation
2. **Spark:** Shuffle-bound during groupBy (observed X GB shuffle)

### Recommendations

- For datasets < 5GB: Use local pandas (faster due to no overhead)
- For datasets > 10GB: Use Spark with N executors
- Crossover point: ~X GB based on empirical testing