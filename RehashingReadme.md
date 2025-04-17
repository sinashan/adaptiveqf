# CS7280: Rehashing project

Note that you have to install splinterdb for other tests in the Makefile to work.

# Broom Filter approach

This code lives in the `broom-filter` branch.

Steps to Run:

```bash
git checkout broom-filter
make clean
make -B
./test_qfdb 20 9 $(python3 -c "print(pow(10, 6))") # 1M items
```


### Tuning C value:
Update `src/qf_uthash.c:15` with different values and run `make -B`

# Two filter and downtime approch

This code lives in the `approach2-downtime-rehash-same-filter` branch.

Steps to run:

```bash
git checkout approach2-downtime-rehash-same-filter
make clean
make test_rehash_perf_two_filter test_rehash_perf_naive
./test_rehash_perf_naive
./test_rehash_perf_two_filter
```

# Bucket based approach

This code lives in the `feat3-iterative` branch

```bash
git checkout feat3-iterative
make clean
make -B
```

## Understanding Bucket-Based Rehashing

The bucket-based approach divides the filter's hash space into independently managed bucket groups:
- Each bucket group tracks its own statistics (queries, false positives, FP rate)
- When a bucket's FP rate exceeds a threshold, only that bucket's items are rehashed
- This localization prevents adversarial attacks from affecting the entire filter

## Running Adversarial Tests

The repository provides several test modes for evaluating the bucket-based approach under adversarial workloads:

### Block-based Adversarial Test

This test evaluates how the bucket-based approach performs under targeted attacks:

```bash
./test_qfdb_adversarial block <qbits> <rbits> <num_queries> <attack_pct> [options]
```

Parameters:
- `qbits`: Log2 of number of slots in filter (e.g., 20 for ~1M slots)
- `rbits`: Number of remainder bits per slot (e.g., 9)
- `num_queries`: Total number of queries to perform
- `attack_pct`: Percentage of queries dedicated to adversarial attack (0-100)

Options:
- `--verbose`: Show detailed output
- `--bucket-size <size>`: Set bucket group size (default: 128)
- `--fp-threshold <rate>`: Set false positive threshold (default: 0.05)
- `--output <file>`: Specify output CSV file
- `--seed <value>`: Set random seed for reproducibility

Example:
```bash
./test_qfdb_adversarial block 20 9 1000000 20 --bucket-size 256 --fp-threshold 0.05
```

### Capacity Test

This test evaluates how the filter performs when close to capacity:

```bash
./test_qfdb_adversarial capacity <qbits> <rbits> <num_queries> <mode> [options]
```

Parameters:
- `qbits`, `rbits`: Same as above
- `num_queries`: Number of queries to perform against a 95% full filter
- `mode`: Implementation to test ('block' or 'naive')

Example:
```bash
./test_qfdb_adversarial capacity 20 9 1000000 block --bucket-size 128
```

### Naive Rehashing Test

For comparison, you can also test a naive global rehashing approach:

```bash
./test_qfdb_adversarial naive <qbits> <rbits> <num_ops> <rehash_threshold> [options]
```

## Parameter Tuning

For best results, consider experimenting with:

- **Bucket Size**: Smaller buckets (64-128) provide finer-grained adaptation but higher overhead; larger buckets (256-512) have less overhead but coarser adaptation.
- **FP Threshold**: Lower thresholds (0.01-0.05) trigger rehashing sooner, while higher values (0.1-0.3) are more tolerant of false positives.
- **Attack Percentage**: Higher values simulate more aggressive adversaries.

## Analyzing Results

Test results include:
- False positive rates before/during/after attack
- Number of rehashing operations
- Memory overhead
- Query latency statistics

Results are printed to stdout and can also be saved to CSV files for further analysis.
