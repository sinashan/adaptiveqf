#!/bin/bash
# filepath: /home/ubuntu/AF/adaptiveqf/test/run_qfdb_test.sh

# Use absolute paths to avoid directory issues
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="${BASE_DIR}/test/logs"

echo "=== QFDB Configuration Testing ==="
echo "Testing different bucket sizes and false positive thresholds"
echo "Base directory: ${BASE_DIR}"
echo "Log directory: ${LOG_DIR}"
echo "-------------------------------------------------------"

# Create log directory with explicit permissions
mkdir -p "${LOG_DIR}"
chmod 755 "${LOG_DIR}"

# Base parameters - reduce size for faster testing
QBITS=26
RBITS=9
NUM_OPS=5000000  # Reduced for faster testing

# Compile the test if needed
cd "${BASE_DIR}"
make test_qfdb

# Function to run test with parameters and log results
run_test() {
    local bucket_size=$1
    local fp_threshold=$2
    local test_name="qbits=${QBITS}_rbits=${RBITS}_bucket=${bucket_size}_fp=${fp_threshold}"
    
    echo ""
    echo "Running test: ${test_name}"
    echo "Bucket size: ${bucket_size}, FP threshold: ${fp_threshold}"
    
    local log_file="${LOG_DIR}/${test_name}.log"
    
    # Clear any existing log file
    rm -f "${log_file}"
    touch "${log_file}"
    
    # Run the test with absolute paths and capture output
    echo "Command: ${BASE_DIR}/test_qfdb ${QBITS} ${RBITS} ${NUM_OPS} --bucket-size ${bucket_size} --fp-threshold ${fp_threshold} --rehash-test"
    "${BASE_DIR}/test_qfdb" ${QBITS} ${RBITS} ${NUM_OPS} --bucket-size ${bucket_size} --fp-threshold ${fp_threshold} --rehash-test > "${log_file}" 2>&1
    
    # Check if the test completed successfully
    if [ $? -ne 0 ] || [ ! -s "${log_file}" ]; then
        echo "ERROR: Test failed or produced no output. Check ${log_file} for details."
        cat "${log_file}"  # Print the error output for debugging
        return 1
    fi
    
    # Extract key metrics from the log file
    local exact_hit_rate=$(grep "Found .* exact keys" "${log_file}" | head -1 | awk '{print $NF}' | tr -d '(%)')
    local fp_rate=$(grep "False positive rate:" "${log_file}" | awk '{print $4}')
    local rehash_ops=$(grep "Rehashing operations:" "${log_file}" | awk '{print $3}')
    local rehashed_items=$(grep "Rehashed items:" "${log_file}" | awk '{print $3}')
    
    # Extract memory metrics
    local bucket_stats_mem=$(grep "Bucket stats:" "${log_file}" | awk '{print $3}')
    local rehash_overhead=$(grep "Total rehashing overhead:" "${log_file}" | awk '{print $4}')
    local rehash_pct=$(grep "Total rehashing overhead:" "${log_file}" | awk '{print $6}' | tr -d '(%)')
    
    # Print summary with memory info
    echo "Hit rate: ${exact_hit_rate}%"
    echo "False positive rate: ${fp_rate}"
    echo "Rehashing operations: ${rehash_ops}"
    echo "Rehashed items: ${rehashed_items}"
    echo "Bucket stats memory: ${bucket_stats_mem} KB"
    echo "Rehashing overhead: ${rehash_overhead} KB (${rehash_pct}% of QF)"
    echo "Full log in: ${log_file}"
    echo "-------------------------------------------------------"
    
    # Add to CSV summary
    echo "${bucket_size},${fp_threshold},${exact_hit_rate},${fp_rate},${rehash_ops},${rehashed_items},${bucket_stats_mem},${rehash_overhead},${rehash_pct}" >> "${LOG_DIR}/summary.csv"
    
    # Sleep briefly to ensure system resources are freed
    sleep 1
    
    return 0
}

# Initialize summary CSV with absolute path
echo "Bucket Size,FP Threshold,Hit Rate,FP Rate,Rehash Ops,Rehashed Items,Bucket Stats KB,Rehash Overhead KB,Overhead %" > "${LOG_DIR}/summary.csv"

# Test different bucket sizes (powers of 2) with default FP threshold
echo "=== Testing different bucket sizes with default FP threshold (0.05) ==="
run_test 64 0.05
sleep 10
run_test 128 0.05
sleep 10
run_test 256 0.05
sleep 10
run_test 512 0.05
sleep 10

# Test different FP thresholds with default bucket size
echo "=== Testing different FP thresholds with default bucket size (128) ==="
run_test 128 0.01
sleep 10
run_test 128 0.05
sleep 10
run_test 128 0.10
sleep 10
run_test 128 0.20
sleep 10
run_test 128 0.30
sleep 10

echo "=== Testing complete ==="
echo "All logs available in: ${LOG_DIR}"
echo "Summary report generated: ${LOG_DIR}/summary.csv"