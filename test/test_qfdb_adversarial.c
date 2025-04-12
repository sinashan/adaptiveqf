#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/time.h>
#include <time.h>
#include <sys/stat.h>
#include "qf_uthash.h"
#include "gqf_int.h"

// Test modes
#define TEST_MODE_BLOCK_BASED 1
#define TEST_MODE_NAIVE_REHASH 2

// Track false positive information for each bucket
typedef struct {
    uint64_t bucket_idx;
    uint64_t queries;
    uint64_t false_positives;
    double fp_rate;
} bucket_info_t;

// Test results structure to store metrics
typedef struct {
    double insert_throughput;
    double query_throughput;
    uint64_t false_positives;
    double false_positive_rate;
    uint64_t rehashing_operations;
    uint64_t rehashed_items;
    double avg_query_latency_us;
    double min_query_latency_us;
    double max_query_latency_us;
    double memory_kb;
    
    // Additional metrics for naive rehashing
    double avg_rehash_time_ms;
    double max_rehash_time_ms;
    double total_downtime_ms;
    uint64_t adaptive_slots_used;
    double adaptive_slot_ratio;
} test_results_t;

// To track rehashing events for naive rehash
typedef struct {
    uint64_t time_ms;          // When rehashing occurred
    uint64_t items_rehashed;   // Number of items rehashed
    double duration_ms;        // How long the rehash took
    double adaptive_ratio;     // Ratio of adaptive slots at trigger
} rehash_event_t;

#define MAX_REHASH_EVENTS 100

// Function to print memory usage statistics
void print_rehashing_memory_usage(const QFDB *qfdb) {
    if (!qfdb) return;
    
    // Count hashmap entries
    size_t hashmap_entries = 0;
    minirun_entry *entry, *tmp;
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        hashmap_entries++;
    }
    
    // Count bucket stats entries
    size_t bucket_entries = 0;
    bucket_stats *bucket, *btmp;
    HASH_ITER(hh, qfdb->buckets, bucket, btmp) {
        bucket_entries++;
    }
    
    // Calculate memory used by rehashing-specific structures
    size_t bucket_stats_size = bucket_entries * sizeof(bucket_stats);
    size_t hashmap_size = hashmap_entries * sizeof(minirun_entry);
    
    // Estimate UTHash overhead (approximately 32 bytes per entry)
    size_t uthash_overhead_buckets = bucket_entries * 32;
    size_t uthash_overhead_hashmap = hashmap_entries * 32;
    
    // Base QF size (for reference)
    size_t qf_size = (1ULL << qfdb->qf->metadata->quotient_bits) * 
                     ((qfdb->qf->metadata->bits_per_slot/8) + 1);
    
    // QFDB base structure size
    size_t qfdb_struct_size = sizeof(QFDB);
    
    // Size of QF metadata
    size_t qf_metadata_size = sizeof(quotient_filter_metadata);
    
    // Total rehashing memory overhead (excluding uthash)
    size_t rehashing_overhead = bucket_stats_size;
    
    // Total memory usage
    size_t total_memory = qf_size + 
                         qfdb_struct_size + qf_metadata_size + rehashing_overhead;
    
    printf("\nRehashing Memory Usage:\n");
    printf("Bucket stats:            %7.2f KB (%lu entries, avg %.1f bytes/entry)\n", 
           (bucket_stats_size + uthash_overhead_buckets) / 1024.0, 
           bucket_entries,
           bucket_entries > 0 ? (bucket_stats_size + uthash_overhead_buckets) / (double)bucket_entries : 0);
    printf("Hashmap:                 %7.2f MB (%lu entries, avg %.1f bytes/entry)\n", 
           (hashmap_size + uthash_overhead_hashmap) / (1024.0 * 1024.0), 
           hashmap_entries,
           hashmap_entries > 0 ? (hashmap_size + uthash_overhead_hashmap) / (double)hashmap_entries : 0);
    printf("QFDB structure:          %7.2f KB\n", qfdb_struct_size / 1024.0);
    printf("QF metadata:             %7.2f KB\n", qf_metadata_size / 1024.0);
    printf("--------------------------------\n");
    printf("Total rehashing overhead:  %7.2f KB (%.2f%% of QF size)\n", 
           rehashing_overhead / 1024.0,
           (rehashing_overhead * 100.0) / qf_size);
    printf("Total memory usage:      %7.2f MB\n", total_memory / (1024.0 * 1024.0));
    
    // Memory usage by bucket group sizes
    if (bucket_entries > 0) {
        printf("\nBucket Group Size Distribution:\n");
        
        // Count entries by bucket group size
        size_t groups_by_size[5] = {0}; // [<10, 10-99, 100-999, 1000-9999, ≥10000]
        
        HASH_ITER(hh, qfdb->buckets, bucket, btmp) {
            if (bucket->queries < 10) groups_by_size[0]++;
            else if (bucket->queries < 100) groups_by_size[1]++;
            else if (bucket->queries < 1000) groups_by_size[2]++;
            else if (bucket->queries < 10000) groups_by_size[3]++;
            else groups_by_size[4]++;
        }
        
        printf("Groups with <10 queries:     %lu (%.1f%%)\n", 
               groups_by_size[0], (groups_by_size[0] * 100.0) / bucket_entries);
        printf("Groups with 10-99 queries:   %lu (%.1f%%)\n", 
               groups_by_size[1], (groups_by_size[1] * 100.0) / bucket_entries);
        printf("Groups with 100-999 queries: %lu (%.1f%%)\n", 
               groups_by_size[2], (groups_by_size[2] * 100.0) / bucket_entries);
        printf("Groups with 1000-9999 queries: %lu (%.1f%%)\n", 
               groups_by_size[3], (groups_by_size[3] * 100.0) / bucket_entries);
        printf("Groups with ≥10000 queries:  %lu (%.1f%%)\n", 
               groups_by_size[4], (groups_by_size[4] * 100.0) / bucket_entries);
    }
}

// Compare function for qsort
int compare_fp_rates(const void *a, const void *b) {
    bucket_info_t *bucket_a = (bucket_info_t *)a;
    bucket_info_t *bucket_b = (bucket_info_t *)b;
    
    // Sort by false positive rate (highest first)
    if (bucket_b->fp_rate > bucket_a->fp_rate) return 1;
    if (bucket_b->fp_rate < bucket_a->fp_rate) return -1;
    
    // If rates are equal, prioritize buckets with more queries
    return (bucket_b->queries - bucket_a->queries);
}

void generate_timestamped_filename(char *buffer, size_t buffer_size, const char *prefix, 
    uint64_t qbits, uint64_t rbits, uint64_t num_ops) {
    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);

    // Format: prefix_qbits_rbits_numops_yyyymmddhhmmss.csv
    strftime(buffer + strlen(buffer), buffer_size - strlen(buffer), 
    "_%Y%m%d%H%M%S.csv", tm_info);

    // Insert parameters before the timestamp
    char temp[256];
    strcpy(temp, buffer + strlen(buffer) - 20); // Save the timestamp part
    sprintf(buffer + strlen(buffer) - 20, "_%lu_%lu_%lu%s", 
    qbits, rbits, num_ops, temp);
}

    // Modify the write_block_results_to_csv function
void write_block_results_to_csv(const char *filename, const test_results_t *results, 
        uint64_t qbits, uint64_t rbits, uint64_t bucket_size,
        double fp_threshold, double adv_pct, uint64_t num_ops) {
    char timestamped_filename[512] = "logs/block_adv";

    generate_timestamped_filename(timestamped_filename, sizeof(timestamped_filename),
    "block_adv", qbits, rbits, num_ops);

    FILE *csv_file;
    int file_exists = 0;

    // Check if file exists
    csv_file = fopen(timestamped_filename, "r");
    if (csv_file) {
    file_exists = 1;
    fclose(csv_file);
    }

    // Open file for appending
    csv_file = fopen(timestamped_filename, "a");
    if (!csv_file) {
    fprintf(stderr, "Error: Could not open file %s for writing\n", timestamped_filename);
    return;
    }

    // Write header if file is new
    if (!file_exists) {
    fprintf(csv_file, "timestamp,qbits,rbits,bucket_size,fp_threshold,adv_pct,"
    "insert_throughput,query_throughput,false_positives,fp_rate,"
    "rehashing_operations,rehashed_items,avg_latency_us,"
    "min_latency_us,max_latency_us,memory_kb\n");
    }

    // Get current timestamp
    time_t now = time(NULL);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", localtime(&now));

    // Write results row
    fprintf(csv_file, "%s,%lu,%lu,%lu,%.4f,%.1f,%.2f,%.2f,%lu,%.6f,%lu,%lu,%.2f,%.2f,%.2f,%.2f\n",
    timestamp, qbits, rbits, bucket_size, fp_threshold, adv_pct,
    results->insert_throughput, results->query_throughput,
    results->false_positives, results->false_positive_rate,
    results->rehashing_operations, results->rehashed_items,
    results->avg_query_latency_us, results->min_query_latency_us,
    results->max_query_latency_us, results->memory_kb);

    fclose(csv_file);
    printf("\nResults saved to %s\n", timestamped_filename);
    }

    // Modify the write_naive_results_to_csv function
void write_naive_results_to_csv(const char *filename, const test_results_t *results, 
    uint64_t qbits, uint64_t rbits, double rehash_threshold) {
    char timestamped_filename[512] = "logs/naive";
    generate_timestamped_filename(timestamped_filename, sizeof(timestamped_filename),
    "naive_rehash", qbits, rbits, 
    (uint64_t)(rehash_threshold * 1000));

    FILE *csv_file;
    int file_exists = 0;

    // Check if file exists
    csv_file = fopen(timestamped_filename, "r");
    if (csv_file) {
    file_exists = 1;
    fclose(csv_file);
    }

    // Open file for appending
    csv_file = fopen(timestamped_filename, "a");
    if (!csv_file) {
    fprintf(stderr, "Error: Could not open file %s for writing\n", timestamped_filename);
    return;
    }

    // Write header if file is new
    if (!file_exists) {
    fprintf(csv_file, "timestamp,qbits,rbits,rehash_threshold,"
    "insert_throughput,query_throughput,false_positives,fp_rate,"
    "rehashing_operations,rehashed_items,avg_latency_us,"
    "min_latency_us,max_latency_us,memory_kb,"
    "avg_rehash_time_ms,max_rehash_time_ms,total_downtime_ms,"
    "adaptive_slots_used,adaptive_slot_ratio\n");
    }

    // Get current timestamp
    time_t now = time(NULL);
    char timestamp[64];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", localtime(&now));

    // Write results row
    fprintf(csv_file, "%s,%lu,%lu,%.4f,%.2f,%.2f,%lu,%.6f,%lu,%lu,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%lu,%.6f\n",
    timestamp, qbits, rbits, rehash_threshold,
    results->insert_throughput, results->query_throughput,
    results->false_positives, results->false_positive_rate,
    results->rehashing_operations, results->rehashed_items,
    results->avg_query_latency_us, results->min_query_latency_us,
    results->max_query_latency_us, results->memory_kb,
    results->avg_rehash_time_ms, results->max_rehash_time_ms,
    results->total_downtime_ms,
    results->adaptive_slots_used, results->adaptive_slot_ratio);

    fclose(csv_file);
    printf("\nResults saved to %s\n", timestamped_filename);
}

// Print final test results - Block-based version
void print_block_test_results(const test_results_t *results, uint64_t qbits, uint64_t rbits,
                      uint64_t bucket_size, double fp_threshold, double adv_pct) {
    printf("\n==================== ADVERSARIAL TEST RESULTS ====================\n");
    printf("Configuration:\n");
    printf("  Implementation:     block_based\n");
    printf("  Filter size:        %llu slots (qbits=%lu, rbits=%lu)\n", 
           1ULL << qbits, qbits, rbits);
    printf("  Bucket size:        %lu\n", bucket_size);
    printf("  FP threshold:       %.4f\n", fp_threshold);
    printf("  Adversarial load:   %.1f%%\n", adv_pct);
    
    printf("\nPerformance:\n");
    printf("  Insert throughput:  %.2f ops/sec\n", results->insert_throughput);
    printf("  Query throughput:   %.2f ops/sec\n", results->query_throughput);
    
    printf("\nLatency Metrics:\n");
    printf("  Avg query latency:  %.2f us\n", results->avg_query_latency_us);
    printf("  Min query latency:  %.2f us\n", results->min_query_latency_us);
    printf("  Max query latency:  %.2f us\n", results->max_query_latency_us);
    
    printf("\nFalse Positives:\n");
    printf("  Total FPs:          %lu\n", results->false_positives);
    printf("  Final FP rate:      %.6f (%.4f%%)\n", 
           results->false_positive_rate, results->false_positive_rate * 100);
    
    if (results->rehashing_operations > 0) {
        printf("\nRehashing:\n");
        printf("  Rehash operations:  %lu\n", results->rehashing_operations);
        printf("  Rehashed items:     %lu\n", results->rehashed_items);
    }
    
    printf("\nMemory Usage:\n");
    printf("  Base QF memory:     %.2f KB\n", results->memory_kb);
    printf("  Memory per slot:    %.2f bytes\n", results->memory_kb * 1024 / (1ULL << qbits));
    printf("================================================================\n");
}

// Print final test results - Naive rehash version
void print_naive_test_results(const test_results_t *results, uint64_t qbits, uint64_t rbits,
                      double rehash_threshold) {
    printf("\n==================== NAIVE REHASHING TEST RESULTS ====================\n");
    printf("Configuration:\n");
    printf("  Implementation:     naive_rehashing\n");
    printf("  Filter size:        %llu slots (qbits=%lu, rbits=%lu)\n", 
           1ULL << qbits, qbits, rbits);
    printf("  Rehash threshold:   %.2f%% of slots\n", rehash_threshold * 100);
    
    printf("\nPerformance:\n");
    printf("  Insert throughput:  %.2f ops/sec\n", results->insert_throughput);
    printf("  Query throughput:   %.2f ops/sec\n", results->query_throughput);
    
    printf("\nLatency Metrics:\n");
    printf("  Avg query latency:  %.2f us\n", results->avg_query_latency_us);
    printf("  Min query latency:  %.2f us\n", results->min_query_latency_us);
    printf("  Max query latency:  %.2f us\n", results->max_query_latency_us);
    
    printf("\nFalse Positives:\n");
    printf("  Total FPs:          %lu\n", results->false_positives);
    printf("  Final FP rate:      %.6f (%.4f%%)\n", 
           results->false_positive_rate, results->false_positive_rate * 100);
    
    printf("\nRehashing Metrics:\n");
    printf("  Rehash operations:  %lu\n", results->rehashing_operations);
    printf("  Rehashed items:     %lu\n", results->rehashed_items);
    printf("  Avg rehash time:    %.2f ms\n", results->avg_rehash_time_ms);
    printf("  Max rehash time:    %.2f ms\n", results->max_rehash_time_ms);
    printf("  Total downtime:     %.2f ms\n", results->total_downtime_ms);
    
    printf("\nAdaptive Slot Usage:\n");
    printf("  Adaptive slots:     %lu\n", results->adaptive_slots_used);
    printf("  Adaptive ratio:     %.6f (%.4f%%)\n", 
           results->adaptive_slot_ratio, results->adaptive_slot_ratio * 100);
    
    printf("\nMemory Usage:\n");
    printf("  Base QF memory:     %.2f KB\n", results->memory_kb);
    printf("  Memory per slot:    %.2f bytes\n", results->memory_kb * 1024 / (1ULL << qbits));
    printf("==================================================================\n");
}

// Estimate the current adaptive slot usage
uint64_t get_adaptive_slot_usage(const QFDB *qfdb) {
    return qfdb->adaptations_performed;
}


double naive_rehash_qfdb(QFDB *qfdb) {
    struct timeval start_time, end_time;
    gettimeofday(&start_time, NULL);
    
    // 1. Collect all keys from the QFDB
    size_t key_count = 0;
    minirun_entry *entry, *tmp;
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        key_count++;
    }
    
    if (key_count == 0) {
        gettimeofday(&end_time, NULL);
        return (end_time.tv_sec - start_time.tv_sec) * 1000.0 + 
               (end_time.tv_usec - start_time.tv_usec) / 1000.0;
    }
    
    uint64_t *keys = (uint64_t*)malloc(key_count * sizeof(uint64_t));
    if (!keys) return -1.0;
    
    size_t i = 0;
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        keys[i++] = entry->original_key;
    }
    
    // 2. Reset the QF with a new seed
    qf_reset(qfdb->qf);
    qfdb->qf->metadata->seed = qfdb->qf->metadata->seed ^ 0x5555555555555555ULL;
    
    // 3. Clear all the hash tables
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        HASH_DEL(qfdb->hashmap, entry);
        free(entry);
    }
    qfdb->hashmap = NULL;
    
    bucket_stats *bucket, *btmp;
    HASH_ITER(hh, qfdb->buckets, bucket, btmp) {
        HASH_DEL(qfdb->buckets, bucket);
        free(bucket);
    }
    qfdb->buckets = NULL;
    
    // 4. Reinsert all the keys
    for (i = 0; i < key_count; i++) {
        qfdb_insert(qfdb, keys[i], 1);
    }
    
    free(keys);
    
    // 5. Reset adaptation counters
    qfdb->adaptations_performed = 0;
    qfdb->fp_rehashes = 0;
    
    // 6. Record the time taken
    gettimeofday(&end_time, NULL);
    double duration_ms = (end_time.tv_sec - start_time.tv_sec) * 1000.0 + 
                         (end_time.tv_usec - start_time.tv_usec) / 1000.0;
    
    qfdb->rehashing_operations++;
    qfdb->rehashed_items += key_count;
    
    return duration_ms;
}

void print_usage() {
    printf("Usage: test_qfdb_adversarial <mode> [mode-specific arguments]\n");
    printf("\nAvailable modes:\n");
    printf("  block    Run block-based adversarial test (targeted attacks on specific buckets)\n");
    printf("  naive    Run naive rehashing test (global rehashing with downtime)\n");
    printf("\nMode-specific arguments:\n");
    printf("For block mode:\n");
    printf("  test_qfdb_adversarial block <qbits> <rbits> <num_queries> <attack_pct> [options]\n");
    printf("    qbits: log2 of number of slots in filter\n");
    printf("    rbits: number of remainder bits per slot\n");
    printf("    num_queries: number of total queries to perform\n");
    printf("    attack_pct: percentage of queries to dedicate to adversarial attack (0-100)\n");
    printf("  Options:\n");
    printf("    --verbose                  Show detailed output\n");
    printf("    --bucket-size <size>       Set bucket group size (default: 128)\n");
    printf("    --fp-threshold <rate>      Set false positive threshold (default: 0.05)\n");
    printf("    --output <filename>        Output CSV file (default: logs/adversarial_results.csv)\n");
    printf("    --seed <value>             Random seed value\n");
    printf("\nFor naive mode:\n");
    printf("  test_qfdb_adversarial naive <qbits> <rbits> <num_ops> <rehash_threshold> [options]\n");
    printf("    qbits: log2 of number of slots in filter\n");
    printf("    rbits: number of remainder bits per slot\n");
    printf("    num_ops: number of operations to perform\n");
    printf("    rehash_threshold: ratio of adaptive slots to trigger rehashing (0.0-1.0)\n");
    printf("  Options:\n");
    printf("    --verbose                 Print detailed operation information\n");
    printf("    --output <filename>       Output CSV file (default: logs/naive_rehash_results.csv)\n");
    printf("    --seed <value>            Random seed value\n");
}

// Run the block-based adversarial test
int run_block_based_test(int argc, char *argv[]) {
    if (argc < 6) {
        printf("Usage: %s block <qbits> <rbits> <num_queries> <attack_pct> [options]\n", argv[0]);
        return 1;
    }
    
    uint64_t qbits = atoi(argv[2]);
    uint64_t rbits = atoi(argv[3]);
    uint64_t num_queries = strtoull(argv[4], NULL, 10);
    uint64_t attack_pct = atoi(argv[5]);
    int verbose = 0;
    uint64_t bucket_size = 128;  // Default bucket group size
    double fp_threshold = 0.05;  // Default threshold
    uint64_t seed = time(NULL);  // Default seed is current time
    char output_file[256] = "logs/adversarial_results.csv";
    
    // Parse optional arguments
    for (int i = 6; i < argc; i++) {
        if (strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
        } else if (strcmp(argv[i], "--bucket-size") == 0 && i + 1 < argc) {
            bucket_size = strtoull(argv[i+1], NULL, 10);
            i++;
        } else if (strcmp(argv[i], "--fp-threshold") == 0 && i + 1 < argc) {
            fp_threshold = atof(argv[i+1]);
            i++;
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            strncpy(output_file, argv[i+1], sizeof(output_file) - 1);
            i++;
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            seed = strtoull(argv[i+1], NULL, 10);
            i++;
        }
    }
    
    if (attack_pct > 100) {
        printf("Warning: attack_pct must be between 0-100, setting to 100\n");
        attack_pct = 100;
    }
    
    // Set random seed
    srand(seed);
    printf("Using random seed: %lu\n", seed);
    
    // Create logs directory if needed
    mkdir("logs", 0777);
    
    // Calculate number of buckets and adversarial queries
    uint64_t num_buckets = 1ULL << qbits;
    uint64_t num_adv_queries = (num_queries * attack_pct) / 100;
    uint64_t num_normal_queries = num_queries - num_adv_queries;
    
    printf("\nUsing implementation: block_based\n");
    
    printf("\nRunning adversarial test with:\n");
    printf("  QF size:           %llu slots (qbits=%lu, rbits=%lu)\n", 
           1ULL << qbits, qbits, rbits);
    printf("  Bucket size:       %lu buckets\n", bucket_size);
    printf("  FP threshold:      %.4f\n", fp_threshold);
    printf("  Queries:           %lu\n", num_queries);
    printf("  Adversarial load:  %.1f%% of queries\n", (double)attack_pct);
    printf("  Output file:       %s\n", output_file);
    
    // Initialize QFDB
    QFDB *qfdb = qfdb_init_extended(qbits, rbits, bucket_size, fp_threshold);
    if (!qfdb) {
        fprintf(stderr, "Failed to initialize QFDB\n");
        return 1;
    }
    
    if (verbose) {
        printf("Initialized QFDB with:\n");
        printf("  Implementation:    block_based\n");
        printf("  qbits:             %lu\n", qbits);
        printf("  rbits:             %lu\n", rbits);
        printf("  bucket_size:       %lu\n", bucket_size);
        printf("  fp_threshold:      %.4f\n", fp_threshold);
    }
    
    // Calculate number of items to insert (90% load factor)
    uint64_t num_inserts = (num_buckets * 9) / 10;
    printf("Inserting %lu items (90%% load factor)...\n", num_inserts);
    
    // Initialize result structure
    test_results_t results = {0};
    results.min_query_latency_us = UINT64_MAX;
    
    // Allocate memory for keys and latency measurements
    uint64_t *insert_keys = malloc(num_inserts * sizeof(uint64_t));
    uint64_t *query_latencies = malloc(num_queries * sizeof(uint64_t));
    
    if (!insert_keys || !query_latencies) {
        fprintf(stderr, "Failed to allocate memory\n");
        if (insert_keys) free(insert_keys);
        if (query_latencies) free(query_latencies);
        qfdb_destroy(qfdb);
        return 1;
    }
    
    // Timing variables
    struct timeval tv;
    uint64_t start_time, end_time;
    
    // Start timing insert phase
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    // Insert random items
    for (uint64_t i = 0; i < num_inserts; i++) {
        // Generate random key
        insert_keys[i] = ((uint64_t)rand() << 32) | rand();
        
        // Insert into filter
        qfdb_insert(qfdb, insert_keys[i], 1);
        
        // Show progress every 10%
        if (i % (num_inserts / 10) == 0) {
            printf("  Inserted %lu items (%.1f%%)\n", i, (double)i * 100 / num_inserts);
        }
    }
    
    // End timing insert phase
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    // Calculate insertion performance metrics
    double insert_time = (double)(end_time - start_time) / 1000000;
    results.insert_throughput = num_inserts / insert_time;
    
    printf("\nInsertion Performance:\n");
    printf("  Successful inserts:   %lu of %lu\n", num_inserts, num_inserts);
    printf("  Insert time:          %.3f seconds\n", insert_time);
    printf("  Insert throughput:    %.2f ops/sec\n", results.insert_throughput);
    printf("  CPU time for inserts: %.3f seconds\n", insert_time);
    
    // Get rehashing stats before query phase
    uint64_t rehashing_ops_before = 0;
    uint64_t rehashed_items_before = 0;
    qfdb_get_rehash_stats(qfdb, &rehashing_ops_before, &rehashed_items_before, NULL);
    
    if (verbose) {
        printf("Bucket Group Statistics:\n");
        print_bucket_stats(qfdb);
    }
    
    // Phase 1: Perform random queries to discover vulnerable buckets
    printf("\nPhase 1: Performing %lu random queries to discover vulnerable buckets...\n", num_normal_queries);
    
    // Allocate array to track bucket information
    bucket_info_t *bucket_data = calloc(num_buckets, sizeof(bucket_info_t));
    if (!bucket_data) {
        fprintf(stderr, "Failed to allocate memory for bucket data\n");
        free(insert_keys);
        free(query_latencies);
        qfdb_destroy(qfdb);
        return 1;
    }
    
    // Initialize bucket data
    for (uint64_t i = 0; i < num_buckets; i++) {
        bucket_data[i].bucket_idx = i;
    }
    
    // Perform random queries
    uint64_t total_fps = 0;
    uint64_t total_query_time = 0;
    
    for (uint64_t i = 0; i < num_normal_queries; i++) {
        // Generate random key that wasn't inserted
        uint64_t random_key = ((uint64_t)rand() << 32) | rand();
        
        // Record fp_rehashes before query
        uint64_t fp_before = qfdb->fp_rehashes;
        
        // Measure query time
        gettimeofday(&tv, NULL);
        uint64_t query_start = tv.tv_sec * 1000000 + tv.tv_usec;
        
        // Perform query
        (void)qfdb_query(qfdb, random_key);  // Cast to void to suppress warning
        
        gettimeofday(&tv, NULL);
        uint64_t query_end = tv.tv_sec * 1000000 + tv.tv_usec;
        uint64_t query_time = query_end - query_start;
        
        // Update latency metrics
        query_latencies[i] = query_time;
        total_query_time += query_time;
        
        if (query_time < results.min_query_latency_us)
            results.min_query_latency_us = query_time;
        
        if (query_time > results.max_query_latency_us)
            results.max_query_latency_us = query_time;
        
        // Check if it was a false positive
        if (qfdb->fp_rehashes > fp_before) {
            total_fps++;
            
            // Determine which bucket this key maps to
            uint64_t bucket_idx = (random_key >> rbits) & (num_buckets - 1);
            
            // Update bucket statistics
            bucket_data[bucket_idx].queries++;
            bucket_data[bucket_idx].false_positives++;
            bucket_data[bucket_idx].fp_rate = (double)bucket_data[bucket_idx].false_positives / 
                                             bucket_data[bucket_idx].queries;
        }
        
        // Show progress every 10%
        if (i % (num_normal_queries / 10) == 0) {
            printf("  Completed %lu random queries (%.1f%%), current FP rate: %.4f%%, avg latency: %.2f us\n", 
                   i, (double)i * 100 / num_normal_queries, 
                   (double)total_fps * 100 / (i + 1),
                   (double)total_query_time / (i + 1));
        }
    }
    
    printf("Random query phase complete with %.4f%% false positive rate\n", 
           (double)total_fps * 100 / num_normal_queries);
    
    // Sort buckets by false positive rate
    qsort(bucket_data, num_buckets, sizeof(bucket_info_t), compare_fp_rates);
    
    // Print top vulnerable buckets
    printf("\nTop 10 most vulnerable buckets:\n");
    printf("  Bucket ID  |  Queries  |  False Positives  |  FP Rate\n");
    printf("  --------------------------------------------------\n");
    
    int num_vulnerable = 0;
    for (uint64_t i = 0; i < num_buckets && num_vulnerable < 10; i++) {
        if (bucket_data[i].queries > 0) {
            printf("  %9lu  |  %7lu  |  %15lu  |  %.4f\n", 
                   bucket_data[i].bucket_idx, 
                   bucket_data[i].queries,
                   bucket_data[i].false_positives,
                   bucket_data[i].fp_rate);
            num_vulnerable++;
        }
    }
    
    // Phase 2: Targeted attack on vulnerable buckets
    printf("\nPhase 2: Performing %lu targeted queries against vulnerable buckets...\n", 
           num_adv_queries);
    
    // Select top vulnerable buckets (those with FP rate > 0)
    uint64_t vulnerable_count = 0;
    for (uint64_t i = 0; i < num_buckets; i++) {
        if (bucket_data[i].fp_rate > 0) {
            vulnerable_count++;
        }
    }
    
    if (vulnerable_count == 0) {
        printf("No vulnerable buckets found! Using random buckets instead.\n");
        vulnerable_count = 10; // Just use first 10 buckets if none are vulnerable
    } else {
        printf("Found %lu vulnerable buckets to target\n", vulnerable_count);
    }
    
    // Keep track of FP rates after targeted attack
    uint64_t adv_fps = 0;
    uint64_t fp_before_attack = qfdb->fp_rehashes;
    
    // Track rehashing operations
    uint64_t rehash_ops_before, rehashed_items_before2;
    qfdb_get_rehash_stats(qfdb, &rehash_ops_before, &rehashed_items_before2, NULL);
    
    // Start timing adversarial query phase
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    // Generate and query with adversarial keys
    for (uint64_t i = 0; i < num_adv_queries; i++) {
        // Pick a target bucket (rotate through vulnerable buckets)
        uint64_t target_idx = i % vulnerable_count;
        uint64_t target_bucket = bucket_data[target_idx].bucket_idx;
        
        // Create an adversarial key that targets this bucket
        uint64_t r_mask = (1ULL << rbits) - 1;
        uint64_t remainder = rand() & r_mask;
        uint64_t adv_key = (target_bucket << rbits) | remainder;
        
        // Make sure this is different from any inserted key by flipping high bit
        adv_key |= (1ULL << 63);
        
        // Record FP count before query
        uint64_t fp_before = qfdb->fp_rehashes;
        
        // Measure query time
        gettimeofday(&tv, NULL);
        uint64_t query_start = tv.tv_sec * 1000000 + tv.tv_usec;
        
        // Perform query
        (void)qfdb_query(qfdb, adv_key);  // Cast to void to suppress warning
        
        gettimeofday(&tv, NULL);
        uint64_t query_end = tv.tv_sec * 1000000 + tv.tv_usec;
        uint64_t query_time = query_end - query_start;
        
        // Update latency metrics
        query_latencies[num_normal_queries + i] = query_time;
        total_query_time += query_time;
        
        if (query_time < results.min_query_latency_us)
            results.min_query_latency_us = query_time;
        
        if (query_time > results.max_query_latency_us)
            results.max_query_latency_us = query_time;
        
        // Check if it was a false positive
        if (qfdb->fp_rehashes > fp_before) {
            adv_fps++;
        }
        
        // Show progress every 10%
        if (i % (num_adv_queries / 10) == 0) {
            printf("  Completed %lu adversarial queries (%.1f%%), current FP rate: %.4f%%\n", 
                   i, (double)i * 100 / num_adv_queries, 
                   (double)adv_fps * 100 / (i + 1));
        }
    }
    
    // End timing adversarial query phase
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    // Calculate performance metrics
    double query_time = (double)(end_time - start_time) / 1000000;
    results.query_throughput = num_queries / query_time;
    results.avg_query_latency_us = (double)total_query_time / num_queries;
    
    // Get final statistics
    uint64_t fp_after_attack = qfdb->fp_rehashes;
    uint64_t rehash_ops_after, rehashed_items_after;
    qfdb_get_rehash_stats(qfdb, &rehash_ops_after, &rehashed_items_after, NULL);
    
    results.false_positives = fp_after_attack - fp_before_attack;
    results.false_positive_rate = (double)results.false_positives / num_queries;
    results.rehashing_operations = rehash_ops_after - rehash_ops_before;
    results.rehashed_items = rehashed_items_after - rehashed_items_before2;
    
    // Calculate memory usage
    size_t qf_size = (1ULL << qbits) * ((rbits/8) + 1);
    results.memory_kb = qf_size / 1024.0;
    
    printf("\nResults of adversarial attack:\n");
    printf("  Total false positives from attack: %lu of %lu queries (%.4f%%)\n", 
           adv_fps, num_adv_queries, (double)adv_fps * 100 / num_adv_queries);
    printf("  Overall false positives: %lu (before: %lu, during attack: %lu)\n", 
           fp_after_attack, fp_before_attack, adv_fps);
    printf("  Rehashing operations: %lu (new: %lu)\n", 
           rehash_ops_after, rehash_ops_after - rehash_ops_before);
    printf("  Items rehashed: %lu\n", rehashed_items_after - rehashed_items_before2);
    
    // Print final bucket statistics
    printf("\nFilter Statistics After Adversarial Queries:\n");
    print_bucket_stats(qfdb);
    
    if (verbose) {
        print_rehashing_memory_usage(qfdb);
    }
    
    // Print summary results
    print_block_test_results(&results, qbits, rbits, bucket_size, fp_threshold, (double)attack_pct);
    
    // Write results to CSV file
    write_block_results_to_csv(output_file, &results, qbits, rbits, bucket_size, 
        fp_threshold, (double)attack_pct, num_queries);
    
    // Clean up
    free(bucket_data);
    free(insert_keys);
    free(query_latencies);
    qfdb_destroy(qfdb);
    
    return 0;
}

// Run the naive rehashing test
int run_naive_rehash_test(int argc, char *argv[]) {
    if (argc < 6) {
        printf("Usage: %s naive <qbits> <rbits> <num_ops> <rehash_threshold> [options]\n", argv[0]);
        return 1;
    }
    
    uint64_t qbits = atoll(argv[2]);
    uint64_t rbits = atoll(argv[3]);
    uint64_t num_ops = atoll(argv[4]);
    double rehash_threshold = atof(argv[5]);
    int verbose = 0;
    uint64_t seed = time(NULL);  // Default seed is current time
    char output_file[256] = "logs/naive_rehash_results.csv";
    
    // Parse optional arguments
    for (int i = 6; i < argc; i++) {
        if (strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
        } else if (strcmp(argv[i], "--output") == 0 && i + 1 < argc) {
            strncpy(output_file, argv[i+1], sizeof(output_file) - 1);
            i++;
        } else if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc) {
            seed = atoll(argv[i+1]);
            i++;
        }
    }
    
    // Validate input
    if (rehash_threshold <= 0.0 || rehash_threshold >= 1.0) {
        fprintf(stderr, "Error: rehash_threshold must be between 0.0 and 1.0\n");
        return 1;
    }
    
    // Set random seed
    srand(seed);
    printf("Using random seed: %lu\n", seed);
    
    // Create logs directory if needed
    mkdir("logs", 0777);
    
    // Initialize the QFDB
    QFDB *qfdb = qfdb_init(qbits, rbits);
    if (!qfdb) {
        fprintf(stderr, "Failed to initialize QFDB\n");
        return 1;
    }
    
    printf("Initialized QFDB with %lu qbits and %lu rbits\n", qbits, rbits);
    printf("Naive rehashing threshold: %.4f (%.2f%% of slots)\n", 
          rehash_threshold, rehash_threshold * 100);
    
    // Calculate total slots
    uint64_t total_slots = 1ULL << qbits;
    uint64_t adaptive_threshold = (uint64_t)(total_slots * rehash_threshold);
    
    printf("Filter size: %llu slots\n", total_slots);
    printf("Adaptive threshold: %lu slots\n", adaptive_threshold);
    
    // Initialize tracking arrays
    uint64_t *keys = malloc(num_ops * sizeof(uint64_t));
    if (!keys) {
        fprintf(stderr, "Failed to allocate memory for keys\n");
        qfdb_destroy(qfdb);
        return 1;
    }
    
    // Initialize timing variables
    struct timeval tv;
    uint64_t start_time, end_time;
    uint64_t query_time_total = 0;
    uint64_t min_query_time = UINT64_MAX;
    uint64_t max_query_time = 0;
    
    // Track rehashing events
    rehash_event_t rehash_events[MAX_REHASH_EVENTS] = {0};
    int rehash_count = 0;
    double total_rehash_time = 0.0;
    double max_rehash_time = 0.0;
    
    // Initialize operations
    int is_query = 0;  // Whether to perform insert (0) or query (1)
    uint64_t total_inserts = 0;
    uint64_t fp_count = 0;
    
    // Start timing
    clock_t start_clock = clock();
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    printf("\nStarting mixed operation test (%lu operations)...\n", num_ops);
    
    // Main operation loop
    for (uint64_t i = 0; i < num_ops; i++) {
        // Determine operation type (60% insert, 40% query)
        is_query = (rand() % 100) < 40;
        
        // Generate a key
        uint64_t key = ((uint64_t)rand() << 32) | rand();
        keys[i] = key;
        
        // Check if we need to rehash before proceeding
        uint64_t adaptive_slots = get_adaptive_slot_usage(qfdb);
        double adaptive_ratio = (double)adaptive_slots / total_slots;
        
        if (adaptive_slots >= adaptive_threshold) {
            if (verbose) {
                printf("\nRehashing triggered at operation %lu\n", i);
                printf("Adaptive slots: %lu (%.4f%% of total)\n", 
                       adaptive_slots, adaptive_ratio * 100);
            }
            
            // Perform naive rehashing with downtime
            double rehash_time = naive_rehash_qfdb(qfdb);
            
            // Record rehashing event
            if (rehash_count < MAX_REHASH_EVENTS) {
                rehash_events[rehash_count].time_ms = 
                    (tv.tv_sec * 1000) + (tv.tv_usec / 1000);
                rehash_events[rehash_count].items_rehashed = total_inserts;
                rehash_events[rehash_count].duration_ms = rehash_time;
                rehash_events[rehash_count].adaptive_ratio = adaptive_ratio;
                rehash_count++;
            }
            
            // Update rehashing statistics
            total_rehash_time += rehash_time;
            if (rehash_time > max_rehash_time) {
                max_rehash_time = rehash_time;
            }
            
            if (verbose) {
                printf("Rehashing completed in %.2f ms\n", rehash_time);
            }
        }
        
        // Measure individual operation time
        gettimeofday(&tv, NULL);
        uint64_t op_start = tv.tv_sec * 1000000 + tv.tv_usec;
        
        // Perform the operation
        if (is_query) {
            // Record false positive count before query
            uint64_t fp_before = qfdb->fp_rehashes;
            
            // Perform query
            int result = qfdb_query(qfdb, key);
            
            // Check if it was a false positive
            if (qfdb->fp_rehashes > fp_before) {
                fp_count++;
            }
            
            if (verbose && i < 10) {
                printf("Query %lu: key=%lu, result=%d\n", i, key, result);
            }
        } else {
            // Perform insert
            int result = qfdb_insert(qfdb, key, 1);
            if (result >= 0) {
                total_inserts++;
            }
            
            if (verbose && i < 10) {
                printf("Insert %lu: key=%lu, result=%d\n", i, key, result);
            }
        }
        
        // Measure operation time
        gettimeofday(&tv, NULL);
        uint64_t op_end = tv.tv_sec * 1000000 + tv.tv_usec;
        uint64_t op_time = op_end - op_start;
        
        // Update query time statistics
        query_time_total += op_time;
        if (op_time < min_query_time) min_query_time = op_time;
        if (op_time > max_query_time) max_query_time = op_time;
        
        // Show progress
        if (i % (num_ops / 10) == 0) {
            printf("  Completed %lu operations (%.1f%%)\n", 
                   i, (double)i * 100 / num_ops);
        }
    }
    
    // End timing
    clock_t end_clock = clock();
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    // Calculate final adaptive slot usage
    uint64_t final_adaptive_slots = get_adaptive_slot_usage(qfdb);
    double final_adaptive_ratio = (double)final_adaptive_slots / total_slots;
    
    // Collect query statistics
    uint64_t total_queries, verified_queries, fp_rehashes;
    double fp_rate;
    qfdb_get_stats(qfdb, &total_queries, &verified_queries, &fp_rehashes, NULL, NULL, &fp_rate);
    
    // Collect rehashing statistics
    uint64_t rehashing_operations, rehashed_items;
    qfdb_get_rehash_stats(qfdb, &rehashing_operations, &rehashed_items, NULL);
    
    // Prepare test results
    test_results_t results = {0};
    
    // Calculate runtime in seconds
    double total_runtime = (double)(end_time - start_time) / 1000000.0;
    double cpu_runtime = (double)(end_clock - start_clock) / CLOCKS_PER_SEC;
    
    // Fill in results structure
    results.insert_throughput = total_inserts / total_runtime;
    results.query_throughput = total_queries / total_runtime;
    results.false_positives = fp_rehashes;
    results.false_positive_rate = fp_rate;
    results.rehashing_operations = rehashing_operations;
    results.rehashed_items = rehashed_items;
    results.avg_query_latency_us = (double)query_time_total / num_ops;
    results.min_query_latency_us = min_query_time;
    results.max_query_latency_us = max_query_time;
    
    // Memory usage estimation
    size_t qf_size = (1ULL << qbits) * ((rbits/8) + 1);
    results.memory_kb = qf_size / 1024.0;
    
    // Naive rehashing specific metrics
    results.avg_rehash_time_ms = rehash_count > 0 ? total_rehash_time / rehash_count : 0;
    results.max_rehash_time_ms = max_rehash_time;
    results.total_downtime_ms = total_rehash_time;
    results.adaptive_slots_used = final_adaptive_slots;
    results.adaptive_slot_ratio = final_adaptive_ratio;
    
    // Print overall performance
    printf("\nTest completed in %.3f seconds (CPU: %.3f seconds)\n", 
           total_runtime, cpu_runtime);
    printf("Operations: %lu total, %lu inserts, %lu queries\n",
           num_ops, total_inserts, total_queries);
    printf("Rehashing: %d operations, %.2f ms average, %.2f ms total downtime\n",
           rehash_count, results.avg_rehash_time_ms, results.total_downtime_ms);
    
    // Print detailed rehashing events if verbose
    if (verbose && rehash_count > 0) {
        printf("\nRehashing Events:\n");
        printf("  #  |  Items  |  Adaptive %%  |  Duration (ms)\n");
        printf("  -------------------------------------\n");
        
        for (int i = 0; i < rehash_count; i++) {
            printf("  %2d |  %6lu  |    %6.2f%%   |     %7.2f\n",
                   i+1, rehash_events[i].items_rehashed,
                   rehash_events[i].adaptive_ratio * 100,
                   rehash_events[i].duration_ms);
        }
    }
    
    print_naive_test_results(&results, qbits, rbits, rehash_threshold);
    
    write_naive_results_to_csv(output_file, &results, qbits, rbits, rehash_threshold);
    
    free(keys);
    qfdb_destroy(qfdb);
    
    return 0;
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        print_usage();
        return 1;
    }
    
    const char *mode = argv[1];
    
    // Determine which test to run
    if (strcmp(mode, "block") == 0) {
        return run_block_based_test(argc, argv);
    } else if (strcmp(mode, "naive") == 0) {
        return run_naive_rehash_test(argc, argv);
    } else {
        printf("Unknown test mode: %s\n", mode);
        print_usage();
        return 1;
    }
}