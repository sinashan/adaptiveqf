#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "qf_uthash.h" 
#include <sys/time.h>
#include <time.h>
#include <string.h>

#include "gqf.h"
#include "gqf_int.h"  // Add this include for internal QF structures
#include "qf_uthash.h"


void print_usage(const char* program_name) {
    fprintf(stderr, "Usage: %s <qbits> <rbits> <num_ops> [options]\n", program_name);
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  --verbose                 Print detailed operation information\n");
    fprintf(stderr, "  --bucket-size <size>      Set bucket group size (default: 128)\n");
    fprintf(stderr, "  --fp-threshold <rate>     Set false positive threshold (default: 0.05)\n");
    fprintf(stderr, "  --rehash-test             Run rehashing performance test\n");
    fprintf(stderr, "  --help                    Show this help message\n");
}

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
    printf("Bucket stats:          %7.2f KB (%lu entries, avg %.1f bytes/entry)\n", 
           (bucket_stats_size + uthash_overhead_buckets) / 1024.0, 
           bucket_entries,
           bucket_entries > 0 ? (bucket_stats_size + uthash_overhead_buckets) / (double)bucket_entries : 0);
    printf("Hashmap:               %7.2f MB (%lu entries, avg %.1f bytes/entry)\n", 
           (hashmap_size + uthash_overhead_hashmap) / (1024.0 * 1024.0), 
           hashmap_entries,
           hashmap_entries > 0 ? (hashmap_size + uthash_overhead_hashmap) / (double)hashmap_entries : 0);
    printf("QFDB structure:        %7.2f KB\n", qfdb_struct_size / 1024.0);
    printf("QF metadata:           %7.2f KB\n", qf_metadata_size / 1024.0);
    printf("--------------------------------\n");
    printf("Total rehashing overhead: %7.2f KB (%.2f%% of QF size)\n", 
           rehashing_overhead / 1024.0,
           (rehashing_overhead * 100.0) / qf_size);
    printf("Total memory usage:    %7.2f MB\n", total_memory / (1024.0 * 1024.0));
    
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

int main(int argc, char *argv[]) {
    if (argc < 4) {
        print_usage(argv[0]);
        return 1;
    }

    uint64_t qbits = atoll(argv[1]);
    uint64_t rbits = atoll(argv[2]);
    uint64_t num_ops = atoll(argv[3]);
    int verbose = 0;
    int rehash_test = 0;
    uint64_t bucket_size = DEFAULT_BUCKET_GROUP_SIZE;
    double fp_threshold = DEFAULT_REHASH_THRESHOLD;

    // Parse optional arguments
    for (int i = 4; i < argc; i++) {
        if (strcmp(argv[i], "--verbose") == 0) {
            verbose = 1;
        } else if (strcmp(argv[i], "--rehash-test") == 0) {
            rehash_test = 1;
        } else if (strcmp(argv[i], "--bucket-size") == 0 && i + 1 < argc) {
            bucket_size = atoll(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "--fp-threshold") == 0 && i + 1 < argc) {
            fp_threshold = atof(argv[i + 1]);
            i++;
        } else if (strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    // Initialize the QFDB with specified parameters
    QFDB *qfdb = qfdb_init_extended(qbits, rbits, bucket_size, fp_threshold);
    if (!qfdb) {
        fprintf(stderr, "Failed to initialize QFDB\n");
        return 1;
    }

    printf("Initialized QFDB with %lu qbits and %lu rbits\n", qbits, rbits);
    printf("Rehashing parameters: bucket_size=%lu, fp_threshold=%.4f\n", 
           bucket_size, fp_threshold);

    uint64_t *keys = NULL;
    srand(time(NULL));

    // We'll save the keys just for testing purposes later
    keys = (uint64_t *)malloc(num_ops * sizeof(uint64_t));
    if (!keys) {
        fprintf(stderr, "Failed to allocate memory for keys\n");
        qfdb_destroy(qfdb);
        return 1;
    }

    // Timing variables
    struct timeval tv;
    clock_t start_clock, end_clock;
    uint64_t start_time, end_time;
    uint64_t num_updates = 0;

    // Start timing before insertions
    start_clock = clock();
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;

    printf("Inserting %lu keys...\n", num_ops);
    for (int i = 0; i < num_ops; i++) {
        if (i % 2 == 0) {
            keys[i] = 1000000 + i;
        } else {
            keys[i] = ((uint64_t)rand() << 32) | rand();
        }

        int ret = qfdb_insert(qfdb, keys[i], 1);
        if (ret >= 0) num_updates++;

        if (verbose && (i < 5 || i % 1000 == 0)) {
            printf("Inserted item %d: key=%lu, result=%d\n", i, keys[i], ret);
        }
    }

    // End timing after insertions
    end_clock = clock();
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec * 1000000 + tv.tv_usec;

    // Print performance stats
    printf("\nInsertion Performance:\n");
    printf("Number of inserts:     %lu\n", num_ops);
    printf("Successful inserts:    %lu\n", num_updates);
    printf("Time for inserts:      %.3f sec\n", (double)(end_time - start_time) / 1000000);
    printf("Insert throughput:     %.2f ops/sec\n", (double)num_ops * 1000000 / (end_time - start_time));
    printf("CPU time for inserts:  %.3f sec\n", (double)(end_clock - start_clock) / CLOCKS_PER_SEC);

    // Print hashmap statistics after insertion
    print_hashmap_stats(qfdb);

    // Print bucket statistics (new!)
    if (verbose) {
        printf("\nBucket Statistics After Insertion:\n");
        print_bucket_stats(qfdb);
    }

    // Start timing for exact queries
    start_clock = clock();
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;

    // 1. Query for the exact same keys we inserted (should be 100% hits)
    printf("\nPhase 1: Querying for the exact keys we inserted (should be 100%% hits)...\n");
    int exact_hits = 0;
    for (int i = 0; i < num_ops; i++) {
        int result = qfdb_query(qfdb, keys[i]);
        if (result > 0) {
            exact_hits++;
        }
        if (verbose && i < 5) {
            printf("Queried exact key %d: key=%lu, found=%s\n",
                   i, keys[i], result > 0 ? "YES" : "NO");
        }
    }
    
    // End timing for exact queries
    end_clock = clock();
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec * 1000000 + tv.tv_usec;
    
    // Print exact query performance stats
    printf("\nExact Query Performance:\n");
    printf("Time for exact queries: %.3f sec\n", (double)(end_time - start_time) / 1000000);
    printf("Exact query throughput: %.2f ops/sec\n", (double)num_ops * 1000000 / (end_time - start_time));
    printf("CPU time for queries:   %.3f sec\n", (double)(end_clock - start_clock) / CLOCKS_PER_SEC);
    printf("Found %d out of %lu exact keys (%.2f%%)\n",
           exact_hits, num_ops, (double)exact_hits / num_ops * 100);
    
    // Generate false positives to trigger rehashing
    if (rehash_test) {
        printf("\n========== Rehashing Test Phase ==========\n");
        printf("Generating false positives to trigger rehashing...\n");
        
        // Generate keys that hash to the same buckets but have different values
        uint64_t *collision_keys = (uint64_t *)malloc(num_ops * sizeof(uint64_t));
        if (!collision_keys) {
            fprintf(stderr, "Failed to allocate memory for collision keys\n");
        } else {
            // Create keys that will likely cause false positives
            for (int i = 0; i < num_ops; i++) {
                // Create a key with the same low bits but different high bits
                collision_keys[i] = keys[i] ^ (1ULL << 32);
            }
            
            // Query with these keys to trigger false positives and rehashing
            start_clock = clock();
            gettimeofday(&tv, NULL);
            start_time = tv.tv_sec * 1000000 + tv.tv_usec;
            
            int fp_hits = 0;
            for (int i = 0; i < num_ops; i++) {
                int result = qfdb_query(qfdb, collision_keys[i]);
                if (result > 0) {
                    fp_hits++;
                }
                if (verbose && i < 5) {
                    printf("Queried collision key %d: key=%lu, found=%s\n",
                           i, collision_keys[i], result > 0 ? "YES" : "NO");
                }
            }
            
            end_clock = clock();
            gettimeofday(&tv, NULL);
            end_time = tv.tv_sec * 1000000 + tv.tv_usec;
            
            printf("False positive queries: %d out of %lu (%.4f%%)\n", 
                   fp_hits, num_ops, (double)fp_hits / num_ops * 100);
            printf("Time for FP queries: %.3f sec\n", (double)(end_time - start_time) / 1000000);
            
            free(collision_keys);
        }
        
        // Print bucket statistics to see which buckets were rehashed
        printf("\nBucket Statistics After False Positive Queries:\n");
        print_bucket_stats(qfdb);
        
        // Test explicit rehashing of a bucket group
        printf("\n========== Explicit Bucket Rehashing Test ==========\n");
        
        // Find a bucket group with high FP rate
        bucket_stats *worst_bucket = NULL;
        bucket_stats *current, *tmp;
        double worst_fp_rate = 0;
        uint64_t worst_bucket_idx = 0;
        
        HASH_ITER(hh, qfdb->buckets, current, tmp) {
            if (current->queries > 50 && current->fp_rate > worst_fp_rate) {
                worst_fp_rate = current->fp_rate;
                worst_bucket_idx = current->bucket_group_idx;
                worst_bucket = current;
            }
        }
        
        if (worst_bucket) {
            printf("Rehashing bucket group %lu with FP rate %.4f\n", 
                   worst_bucket_idx, worst_fp_rate);
            
            start_clock = clock();
            gettimeofday(&tv, NULL);
            start_time = tv.tv_sec * 1000000 + tv.tv_usec;
            
            int items_rehashed = qfdb_rehash_bucket_group(qfdb, worst_bucket_idx);
            
            end_clock = clock();
            gettimeofday(&tv, NULL);
            end_time = tv.tv_sec * 1000000 + tv.tv_usec;
            
            printf("Rehashed %d items in %.6f seconds\n", 
                   items_rehashed, (double)(end_time - start_time) / 1000000);
            
            // Test if keys are still found after rehashing
            int found_after_rehash = 0;
            for (int i = 0; i < num_ops; i++) {
                if (qfdb_query(qfdb, keys[i]) > 0) {
                    found_after_rehash++;
                }
            }
            
            printf("Found %d of %lu keys after bucket rehashing (%.2f%%)\n",
                   found_after_rehash, num_ops, 
                   (double)found_after_rehash / num_ops * 100);
        } else {
            printf("No suitable bucket group found for rehashing test\n");
        }
        
        // Test global rehashing
        printf("\n========== Global Rehashing Test ==========\n");
        
        start_clock = clock();
        gettimeofday(&tv, NULL);
        start_time = tv.tv_sec * 1000000 + tv.tv_usec;
        
        int items_rehashed = qfdb_global_rehash(qfdb);
        
        end_clock = clock();
        gettimeofday(&tv, NULL);
        end_time = tv.tv_sec * 1000000 + tv.tv_usec;
        
        printf("Globally rehashed %d items in %.6f seconds\n", 
               items_rehashed, (double)(end_time - start_time) / 1000000);
        
        // Test if keys are still found after global rehashing
        int found_after_rehash = 0;
        for (int i = 0; i < num_ops; i++) {
            if (qfdb_query(qfdb, keys[i]) > 0) {
                found_after_rehash++;
            }
        }
        
        printf("Found %d of %lu keys after global rehashing (%.2f%%)\n",
               found_after_rehash, num_ops, 
               (double)found_after_rehash / num_ops * 100);
        
        printf("\nBucket Statistics After Global Rehashing:\n");
        print_bucket_stats(qfdb);
    }
    
    // Standard random query test
    start_clock = clock();
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;

    // Query for some different keys (should be mostly misses)
    printf("\nPhase 2: Querying for random keys (should be mostly misses)...\n");
    int random_hits = 0;
    uint64_t random_queries = num_ops;
    for (uint64_t i = 0; i < random_queries; i++) {
        uint64_t random_key = ((uint64_t)rand() << 32) | (rand() + 2000000000);

        int result = qfdb_query(qfdb, random_key);
        if (result > 0) {
            random_hits++;
        }

        if (verbose && i < 5) {
            printf("Queried random key %lu: key=%lu, found=%s\n",
                   i, random_key, result > 0 ? "YES" : "NO");
        }
    }

    // End timing for random queries
    end_clock = clock();
    gettimeofday(&tv, NULL);
    end_time = tv.tv_sec * 1000000 + tv.tv_usec;

    // Print random query performance stats
    printf("\nRandom Query Performance:\n");
    printf("Time for random queries: %.3f sec\n", (double)(end_time - start_time) / 1000000);
    printf("Random query throughput: %.2f ops/sec\n", (double)random_queries * 1000000 / (end_time - start_time));
    printf("CPU time for queries:    %.3f sec\n", (double)(end_clock - start_clock) / CLOCKS_PER_SEC);
    printf("Found %d out of %lu random keys (%.6f%% false positive rate)\n",
           random_hits, random_queries, (double)random_hits / random_queries * 100);

    // Get statistics from QFDB
    uint64_t total_queries, verified_queries, fp_rehashes, adaptations_performed, space_errors;
    double fp_rate;
    qfdb_get_stats(qfdb, &total_queries, &verified_queries, &fp_rehashes, 
                  &adaptations_performed, &space_errors, &fp_rate);

    uint64_t rehashing_operations, rehashed_items;
    double avg_fp_rate;
    qfdb_get_rehash_stats(qfdb, &rehashing_operations, &rehashed_items, &avg_fp_rate);

    printf("\nQFDB Internal Statistics:\n");
    printf("Total queries:          %lu\n", total_queries);
    printf("Verified queries:       %lu\n", verified_queries);
    printf("False positive rehashes: %lu\n", fp_rehashes);
    printf("Adaptations performed:  %lu\n", adaptations_performed);
    printf("Space errors:           %lu\n", space_errors);
    printf("False positive rate:    %.6f\n", fp_rate);
    printf("Rehashing operations:   %lu\n", rehashing_operations);
    printf("Rehashed items:         %lu\n", rehashed_items);
    printf("Average FP rate:        %.6f\n", avg_fp_rate);

    // Memory usage statistics (estimate)
    size_t qf_size = (1ULL << qbits) * ((rbits/8) + 1);
    printf("\nMemory Usage (estimated):\n");
    printf("QF size:               %.2f MB\n", qf_size / (1024.0 * 1024.0));

    // Add detailed memory usage information
    print_rehashing_memory_usage(qfdb);

    // Free memory before destroying QFDB
    if (keys) {
        free(keys);
        keys = NULL;
    }

    // Properly clean up QFDB
    if (qfdb) {
        qfdb_destroy(qfdb);
        qfdb = NULL;
    }

    return 0;
}