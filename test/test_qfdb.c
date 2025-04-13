#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include "qf_splinterdb.h"
#include <sys/time.h>
#include <time.h>
#include <string.h>
#define HEAP_IMPLEMENTATION
#include "sc_min_heap.h"

int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <qbits> <rbits> <num_ops> [--verbose]\n", argv[0]);
        return 1;
    }

    uint64_t qbits = atoll(argv[1]);
    uint64_t rbits = atoll(argv[2]);
    uint64_t num_ops = atoll(argv[3]);
    int verbose = 0;

    if (argc > 4 && strcmp(argv[4], "--verbose") == 0) {
        verbose = 1;
    }

    const char *db_path = "qfdb_test_storage";

    // Remove any existing database
    remove(db_path);

    // Initialize the combined data structure
    QFDB *qfdb = qfdb_init(qbits, rbits, db_path);
    if (!qfdb) {
        fprintf(stderr, "Failed to initialize QFDB\n");
        return 1;
    }

    printf("Initialized QFDB with %d qbits and %d rbits\n", qbits, rbits);

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

    // Remove generating keys out of insert timing measurement.
    for (uint64_t i = 0; i < num_ops; ++i){
        if (i % 2 == 0) {
            keys[i] = 1000000 + i;
        } else {
            keys[i] = ((uint64_t)rand() << 32) | rand();
        }
    }

    // Start timing before insertions
    start_clock = clock();
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;

    printf("Inserting %ld keys...\n", num_ops);
    for (uint64_t i = 0; i < num_ops; i++) {
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
    printf("Number of inserts:     %d\n", num_ops);
    printf("Successful inserts:    %lu\n", num_updates);
    printf("Time for inserts:      %.3f sec\n", (double)(end_time - start_time) / 1000000);
    printf("Insert throughput:     %.2f ops/sec\n", (double)num_ops * 1000000 / (end_time - start_time));
    printf("CPU time for inserts:  %.3f sec\n", (double)(end_clock - start_clock) / CLOCKS_PER_SEC);
    printf("Number of occupied slots: %d\n", qfdb_get_occ_slots(qfdb));

    printf("Inserting elements into heap...\n");
    for (uint64_t i = 0; i < num_ops; ++i){
      if (!sc_heap_add(qfdb->heap, keys[i], NULL)){
        printf("Error inserting key into heap");
      }
    }
    sc_heap_copy(qfdb->heap_copy, qfdb->heap);

    size_t sz = qfdb->heap->cap * sizeof(struct sc_heap_data);
    printf("Size of heap in kbytes : %zu\n", sz/1000);
    sz = qfdb->heap_copy->cap * sizeof(struct sc_heap_data);
    printf("Size of heap copy in kbytes : %zu\n", sz/1000);

    // Print hashmap statistics after insertion
    print_hashmap_stats(qfdb);

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
        if (verbose) {
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
    printf("Found %d out of %d exact keys (%.2f%%)\n",
           exact_hits, num_ops, (double)exact_hits / num_ops * 100);

    // Start timing for random queries
    start_clock = clock();
    gettimeofday(&tv, NULL);
    start_time = tv.tv_sec * 1000000 + tv.tv_usec;

    // 2. Query for some different keys (should be mostly misses)
    printf("\nPhase 2: Querying for random keys (should be mostly misses)...\n");
    int random_hits = 0;
    uint64_t random_queries = 10000000; // Reduced for testing (was previously 2^21 + 2^21)
    for (uint64_t i = 0; i < random_queries; i++) {
        uint64_t random_key = ((uint64_t)rand() << 32) | (rand() + 2000000000);

        int result = qfdb_query(qfdb, random_key);
        if (result > 0) {
            random_hits++;
        }

        if (verbose) {
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
    qfdb_get_stats(qfdb, &total_queries, &verified_queries, &fp_rehashes, &adaptations_performed, &space_errors, &fp_rate);

    printf("\nQFDB Internal Statistics:\n");
    printf("Total queries:          %lu\n", total_queries);
    printf("Verified queries:       %lu\n", verified_queries);
    printf("False positive rehashes: %lu\n", fp_rehashes);
    printf("Adaptations performed: %lu\n", adaptations_performed);
    printf("Space errors (QF_NO_SPACE): %lu\n", space_errors);
    printf("False positive rate:     %.6f\n", fp_rate);

    // Memory usage statistics (estimate)
    size_t qf_size = (1ULL << qbits) * ((rbits/8) + 1);
    printf("\nMemory Usage (estimated):\n");
    printf("QF size:               %.2f MB\n", qf_size / (1024.0 * 1024.0));
    // printf("Total operations:      %d\n", num_ops * 2 + random_queries);

    // Free memory before destroying QFDB
    if (keys) {
        printf("\nFreeing keys array...\n");
        free(keys);
        keys = NULL;
    }

    // Properly clean up QFDB
    if (qfdb) {
        printf("Destroying QFDB...\n");
        qfdb_destroy(qfdb);
        qfdb = NULL;
        printf("QFDB destroyed successfully\n");
    }

    return 0;
}
