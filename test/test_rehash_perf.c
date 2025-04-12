#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h> 
#include <time.h>  

#include "include/qf_splinterdb.h" 
#include "include/gqf_int.h"
#include <errno.h>
#include "rand_util.h" 

#include <sys/time.h> 
#define TEST_QBITS 20
#define TEST_RBITS 9
#define PRIMARY_DB_PATH "correctness_test_db"
#define BACKING_MAP_PATH "correctness_test_bm"
#define VERY_LOW_THRESHOLD 100000
#define FILL_PERCENTAGE 0.85 
#define NUM_INSERTIONS (uint64_t)((1ULL << TEST_QBITS) * FILL_PERCENTAGE)
#define NUM_QUERIES (NUM_INSERTIONS * 200)
#define MAX_REHASH_WAIT_SECONDS 60 


static double get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}

uint64_t rand_uniform(uint64_t max) {
        if (max <= RAND_MAX) return rand() % max;
        uint64_t a = rand();
        uint64_t b = rand();
        a |= (b << 31);
        return a % max;
}

uint64_t* generate_keys(uint64_t count) {
    uint64_t* keys = (uint64_t*)malloc(count * sizeof(uint64_t));
    for (uint64_t i = 0; i < count; ++i) {
        keys[i] = rand_uniform(UINT64_MAX);
    }
    return keys;
}

int main() {
    int exit_code = EXIT_SUCCESS;

    remove(PRIMARY_DB_PATH);
    remove(BACKING_MAP_PATH);
    remove(BACKING_MAP_PATH ".rebuild.tmp");

    printf("Initializing QFDB...\n");
    QFDB *qfdb = qfdb_init(TEST_QBITS, TEST_RBITS, PRIMARY_DB_PATH);
    if (!qfdb) {
        fprintf(stderr, "qfdb is null\n");
        return EXIT_FAILURE;
    }
    qfdb_set_rehash_threshold(qfdb, VERY_LOW_THRESHOLD);


    // --------INSERTION------------- (wont trigger rehash)
    uint64_t* inserted_keys = generate_keys(NUM_INSERTIONS);

    printf("Inserting %lu keys\n", NUM_INSERTIONS);
    uint64_t successful_inserts = 0;
    for (uint64_t i = 0; i < NUM_INSERTIONS; ++i) {
        if (qfdb_insert(qfdb, inserted_keys[i], 1) == 0) { 
            successful_inserts++;
        } else {
            fprintf(stderr, "Filter might be full.\n", inserted_keys[i], i);
        }
    }
    printf("Insertion complete. Inserted %lu keys.\n", successful_inserts);
    if (successful_inserts == 0) {
         fprintf(stderr, "Abort: No keys were inserted.\n");
         exit_code = EXIT_FAILURE;
         goto cleanup;
    }

    printf("Total queries: %lu\n", NUM_QUERIES);
    // --------QUERIES------------- (triggers rehash)
    uint64_t* negative_keys = generate_keys(NUM_QUERIES);
        uint64_t occupied_before_neg = qf_get_num_occupied_slots(qfdb->qf);
    double percent_occupied_before = (qfdb->qf->metadata->nslots > 0) ?
                                     (double)occupied_before_neg * 100.0 / qfdb->qf->metadata->nslots : 0.0;
    printf("\nOccupied slots BEFORE queries: %lu (%.2f%% of %lu total slots)\n",
           occupied_before_neg,
           percent_occupied_before,
           qfdb->qf->metadata->nslots);

    double queries_start = get_time_usec();
    double total_query_latency_us = 0;
    for (uint64_t i = 0; i < NUM_QUERIES; ++i) {
        double op_start = get_time_usec();
        int result = qfdb_query(qfdb, negative_keys[i]);
        if (result < -1) { 
             fprintf(stderr, "TEST FAIL: query for key %lu failed with code %d.\n", negative_keys[i], result);
             exit_code = EXIT_FAILURE;
        }
        double op_end = get_time_usec();
        double operation_latency_us = op_end - op_start;
        total_query_latency_us += operation_latency_us;
    }

    printf("Queries complete\n");
    double queries_end = get_time_usec();
    double queries_duration_s = (queries_end - queries_start) / 1000000.0;

    printf("Query Throughput: %.6f\n ", NUM_QUERIES/queries_duration_s);
    printf("Query Latency: %.6f\n", queries_duration_s/NUM_QUERIES);
    double fpr = qfdb->fp_rehashes/NUM_QUERIES;
    printf("False Positive Rate: %.10f\n", fpr);

    occupied_before_neg = qf_get_num_occupied_slots(qfdb->qf);
    percent_occupied_before = (qfdb->qf->metadata->nslots > 0) ?
                                     (double)occupied_before_neg * 100.0 / qfdb->qf->metadata->nslots : 0.0;
        printf("\nOccupied slots AFTER queries: %lu (%.2f%% of %lu total slots)\n",
           occupied_before_neg,
           percent_occupied_before,
           qfdb->qf->metadata->nslots);

    cleanup:
    free(inserted_keys);
    // if (qfdb) {
    //     qfdb_destroy(qfdb);
    // }
    return exit_code;

}
