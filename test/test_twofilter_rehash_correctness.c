#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>
#include <unistd.h> 
#include <time.h>  

#include "include/qf_splinterdb.h" 
#include "include/gqf_int.h"
#include "rand_util.h"    

#define TEST_QBITS 20
#define TEST_RBITS 8
#define PRIMARY_DB_PATH "correctness_test_db"
#define BACKING_MAP_PATH "correctness_test_bm"
#define VERY_LOW_THRESHOLD 2000
#define FILL_PERCENTAGE 0.80 
#define NUM_INSERTIONS (uint64_t)((1ULL << TEST_QBITS) * FILL_PERCENTAGE)
#define NUM_NEGATIVE_QUERIES (NUM_INSERTIONS * 10)
#define MAX_REHASH_WAIT_SECONDS 60 

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

    printf("Correctness query for %lu inserted keys...\n", successful_inserts);
    uint64_t found_count_before = 0;
    uint64_t false_negatives_before = 0;
    for (uint64_t i = 0; i < successful_inserts; ++i) { 
        int result = qfdb_query(qfdb, inserted_keys[i]);
        if (result == 1) {
            found_count_before++;
        } else if (result == 0) {
            fprintf(stderr, "TEST FAIL: False negative for inserted key %lu!\n", inserted_keys[i]);
            false_negatives_before++;
            exit_code = EXIT_FAILURE;
        } else if (result == -1) {
        } else {
             fprintf(stderr, "query for inserted key %lu failed with code %d.\n", inserted_keys[i], result);
             exit_code = EXIT_FAILURE;
        }
    }
    printf("Found %lu / %lu inserted keys. False Negatives: %lu.\n",
           found_count_before, successful_inserts, false_negatives_before);
    if (false_negatives_before > 0) {
        goto cleanup; 
    }

    printf("Correctness check passed.\n");

    // trigger rehash with negative queries

    uint64_t occupied_before_neg = qf_get_num_occupied_slots(qfdb->qf);
    double percent_occupied_before = (qfdb->qf->metadata->nslots > 0) ?
                                     (double)occupied_before_neg * 100.0 / qfdb->qf->metadata->nslots : 0.0;
    printf("\nOccupied slots BEFORE negative queries: %lu (%.2f%% of %lu total slots)\n",

           occupied_before_neg,
           percent_occupied_before,
           qfdb->qf->metadata->nslots);
    uint64_t* negative_keys = generate_keys(NUM_NEGATIVE_QUERIES);

    printf("Performing %lu negative queries to trigger adaptations and rehash...\n", NUM_NEGATIVE_QUERIES);
    uint64_t adaptations_triggered = 0;
    for (uint64_t i = 0; i < NUM_NEGATIVE_QUERIES; ++i) {
        int result = qfdb_query(qfdb, negative_keys[i]);
        if (result == -1) { 
            adaptations_triggered++;
        } else if (result < -1) { 
             fprintf(stderr, "TEST FAIL: Negative query for key %lu failed with code %d.\n", negative_keys[i], result);
             exit_code = EXIT_FAILURE;
        }
        if (qfdb_is_rehashing(qfdb)) {
             printf("  Rehashing started after %lu negative queries (Adaptations: %lu).\n", i + 1, adaptations_triggered);
             break;
        }
    }
    printf("Queries to trigger adapt complete. Triggered %lu adaptations.\n", adaptations_triggered);
    free(negative_keys); 

    uint64_t occupied_after_neg = qf_get_num_occupied_slots(qfdb->qf);
    double percent_occupied_after = (qfdb->qf->metadata->nslots > 0) ?
                                     (double)occupied_after_neg * 100.0 / qfdb->qf->metadata->nslots : 0.0;
    printf("Occupied slots AFTER negative queries: %lu (%.2f%% of %lu total slots)\n",
           occupied_after_neg,
           percent_occupied_after,
           qfdb->qf->metadata->nslots);

    if (!qfdb_is_rehashing(qfdb)) {
         uint64_t current_adaptive = qfdb_get_adaptive_slots(qfdb);
         fprintf(stderr, "Rehashing did not trigger after %lu negative queries. Current adaptive slots: %lu (Threshold: %lu).\n",
                 NUM_NEGATIVE_QUERIES, current_adaptive, VERY_LOW_THRESHOLD);
    }


    // wait for rehash completion
    printf("Waiting for background rehash to complete");
    int wait_time = 0;
    while (qfdb_is_rehashing(qfdb) && wait_time < MAX_REHASH_WAIT_SECONDS) {
        sleep(1); 
        qfdb_finalize_rehash_if_complete(qfdb);
        wait_time++;
    }

    if (qfdb_is_rehashing(qfdb)) {
         fprintf(stderr, "TEST FAIL: Rehash did not complete");
         exit_code = EXIT_FAILURE;
         goto cleanup;
    }

    qfdb_finalize_rehash_if_complete(qfdb);
    if (qfdb_is_rehashing(qfdb)) {
         fprintf(stderr, "TEST FAIL: Rehashing flag still in progress.\n");
         exit_code = EXIT_FAILURE;
         goto cleanup;
    }
    printf("Rehash process complete.\n");


    //  Correctness Query (Inserted Keys) - AFTER Rehash
    printf("Correctness query for %lu inserted keys...\n", successful_inserts);
    uint64_t found_count_after = 0;
    uint64_t false_negatives_after = 0;
    uint32_t final_seed = (qfdb && qfdb->qf && qfdb->qf->metadata) ? qfdb->qf->metadata->seed : 0;  
    printf("New Seed: %u)\n", final_seed);

    for (uint64_t i = 0; i < successful_inserts; ++i) {
        int result = qfdb_query(qfdb, inserted_keys[i]);
        if (result == 1) {
            found_count_after++;
        } else if (result == 0) {
            false_negatives_after++;
            exit_code = EXIT_FAILURE;
        } else if (result == -1) {
            
        } else {
             exit_code = EXIT_FAILURE;
        }
    }
    printf("Found %lu / %lu inserted keys. False Negatives: %lu.\n",
           found_count_after, successful_inserts, false_negatives_after);

    if (false_negatives_after > 0) {
         printf("\n TEST FAIL: False negatives after rehashing\n");
         exit_code = EXIT_FAILURE;
    } else if (found_count_after != successful_inserts) {
         printf("\n TEST FAIL: Found count and successful inserts after rehashing (%lu vs %lu)\n",
                found_count_after, successful_inserts);
         exit_code = EXIT_FAILURE;
    } else {
         printf("\n TEST PASSED\n", successful_inserts);
    }

cleanup:
    printf("Cleaning up...\n");
    free(inserted_keys);
    // if (qfdb) {
    //     qfdb_destroy(qfdb);
    // }
    return exit_code;
}