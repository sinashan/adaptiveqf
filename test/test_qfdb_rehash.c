#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdbool.h>
#include <string.h>
#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include <unistd.h>
#include <assert.h>

#include "gqf_int.h"
#include "qf_splinterdb.h" // Include your QFDB header
#include "gqf.h"           // For QF error codes like QF_NO_SPACE

// --- Configuration Defaults ---
#define DEFAULT_QBITS 18
#define DEFAULT_RBITS 6 // LOW rbits to encourage adaptation
#define DEFAULT_INITIAL_LOAD_FACTOR 0.5
#define DEFAULT_REHASH_THRESHOLD_FACTOR 0.001 // Trigger rehash when adaptive slots reach 2% of nslots
#define DEFAULT_ADAPT_OPS_FACTOR 1.5       // Perform adapt-triggering ops = 1.5 * threshold
#define DEFAULT_OPS_DURING_REHASH 5000
#define DB_PATH "qfdb_rehash_test_storage"


static double get_time_usec();
static void timespec_diff(struct timespec *start, struct timespec *stop, struct timespec *result);

// --- Main Test Function ---
int main(int argc, char *argv[]) {
    // --- Parameter Parsing ---
    uint64_t qbits = DEFAULT_QBITS;
    uint64_t rbits = DEFAULT_RBITS;
    uint64_t num_initial_inserts = 0; // Calculated later
    uint64_t rehash_threshold = 0;    // Calculated later
    uint64_t num_adapt_ops = 0;       // Calculated later
    int verbose = 0;

    // Basic parsing (can be made more robust)
    if (argc > 1) qbits = atoll(argv[1]);
    if (argc > 2) rbits = atoll(argv[2]);
    if (argc > 3 && strcmp(argv[3], "--verbose") == 0) verbose = 1;

    uint64_t nslots = 1ULL << qbits;
    num_initial_inserts = (uint64_t)(nslots * DEFAULT_INITIAL_LOAD_FACTOR);
    rehash_threshold = (uint64_t)(nslots * DEFAULT_REHASH_THRESHOLD_FACTOR);
    printf("Rehash threashold %lu", rehash_threshold);
    num_adapt_ops = (uint64_t)(10000000);


    printf("--- Rehash Test Configuration ---\n");
    printf("qbits: %lu (nslots: %lu)\n", qbits, nslots);
    printf("rbits: %lu\n", rbits);
    printf("Num Initial Inserts: %lu (%.1f%% load)\n", num_initial_inserts, DEFAULT_INITIAL_LOAD_FACTOR * 100.0);
    printf("Rehash Threshold (adaptive slots): %lu (%.2f%% of nslots)\n", rehash_threshold, (double)rehash_threshold * 100.0 / nslots);
    printf("Num Adapt Ops to trigger: %lu\n", num_adapt_ops);
    printf("Num Ops During Rehash: %d\n", DEFAULT_OPS_DURING_REHASH);
    printf("Verbose: %s\n", verbose ? "Yes" : "No");
    printf("---------------------------------\n");

    srand(time(NULL));
    remove(DB_PATH);

    // --- Initialization ---
    printf("Initializing QFDB...\n");
    double start_time = get_time_usec();
    QFDB *qfdb = qfdb_init(qbits, rbits, DB_PATH);
    if (!qfdb) {
        fprintf(stderr, "Failed to initialize QFDB\n");
        return 1;
    }
    qfdb_set_rehash_threshold(qfdb, rehash_threshold);
    double end_time = get_time_usec();
    printf("Initialization took %.3f ms\n", (end_time - start_time) / 1000.0);

    uint64_t *original_keys = (uint64_t *)malloc(num_initial_inserts * sizeof(uint64_t));
    if (!original_keys) {
        perror("Failed to allocate memory for original keys");
        qfdb_destroy(qfdb);
        return 1;
    }

    // --- Phase 1: Populate the QFDB ---
    printf("\n--- Phase 1: Initial Population (%lu items) ---\n", num_initial_inserts);
    start_time = get_time_usec();
    for (uint64_t i = 0; i < num_initial_inserts; ++i) {
        // Generate unique-ish keys for initial insert
        original_keys[i] = (((uint64_t)rand() << 32) | rand()) ^ i; // Mix index for more uniqueness
         if (original_keys[i] == 0) original_keys[i] = 1; // Avoid key 0 if it causes issues

        int ret = qfdb_insert(qfdb, original_keys[i], 1);
        if (ret < 0) {
            fprintf(stderr, "Initial insert failed for key %lu (index %lu) with code %d. Aborting.\n", original_keys[i], i, ret);
            free(original_keys);
            qfdb_destroy(qfdb);
            return 1;
        }
        if (verbose && (i + 1) % (num_initial_inserts / 10) == 0) {
             printf("  Inserted %lu / %lu items...\n", i + 1, num_initial_inserts);
        }
    }
    end_time = get_time_usec();
    printf("Initial population took %.3f ms (%.2f Kops/s)\n",
           (end_time - start_time) / 1000.0,
           (double)num_initial_inserts / (end_time - start_time) * 1000.0);
     printf("  QF Occupancy: %lu / %lu (%.2f%%)\n", qfdb->qf->metadata->noccupied_slots, qfdb->qf->metadata->nslots,
             (double)qfdb->qf->metadata->noccupied_slots * 100.0 / qfdb->qf->metadata->nslots);
     printf("  Adaptive Slots: %lu\n", qfdb_get_adaptive_slots(qfdb));


    // --- Phase 2: Verify Initial Inserts ---
    printf("\n--- Phase 2: Verifying Initial Inserts ---\n");
    uint64_t verify_errors = 0;
    start_time = get_time_usec();
    for (uint64_t i = 0; i < num_initial_inserts; ++i) {
        int query_ret = qfdb_query(qfdb, original_keys[i]);
        if (query_ret != 1) {
            fprintf(stderr, "ERROR: Verification failed for original key %lu (index %lu). Expected 1, got %d\n",
                    original_keys[i], i, query_ret);
            verify_errors++;
             if (verify_errors > 20) {
                 fprintf(stderr,"Too many verification errors, aborting check.\n");
                 break;
             }
        }
    }
    end_time = get_time_usec();
     if (verify_errors == 0) {
         printf("Verification PASSED (%.3f ms, %.2f Kops/s)\n",
                (end_time - start_time) / 1000.0,
                (double)num_initial_inserts / (end_time - start_time) * 1000.0);
     } else {
         fprintf(stderr, "Verification FAILED with %lu errors.\n", verify_errors);
         free(original_keys);
         qfdb_destroy(qfdb);
         return 1;
     }
     printf("  Stats after verification: Queries: %lu, Verified: %lu, FPs: %lu\n",
           qfdb->total_queries, qfdb->verified_queries, qfdb->adaptations_performed);

    // --- Phase 3: Trigger Adaptations and Rehash ---
    printf("\n--- Phase 3: Triggering Adaptations/Rehash (%lu ops) ---\n", num_adapt_ops);
    printf("  Target adaptive slots for rehash: %lu\n", rehash_threshold);
    uint64_t adapt_ops_done = 0;
    uint64_t false_positives_generated = 0;
    start_time = get_time_usec();
    bool rehash_started = false;
    int done =1;
    while (adapt_ops_done < num_adapt_ops && !qfdb_is_rehashing(qfdb)) {
        uint64_t query_key = (((uint64_t)rand() << 32) | rand()) ^ (num_initial_inserts + adapt_ops_done);
        if (query_key == 0) query_key = 1; 

        int query_ret = qfdb_query(qfdb, query_key);

        if (query_ret == -1) { 
            false_positives_generated++;
            if (verbose && false_positives_generated % 1000 == 0) {
                 printf("  FP count: %lu, Adaptive Slots: %lu / %lu\n",
                        false_positives_generated, qfdb_get_adaptive_slots(qfdb), rehash_threshold);
            }
        } else if (query_ret < -1) {
             fprintf(stderr, "ERROR during adapt query: %d\n", query_ret);
             break;
        }

        adapt_ops_done++;

        if (adapt_ops_done % 10000 == 0 && verbose) {
             printf("  Adapt Ops: %lu/%lu, Adaptive Slots: %lu/%lu\n",
                    adapt_ops_done, num_adapt_ops, qfdb_get_adaptive_slots(qfdb), rehash_threshold);
        }

         if (qfdb_is_rehashing(qfdb)) {
             rehash_started = true;
             printf("  REHASH INITIATED at op %lu! Adaptive Slots: %lu\n",
                    adapt_ops_done, qfdb_get_adaptive_slots(qfdb));
             break; 
         }
    }
    end_time = get_time_usec();

     printf("Adaptation phase took %.3f ms.\n", (end_time - start_time) / 1000.0);
     printf("  Ops performed: %lu\n", adapt_ops_done);
     printf("  False Positives generated: %lu\n", false_positives_generated);
     printf("  Final Adaptive Slots before wait: %lu\n", qfdb_get_adaptive_slots(qfdb)); // Get value before waiting

    if (!rehash_started) {
        if (qfdb_get_adaptive_slots(qfdb) >= rehash_threshold) {
             printf("WARN: Threshold met, but rehash flag not set immediately. Checking again...\n");
             qfdb_finalize_rehash_if_complete(qfdb);
             rehash_started = qfdb_is_rehashing(qfdb);
             if(rehash_started) printf("  Rehash started after manual check.\n");
        }
    }

    if (!rehash_started) {
         fprintf(stderr, "WARNING: Rehash did not start after %lu adaptation operations. Threshold %lu, Adaptive Slots %lu.\n",
                 adapt_ops_done, rehash_threshold, qfdb_get_adaptive_slots(qfdb));
    }


    if (rehash_started) {
        printf("\n--- Phase 4: Waiting for Rehash Completion ---\n");
        start_time = get_time_usec();
        uint64_t wait_iterations = 0;
        while (qfdb_is_rehashing(qfdb)) {
           
            qfdb_finalize_rehash_if_complete(qfdb);
            usleep(100000); // Wait 100ms before checking again

            wait_iterations++;
            if (wait_iterations > 600) { 
                 fprintf(stderr, "ERROR: Timeout waiting for rehash to complete.\n");
    
                 qfdb_finalize_rehash_if_complete(qfdb);
                 if (qfdb_is_rehashing(qfdb)) {
                     // EXIT (FAILURE)
                     free(original_keys);
                     qfdb_destroy(qfdb);
                     return 1;
                 } else {
                      printf("  Rehash completed after final finalize attempt.\n");
                      break;
                 }
            }
             if (wait_iterations % 50 == 0) { // Print status every 5 seconds
                 printf("  Still waiting for rehash... (%lu s elapsed)\n", wait_iterations / 10);
             }
        }
        end_time = get_time_usec();
        printf("Rehash completion wait took %.3f ms.\n", (end_time - start_time) / 1000.0);
        printf("  Adaptive Slots after rehash completion: %lu\n", qfdb_get_adaptive_slots(qfdb));
    } else {
        printf("\n--- Phase 4: Skipping Rehash Wait (Rehash didn't start) ---\n");
    }


    // --- Phase 5: Final Verification ---
    printf("\n--- Phase 5: Final Verification (using qf_query_only) ---\n");
    uint64_t final_verify_errors = 0;
    uint64_t qq = 0;
    start_time = get_time_usec();
    for (uint64_t i = 0; i < num_initial_inserts; ++i) {
        // Use qf_query_only as SplinterDB might be inconsistent if rehash failed/was skipped
        int query_ret = qf_query_only(qfdb, original_keys[i]);
        if (query_ret != 1) {
            fprintf(stderr, "ERROR: Final verification failed for original key %lu (index %lu). Expected 1, got %d\n",
                    original_keys[i], i, query_ret);
            
             if (final_verify_errors > 20) {
                 fprintf(stderr,"Too many final verification errors, aborting check.\n");
                 break;
             }
        }
        if (query_ret>=0){
            qq++;
        }else{
            final_verify_errors++;
        }
    }
    end_time = get_time_usec();
    printf("Final verification took %.3f ms (%.2f Kops/s)\n",
           (end_time - start_time) / 1000.0,
           (double)num_initial_inserts / (end_time - start_time) * 1000.0);
    
    if (final_verify_errors == 0) {
        printf("\n===== TEST RESULT: PASSED =====\n");
        return 0;
    } else {
        fprintf(stderr, "\n===== TEST RESULT: FAILED (%lu final verification errors) =====\n", final_verify_errors);
        return 1;
    }

    return 0;
}


// --- Helper Function Implementations ---
static double get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}

// Simple timespec diff (assumes stop >= start)
static void timespec_diff(struct timespec *start, struct timespec *stop, struct timespec *result) {
    if ((stop->tv_nsec - start->tv_nsec) < 0) {
        result->tv_sec = stop->tv_sec - start->tv_sec - 1;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec + 1000000000;
    } else {
        result->tv_sec = stop->tv_sec - start->tv_sec;
        result->tv_nsec = stop->tv_nsec - start->tv_nsec;
    }
}