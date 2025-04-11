#ifndef QF_UTHASH_H
#define QF_UTHASH_H

#include "gqf.h"
#include "uthash.h"

// Default values for rehashing parameters
#define DEFAULT_REHASH_THRESHOLD 0.05  // 5% false positive rate
#define DEFAULT_BUCKET_GROUP_SIZE 128  // Number of buckets in a group
#define MAX_TRACKED_BUCKETS 1024       // Maximum number of bucket groups to track

// Entry in the hashmap to map from minirun_id to original_key
typedef struct {
    uint64_t minirun_id;    // Key for hash table lookup (minirun ID)
    uint64_t original_key;  // Original key that was inserted
    UT_hash_handle hh;      // Makes this structure hashable
} minirun_entry;

// Structure to track false positive statistics per bucket group
typedef struct {
    uint64_t bucket_group_idx;      // Index of the bucket group
    uint64_t queries;               // Number of queries to this bucket group
    uint64_t false_positives;       // Number of false positives in this bucket group
    double fp_rate;                 // False positive rate for this bucket group
    uint64_t last_rehash_time;      // Time of last rehash (in queries)
    uint8_t hash_seed_modifier;     // Current hash seed modifier for this group
    UT_hash_handle hh;              // Makes this structure hashable
} bucket_stats;

// QFDB structure
typedef struct {
    QF *qf;                         // The Adaptive Quotient Filter
    minirun_entry *hashmap;         // Hashmap to map minirun_id to original_key

    // Rehashing parameters
    uint64_t bucket_group_size;     // Number of buckets in a group (granularity)
    double rehash_threshold;        // FP rate threshold to trigger rehashing
    
    // Bucket tracking
    bucket_stats *buckets;          // Hashmap of bucket group statistics
    
    // Global statistics
    uint64_t fp_rehashes;           // Counter for rehash operations
    uint64_t total_queries;         // Total number of queries
    uint64_t verified_queries;      // Number of queries that required verification
    uint64_t adaptations_performed; // Total adaptations performed (false positives)
    uint64_t space_errors;          // Total times QF_NO_SPACE reported
    uint64_t rehashing_operations;  // Number of rehash operations performed
    uint64_t rehashed_items;        // Number of items rehashed
} QFDB;

// Initialize the QFDB structure with configurable rehashing parameters
QFDB* qfdb_init_extended(uint64_t qbits, uint64_t rbits, 
                        uint64_t bucket_group_size, 
                        double rehash_threshold);

// Initialize with default rehashing parameters
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits);

// Free resources used by the QFDB
void qfdb_destroy(QFDB *qfdb);

// Core operations
int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count);
int qfdb_query(QFDB *qfdb, uint64_t key);
int qfdb_remove(QFDB *qfdb, uint64_t key);
int qfdb_resize(QFDB *qfdb, uint64_t new_qbits);

// Rehashing operations
int qfdb_rehash_bucket_group(QFDB *qfdb, uint64_t bucket_group_idx);
int qfdb_global_rehash(QFDB *qfdb);
void qfdb_set_rehash_params(QFDB *qfdb, uint64_t bucket_group_size, double threshold);

// Statistics
void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries,
                   uint64_t *fp_rehashes, uint64_t *adaptations_performed,
                   uint64_t *space_errors, double *false_positive_rate);
void qfdb_get_rehash_stats(QFDB *qfdb, uint64_t *rehashing_operations, 
                          uint64_t *rehashed_items, double *avg_fp_rate);
void print_hashmap_stats(QFDB *qfdb);
void print_bucket_stats(QFDB *qfdb);

#endif // QF_UTHASH_H