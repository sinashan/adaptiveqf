#ifndef QF_SPLINTERDB_H
#define QF_SPLINTERDB_H

#include "gqf.h"
#include "splinterdb/splinterdb.h"

// Add a data_cfg pointer to QFDB structure in qf_splinterdb.h
typedef struct {
    QF *qf;                       // The Adaptive Quotient Filter
    splinterdb *ext_store;        // External SplinterDB store

    data_config *data_cfg;        // SplinterDB configuration
    uint64_t fp_rehashes;         // Counter for rehash operations
    uint64_t fp_retrievals;       // Counter for external retrievals
    uint64_t total_queries;       // Total number of queries
    uint64_t verified_queries;    // Number of queries that required verification
    uint64_t adaptations_performed; // Total adaptation performed --> FALSE POSITIVES
    uint64_t space_errors; // Total times QF_NO_SPACE reported
} QFDB;

// Initialize the combined QF+SplinterDB structure
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits, const char* db_path);

// Free resources used by the combined structure
void qfdb_destroy(QFDB *qfdb);

// Insert an item
int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count);

// In qf_splinterdb.h:
int qfdb_query(QFDB *qfdb, uint64_t key);  // Changed from uint64_t to int

// Remove an item
int qfdb_remove(QFDB *qfdb, uint64_t key);

// Resize the underlying QF
int qfdb_resize(QFDB *qfdb, uint64_t new_qbits);

// Get statistics about the data structure
void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries,
                    uint64_t *fp_rehashes, uint64_t *adaptations_performed,
                    uint64_t *space_errors, double *false_positive_rate);

// Rehash items in a high false positive bucket
int qfdb_rehash_bucket(QFDB *qfdb, uint64_t bucket_idx);

#endif // QF_SPLINTERDB_H
