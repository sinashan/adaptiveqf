#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include "hashutil.h"

#include "qf_uthash.h"
#include "gqf_int.h"

#define DEBUG_MODE 0

#define DEBUG_PRINT(fmt, ...) \
    do { if (DEBUG_MODE) fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, __FILE__, \
                                __LINE__, __func__, ##__VA_ARGS__); } while (0)

// Hash function for QFDB
static inline uint64_t qfdb_hash(QFDB *qfdb, const void *key, size_t key_size, uint8_t seed_modifier) {
    if (!qfdb || !qfdb->qf || !qfdb->qf->metadata) return 0;

    quotient_filter_metadata *m = qfdb->qf->metadata;
    uint64_t seed = qfdb->qf->metadata->seed + seed_modifier;

    // Check if we need to use alternate hash function for frontier
    if (m->frontier != NULL && *(uint64_t*)key >= (uint64_t)m->frontier) {
        return MurmurHash64A(key, key_size, qfdb->qf->metadata->seed_b + seed_modifier);
    }

    return MurmurHash64A(key, key_size, seed);
}

// Initialize the QFDB structure with extended parameters
QFDB* qfdb_init_extended(uint64_t qbits, uint64_t rbits,
                        uint64_t bucket_group_size,
                        double rehash_threshold) {
    // Limit qbits to reasonable size to prevent overflows
    if (qbits >= 30) {
        fprintf(stderr, "Warning: qbits=%lu is too large. Limiting to 29\n", qbits);
        qbits = 29;
    }

    QFDB *qfdb = (QFDB*)calloc(1, sizeof(QFDB));
    if (!qfdb) return NULL;

    // Initialize the QF
    qfdb->qf = (QF*)malloc(sizeof(QF));
    if (!qfdb->qf) {
        free(qfdb);
        return NULL;
    }

    // Create QF with specified parameters
    if (!qf_malloc(qfdb->qf, 1ULL << qbits, qbits + rbits, 0,
                  QF_HASH_INVERTIBLE, 0)) {
        fprintf(stderr, "Failed to allocate QF with qbits=%lu, rbits=%lu\n", qbits, rbits);
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }

    // Initialize hashmaps
    qfdb->hashmap = NULL;
    qfdb->buckets = NULL;

    // Initialize rehashing parameters
    qfdb->bucket_group_size = bucket_group_size;
    qfdb->rehash_threshold = rehash_threshold;

    // Initialize counters
    qfdb->fp_rehashes = 0;
    qfdb->total_queries = 0;
    qfdb->verified_queries = 0;
    qfdb->adaptations_performed = 0;
    qfdb->space_errors = 0;
    qfdb->rehashing_operations = 0;
    qfdb->rehashed_items = 0;

    return qfdb;
}

// Initialize with default parameters
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits) {
    return qfdb_init_extended(qbits, rbits, DEFAULT_BUCKET_GROUP_SIZE, DEFAULT_REHASH_THRESHOLD);
}

// Free resources used by the combined structure
void qfdb_destroy(QFDB *qfdb) {
    if (!qfdb) return;

    // Free the hashmap entries
    minirun_entry *current_entry, *tmp_entry;
    HASH_ITER(hh, qfdb->hashmap, current_entry, tmp_entry) {
        HASH_DEL(qfdb->hashmap, current_entry);
        free(current_entry);
    }
    qfdb->hashmap = NULL;

    // Free the bucket stats entries
    bucket_stats *current_bucket, *tmp_bucket;
    HASH_ITER(hh, qfdb->buckets, current_bucket, tmp_bucket) {
        HASH_DEL(qfdb->buckets, current_bucket);
        free(current_bucket);
    }
    qfdb->buckets = NULL;

    // Free the QF
    if (qfdb->qf) {
        qf_free(qfdb->qf);
        free(qfdb->qf);
        qfdb->qf = NULL;
    }

    // Free the QFDB structure itself
    free(qfdb);
}

// Get or create bucket stats for a bucket group
static bucket_stats* get_or_create_bucket_stats(QFDB *qfdb, uint64_t bucket_group_idx) {
    bucket_stats *stats = NULL;

    // Look up existing stats
    HASH_FIND(hh, qfdb->buckets, &bucket_group_idx, sizeof(uint64_t), stats);

    // If not found, create a new entry
    if (!stats) {
        stats = (bucket_stats*)calloc(1, sizeof(bucket_stats));
        if (!stats) return NULL;

        stats->bucket_group_idx = bucket_group_idx;
        stats->queries = 0;
        stats->false_positives = 0;
        stats->fp_rate = 0.0;
        stats->last_rehash_time = 0;
        stats->hash_seed_modifier = 0;

        HASH_ADD(hh, qfdb->buckets, bucket_group_idx, sizeof(uint64_t), stats);
    }

    return stats;
}

// Update bucket stats with query result
static void update_bucket_stats(QFDB *qfdb, uint64_t bucket_idx, bool was_false_positive) {
    uint64_t bucket_group_idx = bucket_idx / qfdb->bucket_group_size;
    bucket_stats *stats = get_or_create_bucket_stats(qfdb, bucket_group_idx);

    if (!stats) return;

    stats->queries++;
    if (was_false_positive) {
        stats->false_positives++;
    }

    // Update false positive rate
    stats->fp_rate = (double)stats->false_positives / stats->queries;

    // Check if rehashing is needed
    if (stats->queries >= 100 && stats->fp_rate > qfdb->rehash_threshold) {
        // Only rehash if it hasn't been rehashed recently
        if (qfdb->total_queries - stats->last_rehash_time > 10000) {
            DEBUG_PRINT("Triggering rehash of bucket group %lu (FP rate: %.4f)\n",
                       bucket_group_idx, stats->fp_rate);
            qfdb_rehash_bucket_group(qfdb, bucket_group_idx);
            stats->last_rehash_time = qfdb->total_queries;
        }
    }
}

// Get all keys that hash to a specific bucket group
static uint64_t* collect_keys_for_bucket_group(QFDB *qfdb, uint64_t bucket_group_idx, int *num_keys) {
    if (!qfdb || !qfdb->hashmap) {
        *num_keys = 0;
        return NULL;
    }

    // First pass: count how many keys we'll need to collect
    int count = 0;
    minirun_entry *entry, *tmp;

    uint64_t start_bucket = bucket_group_idx * qfdb->bucket_group_size;
    uint64_t end_bucket = (bucket_group_idx + 1) * qfdb->bucket_group_size - 1;
    uint64_t q_bits = qfdb->qf->metadata->quotient_bits;
    uint64_t r_bits = qfdb->qf->metadata->bits_per_slot;

    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        uint64_t hash_bucket_index = (entry->minirun_id >> r_bits) & BITMASK(q_bits);
        if (hash_bucket_index >= start_bucket && hash_bucket_index <= end_bucket) {
            count++;
        }
    }

    // Allocate memory for the keys
    uint64_t *keys = NULL;
    if (count > 0) {
        keys = (uint64_t*)malloc(count * sizeof(uint64_t));
        if (!keys) {
            *num_keys = 0;
            return NULL;
        }

        // Second pass: collect the keys
        int i = 0;
        HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
            uint64_t hash_bucket_index = (entry->minirun_id >> r_bits) & BITMASK(q_bits);
            if (hash_bucket_index >= start_bucket && hash_bucket_index <= end_bucket) {
                keys[i++] = entry->original_key;
            }
        }
    }

    *num_keys = count;
    return keys;
}

// Rehash items in a specific bucket group
int qfdb_rehash_bucket_group(QFDB *qfdb, uint64_t bucket_group_idx) {
    if (!qfdb || !qfdb->qf) {
        return -1;
    }

    // Get bucket stats
    bucket_stats *stats = get_or_create_bucket_stats(qfdb, bucket_group_idx);
    if (!stats) return -1;

    // Collect all keys in this bucket group
    int num_keys = 0;
    uint64_t *keys = collect_keys_for_bucket_group(qfdb, bucket_group_idx, &num_keys);

    if (num_keys == 0 || !keys) {
        if (keys) free(keys);
        return 0;
    }

    DEBUG_PRINT("Rehashing bucket group %lu with %d keys\n", bucket_group_idx, num_keys);

    // Increment the seed modifier for this bucket group
    stats->hash_seed_modifier = (stats->hash_seed_modifier + 1) % 255;
    uint8_t new_seed = stats->hash_seed_modifier;

    // Remove all keys from this bucket group
    for (int i = 0; i < num_keys; i++) {
        qfdb_remove(qfdb, keys[i]);
    }

    // Reinsert with new hash seed
    for (int i = 0; i < num_keys; i++) {
        // Hash using new seed
        uint64_t key = keys[i];
        uint64_t hash = qfdb_hash(qfdb, &key, sizeof(key), new_seed);

        // Insert directly with the new hash
        qf_insert_result result;
        int ret = qf_insert_using_ll_table(qfdb->qf, hash, 1, &result, QF_KEY_IS_HASH);

        if (ret >= 0) {
            // Update or create hashmap entry
            minirun_entry *new_entry = (minirun_entry*)malloc(sizeof(minirun_entry));
            if (!new_entry) continue;

            new_entry->minirun_id = result.minirun_id;
            new_entry->original_key = key;

            minirun_entry *existing_entry = NULL;
            HASH_FIND(hh, qfdb->hashmap, &result.minirun_id, sizeof(uint64_t), existing_entry);

            if (existing_entry) {
                existing_entry->original_key = key;
                free(new_entry);
            } else {
                HASH_ADD(hh, qfdb->hashmap, minirun_id, sizeof(uint64_t), new_entry);
            }
        }
    }

    // Update stats
    qfdb->rehashing_operations++;
    qfdb->rehashed_items += num_keys;

    // Reset false positive rate
    stats->false_positives = 0;
    stats->queries = 0;
    stats->fp_rate = 0.0;

    free(keys);
    return num_keys;
}

// Perform a global rehash of the entire filter
int qfdb_global_rehash(QFDB *qfdb) {
    if (!qfdb || !qfdb->qf) {
        return -1;
    }

    // Collect all keys
    int key_count = 0;
    minirun_entry *entry, *tmp;
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        key_count++;
    }

    if (key_count == 0) return 0;

    uint64_t *keys = (uint64_t*)malloc(key_count * sizeof(uint64_t));
    if (!keys) return -1;

    int i = 0;
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        keys[i++] = entry->original_key;
    }

    // Reset QF and change seed
    qf_reset(qfdb->qf);
    qfdb->qf->metadata->seed ^= 0x5555555555555555ULL;

    // Free current hashmap
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        HASH_DEL(qfdb->hashmap, entry);
        free(entry);
    }
    qfdb->hashmap = NULL;

    // Reinsert all keys
    for (i = 0; i < key_count; i++) {
        qfdb_insert(qfdb, keys[i], 1);
    }

    // Reset all bucket stats
    bucket_stats *bucket, *btmp;
    HASH_ITER(hh, qfdb->buckets, bucket, btmp) {
        HASH_DEL(qfdb->buckets, bucket);
        free(bucket);
    }
    qfdb->buckets = NULL;

    // Update stats
    qfdb->rehashing_operations++;
    qfdb->rehashed_items += key_count;

    free(keys);
    return key_count;
}

// Set rehashing parameters
void qfdb_set_rehash_params(QFDB *qfdb, uint64_t bucket_group_size, double threshold) {
    if (!qfdb) return;

    qfdb->bucket_group_size = bucket_group_size;
    qfdb->rehash_threshold = threshold;

    // Reset bucket stats since bucket groups have changed
    bucket_stats *bucket, *tmp;
    HASH_ITER(hh, qfdb->buckets, bucket, tmp) {
        HASH_DEL(qfdb->buckets, bucket);
        free(bucket);
    }
    qfdb->buckets = NULL;
}

// Insert an item into QFDB
int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count) {
    if (!qfdb || !qfdb->qf) {
        return -1; // Invalid parameters
    }

    // Get the appropriate bucket stats
    uint64_t q_bits = qfdb->qf->metadata->quotient_bits;
    uint64_t r_bits = qfdb->qf->metadata->bits_per_slot;
    uint64_t bucket_idx = (key >> r_bits) & BITMASK(q_bits);
    uint64_t bucket_group_idx = bucket_idx / qfdb->bucket_group_size;

    bucket_stats *stats = get_or_create_bucket_stats(qfdb, bucket_group_idx);
    uint8_t seed_modifier = stats ? stats->hash_seed_modifier : 0;

    // Create a copy of the key for hashing
    uint64_t key_copy = key;

    // Hash the key with the appropriate seed modifier
    uint64_t hash = qfdb_hash(qfdb, &key_copy, sizeof(key_copy), seed_modifier);
    DEBUG_PRINT("Inserting key: %lu, hash: %lu, seed_modifier: %u\n", key, hash, seed_modifier);

    // Try to insert into QF - Use the hash here, not the key
    qf_insert_result result;
    int ret = qf_insert_using_ll_table(qfdb->qf, hash, count, &result, QF_KEY_IS_HASH);
    DEBUG_PRINT("QF insert result: %d\n", ret);

    if (ret >= 0) {
        DEBUG_PRINT("QF insert successful, minirun_id=%lu\n", result.minirun_id);

        // Create a new hashmap entry for the inserted item
        minirun_entry *new_entry = (minirun_entry*)malloc(sizeof(minirun_entry));
        if (!new_entry) {
            fprintf(stderr, "Failed to allocate memory for hash entry\n");
            return -1;
        }

        // Initialize the entry
        new_entry->minirun_id = result.minirun_id;
        new_entry->original_key = key;

        // Check if this minirun_id already exists in hashmap
        minirun_entry *existing_entry = NULL;
        HASH_FIND(hh, qfdb->hashmap, &result.minirun_id, sizeof(uint64_t), existing_entry);

        if (existing_entry) {
            // If it exists, update it
            existing_entry->original_key = key;
            free(new_entry); // Free the unnecessary new entry
        } else {
            // Otherwise, add the new entry to the hashmap
            HASH_ADD(hh, qfdb->hashmap, minirun_id, sizeof(uint64_t), new_entry);
        }

        DEBUG_PRINT("Added to hashmap: minirun_id=%lu, original_key=%lu\n",
                   result.minirun_id, key);
    } else {
        DEBUG_PRINT("QF insert failed (return code %d)\n", ret);
        // If QF_NO_SPACE was returned (-1)
        if (ret == -1) {
            qfdb->space_errors++;
        }
    }

    return ret;
}

// Query for an item in QFDB
int qfdb_query(QFDB *qfdb, uint64_t key) {
    if (!qfdb || !qfdb->qf) {
        return -1; // Invalid parameters
    }

    // Increment total queries counter
    qfdb->total_queries++;

    // Get the appropriate bucket stats
    uint64_t q_bits = qfdb->qf->metadata->quotient_bits;
    uint64_t r_bits = qfdb->qf->metadata->bits_per_slot;
    uint64_t bucket_idx = (key >> r_bits) & BITMASK(q_bits);
    uint64_t bucket_group_idx = bucket_idx / qfdb->bucket_group_size;

    bucket_stats *stats = get_or_create_bucket_stats(qfdb, bucket_group_idx);
    uint8_t seed_modifier = stats ? stats->hash_seed_modifier : 0;

    // Create a copy of the key for hashing
    uint64_t key_copy = key;

    // Compute hash with the appropriate seed modifier
    uint64_t hash = qfdb_hash(qfdb, &key_copy, sizeof(key_copy), seed_modifier);

    // Safety checks for large qbits
    if (qfdb->qf->metadata->quotient_bits >= 18) {
        uint64_t total_bits = qfdb->qf->metadata->quotient_bits + qfdb->qf->metadata->bits_per_slot;
        if (total_bits < 64) {
            uint64_t max_safe_hash = (1ULL << total_bits) - 1;
            hash &= max_safe_hash;
        }

        uint64_t hash_bucket_index = (hash >> qfdb->qf->metadata->bits_per_slot) &
                                    BITMASK(qfdb->qf->metadata->quotient_bits);

        if (hash_bucket_index >= qfdb->qf->metadata->nslots) {
            hash = (hash & BITMASK(qfdb->qf->metadata->bits_per_slot)) |
                  ((qfdb->qf->metadata->nslots - 1) << qfdb->qf->metadata->bits_per_slot);
        }
    }

    DEBUG_PRINT("Querying key: %lu, hash: %lu, seed_modifier: %u\n", key, hash, seed_modifier);

    // Query QF for the hash
    uint64_t ret_hash = 0;
    int minirun_rank = qf_query_using_ll_table(qfdb->qf, hash, &ret_hash, QF_KEY_IS_HASH);

    DEBUG_PRINT("QF query result: minirun_rank=%d, ret_hash=%lu\n", minirun_rank, ret_hash);

    // If not found in QF, return immediately
    if (minirun_rank < 0) {
        DEBUG_PRINT("Not found in QF\n");
        return 0;
    }

    // Increment verified_queries counter
    qfdb->verified_queries++;

    // Calculate minirun_id from the QF result
    uint64_t mask = (q_bits + r_bits >= 64) ?
        0xFFFFFFFFFFFFFFFF : ((1ULL << (q_bits + r_bits)) - 1);

    uint64_t minirun_id = ret_hash & mask;
    DEBUG_PRINT("Potential match found in QF, minirun_id=%lu\n", minirun_id);

    // Look up the minirun_id in our hashmap
    minirun_entry *entry = NULL;

    // Use uthash to find the entry
    HASH_FIND(hh, qfdb->hashmap, &minirun_id, sizeof(uint64_t), entry);

    if (!entry) {
        DEBUG_PRINT("Minirun ID not found in hashmap (hash collision)\n");
        qfdb->fp_rehashes++;
        update_bucket_stats(qfdb, bucket_idx, true);
        return 0; // Not found
    }

    // Check if this is a true match or false positive
    if (entry->original_key == key) {
        DEBUG_PRINT("True positive found\n");
        update_bucket_stats(qfdb, bucket_idx, false);
        return 1; // Found
    } else {
        DEBUG_PRINT("False positive eliminated\n");
        qfdb->fp_rehashes++;
        update_bucket_stats(qfdb, bucket_idx, true);

        // Attempt adaptation
        DEBUG_PRINT("Attempting adaptation\n");

        int adapt_ret = qf_adapt_using_ll_table(qfdb->qf,
                                            entry->original_key, // key that should be present
                                            key,                // key that caused false positive
                                            minirun_rank,       // rank of the item
                                            0);                 // flags

        // >0 means success
        if (adapt_ret > 0) {
            qfdb->adaptations_performed++;
            DEBUG_PRINT("Adaptation successful (return code: %d)\n", adapt_ret);
        } else {
            // = 0 means adaptation was not required.
            // = -1 (QF_NO_SPACE), -2 QF_COULDNT_LOCK, -3 QF_DOESNT_EXIST
            DEBUG_PRINT("Adaptation failed (return code: %d)\n", adapt_ret);
            if (adapt_ret == -1) { // QF_NO_SPACE
                qfdb->space_errors++;
            }
        }

        return 0; // Not found (false positive)
    }
}

// Remove an item from QFDB
int qfdb_remove(QFDB *qfdb, uint64_t key) {
    if (!qfdb || !qfdb->qf) {
        return -1;
    }

    // Get the appropriate bucket stats
    uint64_t q_bits = qfdb->qf->metadata->quotient_bits;
    uint64_t r_bits = qfdb->qf->metadata->bits_per_slot;
    uint64_t bucket_idx = (key >> r_bits) & BITMASK(q_bits);
    uint64_t bucket_group_idx = bucket_idx / qfdb->bucket_group_size;

    bucket_stats *stats = get_or_create_bucket_stats(qfdb, bucket_group_idx);
    uint8_t seed_modifier = stats ? stats->hash_seed_modifier : 0;

    // Hash the key with the appropriate seed modifier
    uint64_t hash = qfdb_hash(qfdb, &key, sizeof(key), seed_modifier);
    uint64_t ret_hash;
    int ret_hash_len;

    // Remove from QF
    int ret = qf_remove(qfdb->qf, hash, &ret_hash, &ret_hash_len, QF_KEY_IS_HASH);

    if (ret > 0) {
        // Calculate minirun_id
        uint64_t minirun_id = ret_hash & BITMASK(q_bits + r_bits);

        // Remove from hashmap
        minirun_entry *entry = NULL;
        HASH_FIND(hh, qfdb->hashmap, &minirun_id, sizeof(uint64_t), entry);

        if (entry) {
            HASH_DEL(qfdb->hashmap, entry);
            free(entry);
            DEBUG_PRINT("Removed minirun_id=%lu from hashmap\n", minirun_id);
        }
    }

    return ret;
}

// Resize the QFDB
int qfdb_resize(QFDB *qfdb, uint64_t new_qbits) {
    if (!qfdb || !qfdb->qf) {
        return -1;
    }

    // Safety check for new_qbits
    if (new_qbits >= 30) {
        fprintf(stderr, "Warning: new_qbits=%lu is too large. Limiting to 29\n", new_qbits);
        new_qbits = 29;
    }

    // Store all keys before resize
    int key_count = 0;
    minirun_entry *entry, *tmp;
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        key_count++;
    }

    uint64_t *keys = NULL;
    if (key_count > 0) {
        keys = (uint64_t*)malloc(key_count * sizeof(uint64_t));
        if (!keys) return -1;

        int i = 0;
        HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
            keys[i++] = entry->original_key;
        }
    }

    // Clear the existing hashmaps
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

    // Resize the QF
    int ret = qfdb->qf->runtimedata->container_resize(qfdb->qf, 1ULL << new_qbits);

    if (ret < 0) {
        // Failed to resize
        if (keys) free(keys);
        return ret;
    }

    // Reinsert all keys
    if (keys) {
        for (int i = 0; i < key_count; i++) {
            qfdb_insert(qfdb, keys[i], 1);
        }
        free(keys);
    }

    return ret;
}

// Get statistics about the QFDB
void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries,
                   uint64_t *fp_rehashes, uint64_t *adaptations_performed,
                   uint64_t *space_errors, double *false_positive_rate) {
    if (!qfdb) return;

    if (total_queries) *total_queries = qfdb->total_queries;
    if (verified_queries) *verified_queries = qfdb->verified_queries;
    if (fp_rehashes) *fp_rehashes = qfdb->fp_rehashes;
    if (adaptations_performed) *adaptations_performed = qfdb->adaptations_performed;
    if (space_errors) *space_errors = qfdb->space_errors;

    if (false_positive_rate) {
        if (qfdb->verified_queries > 0) {
            *false_positive_rate = (double)qfdb->fp_rehashes / qfdb->verified_queries;
        } else {
            *false_positive_rate = 0.0;
        }
    }
}

uint64_t qfdb_get_size_in_bytes(QFDB* qfdb) {

  // This size includes all the pointers, but not the data itself.
  uint64_t base_size = sizeof(QFDB);
  base_size += sizeof(QF);
  base_size += sizeof(quotient_filter_runtime_data);
  base_size += sizeof(quotient_filter_metadata);

  // Dynamic size
  base_size += sizeof(qfblock) * qfdb->qf->metadata->nblocks;
  base_size += sizeof(uint8_t) * qfdb->qf->metadata->nslots;

  return base_size;
}

// Get rehashing statistics
void qfdb_get_rehash_stats(QFDB *qfdb, uint64_t *rehashing_operations,
                          uint64_t *rehashed_items, double *avg_fp_rate) {
    if (!qfdb) return;

    if (rehashing_operations) *rehashing_operations = qfdb->rehashing_operations;
    if (rehashed_items) *rehashed_items = qfdb->rehashed_items;

    if (avg_fp_rate) {
        *avg_fp_rate = 0.0;
        int bucket_count = 0;
        double total_fp_rate = 0.0;

        bucket_stats *bucket, *tmp;
        HASH_ITER(hh, qfdb->buckets, bucket, tmp) {
            if (bucket->queries > 0) {
                total_fp_rate += bucket->fp_rate;
                bucket_count++;
            }
        }

        if (bucket_count > 0) {
            *avg_fp_rate = total_fp_rate / bucket_count;
        }
    }
}

// Utility function to print hashmap statistics
void print_hashmap_stats(QFDB *qfdb) {
    if (!qfdb) return;

    // Count entries in the hashmap
    int count = 0;
    minirun_entry *entry, *tmp;

    // This pattern is the correct way to iterate a uthash
    HASH_ITER(hh, qfdb->hashmap, entry, tmp) {
        count++;
        // Print some sample entries (first 5)
        if (count <= 5) {
            printf("Hashmap entry %d: minirun_id=%lu, original_key=%lu\n",
                   count, entry->minirun_id, entry->original_key);
        }
    }

    printf("\nHashmap Statistics:\n");
    printf("Total entries: %d\n", count);
}

// Utility function to print bucket statistics
void print_bucket_stats(QFDB *qfdb) {
    if (!qfdb) return;

    printf("Bucket Group Statistics:\n");
    printf("Group Size: %lu buckets, Rehash Threshold: %.2f%%\n",
           qfdb->bucket_group_size, qfdb->rehash_threshold * 100);

    int count = 0;
    bucket_stats *bucket, *tmp;

    HASH_ITER(hh, qfdb->buckets, bucket, tmp) {
        if (bucket->queries > 0) {
            printf("Group %lu: queries=%lu, false_positives=%lu, FP_rate=%.4f, seed_mod=%u\n",
                  bucket->bucket_group_idx, bucket->queries, bucket->false_positives,
                  bucket->fp_rate, bucket->hash_seed_modifier);
            count++;
        }

        // Limit output to first 20 active bucket groups
        if (count >= 20) {
            printf("... (more bucket groups not shown)\n");
            break;
        }
    }
    if (count == 0) {
        printf("No active bucket groups yet.\n");
    }
}
