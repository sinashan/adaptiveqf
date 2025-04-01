#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "qf_splinterdb.h"
#include "gqf_int.h"

#define DEBUG_MODE 1


#define DEBUG_PRINT(fmt, ...) \
    do { if (DEBUG_MODE) fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, __FILE__, \
                                __LINE__, __func__, ##__VA_ARGS__); } while (0)

// Define key comparison and related functions
static int 
key_compare_func(const data_config *cfg, slice key1, slice key2)
{
    // Compare two keys (uint64_t values)
    uint64_t k1, k2;
    memcpy(&k1, key1.data, sizeof(uint64_t));
    memcpy(&k2, key2.data, sizeof(uint64_t));
    
    if (k1 < k2) return -1;
    if (k1 > k2) return 1;
    return 0;
}

static uint32_t 
key_hash_func(const void *key, size_t length, uint32_t seed)
{
    // Hash a key
    uint64_t k;
    if (length == sizeof(uint64_t)) {
        memcpy(&k, key, sizeof(uint64_t));
        return (uint32_t)(k ^ (k >> 32));
    } else {
        return 0;
    }
}

static void
key_to_string_func(const data_config *cfg, slice key, char *str, size_t max_len)
{
    uint64_t k;
    memcpy(&k, key.data, sizeof(uint64_t));
    snprintf(str, max_len, "%lu", k);
}

static void
message_to_string_func(const data_config *cfg, message msg, char *str, size_t max_len)
{
    uint64_t val;
    slice value_slice = message_slice(msg);
    if (value_slice.length == sizeof(uint64_t)) {
        memcpy(&val, value_slice.data, sizeof(uint64_t));
        snprintf(str, max_len, "%lu", val);
    } else {
        snprintf(str, max_len, "<binary data: %zu bytes>", value_slice.length);
    }
}

static int
merge_tuples_func(const data_config *cfg,
                  slice key,
                  message old_msg,
                  merge_accumulator *new_msg)
{
    return 0; // Simple implementation
}

// In qfdb_init, save the data_config pointer
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits, const char* db_path) {
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
                  QF_HASH_DEFAULT, 0)) {
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }
    
    // Set auto-resize for QF
    qf_set_auto_resize(qfdb->qf, true);
    
    // Create data_config
    data_config *data_cfg = (data_config *)malloc(sizeof(data_config));
    if (!data_cfg) {
        qf_free(qfdb->qf);
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }
    
    // Initialize data_config
    memset(data_cfg, 0, sizeof(data_config));
    
    // Set callback functions
    data_cfg->key_compare = key_compare_func;
    data_cfg->key_hash = key_hash_func;
    data_cfg->key_to_string = key_to_string_func;
    data_cfg->message_to_string = message_to_string_func;
    data_cfg->merge_tuples = merge_tuples_func;
    
    // Set key size
    data_cfg->max_key_size = sizeof(uint64_t);
    
    // Save data_config
    qfdb->data_cfg = data_cfg;
    
    // Initialize SplinterDB configuration
    splinterdb_config cfg;
    memset(&cfg, 0, sizeof(splinterdb_config));
    cfg.filename = db_path;
    cfg.cache_size = 1024 * 1024 * 128; // 128MB cache
    cfg.disk_size = 1024 * 1024 * 256;  // 256MB disk size (REQUIRED)
    cfg.data_cfg = data_cfg;
    
    // Create SplinterDB
    if (splinterdb_create(&cfg, &qfdb->ext_store) != 0) {
        free(data_cfg);
        qf_free(qfdb->qf);
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }
    
    // Initialize counters
    qfdb->fp_rehashes = 0;
    qfdb->fp_retrievals = 0;
    qfdb->total_queries = 0;
    qfdb->verified_queries = 0;

    return qfdb;
}

// In qfdb_destroy, free the data_config separately 
void qfdb_destroy(QFDB *qfdb) {
    if (!qfdb) return;

    if (qfdb->ext_store) {
        splinterdb *tmp = qfdb->ext_store;
        qfdb->ext_store = NULL; 
        
        splinterdb_close(&tmp);
    }
    
    if (qfdb->qf) {
        qf_free(qfdb->qf);
        free(qfdb->qf);
        qfdb->qf = NULL;
    }
    
    if (qfdb->data_cfg) {
        free(qfdb->data_cfg);
        qfdb->data_cfg = NULL;
    }
    
    free(qfdb);
}


int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count) {
    if (!qfdb || !qfdb->qf || !qfdb->ext_store) {
        return -1; // Invalid parameters
    }
    
    // Create a copy of the key for hashing
    uint64_t key_copy = key;
    
    // Hash the key
    uint64_t hash = MurmurHash64A(&key_copy, sizeof(key_copy), qfdb->qf->metadata->seed);
    DEBUG_PRINT("Inserting key: %lu, hash: %lu\n", key, hash);
    
    // Try to insert into QF - IMPORTANT: Use the hash here, not the key
    qf_insert_result result;
    int ret = qf_insert_using_ll_table(qfdb->qf, hash, count, &result, QF_KEY_IS_HASH);
    DEBUG_PRINT("QF insert result: %d\n", ret);
    
    if (ret >= 0) {
        DEBUG_PRINT("QF insert successful, minirun_id=%lu\n", result.minirun_id);
        
        // Also store in SplinterDB for verification
        slice key_slice, value_slice;
        
        // Allocate memory for key and value that won't go out of scope
        void *key_buf = malloc(sizeof(uint64_t));
        void *value_buf = malloc(sizeof(uint64_t));
        
        if (!key_buf || !value_buf) {
            fprintf(stderr, "Failed to allocate memory for SplinterDB buffers\n");
            if (key_buf) free(key_buf);
            if (value_buf) free(value_buf);
            return ret;
        }
        
        // Store the minirun ID as the key and the original key as the value
        uint64_t minirun_id = result.minirun_id;
        DEBUG_PRINT("Using minirun_id=%lu as SplinterDB key\n", minirun_id);
        
        memcpy(key_buf, &minirun_id, sizeof(uint64_t));
        memcpy(value_buf, &key, sizeof(uint64_t));
        
        key_slice.data = key_buf;
        key_slice.length = sizeof(uint64_t);
        value_slice.data = value_buf;
        value_slice.length = sizeof(uint64_t);
        
        DEBUG_PRINT("Inserting into SplinterDB: key=%lu, value=%lu\n", minirun_id, key);
        int db_ret = splinterdb_insert(qfdb->ext_store, key_slice, value_slice);
        
        // Free the buffers after insertion
        free(key_buf);
        free(value_buf);
        
        if (db_ret != 0) {
            fprintf(stderr, "Failed to insert into SplinterDB, error: %d\n", db_ret);
        } else {
            DEBUG_PRINT("SplinterDB insert successful\n");
        }
    } else {
        DEBUG_PRINT("QF insert failed\n");
    }
    
    return ret;
}


// Completely rewritten qfdb_query function with better error handling
int qfdb_query(QFDB *qfdb, uint64_t key) {
    if (!qfdb || !qfdb->qf || !qfdb->ext_store) {
        return -1; // Invalid parameters
    }
    
    // Safety check for QF metadata
    if (!qfdb->qf->metadata) {
        fprintf(stderr, "ERROR: QF metadata is NULL\n");
        return -1;
    }
    
    // Increment total queries counter
    qfdb->total_queries++;
    
    // Create a copy of the key for hashing
    uint64_t key_copy = key;
    
    // CRITICAL FIX: Compute hash but constrain it to a safe range for large qbits
    uint64_t hash;
    if (qfdb->qf->metadata->hash_mode == QF_HASH_DEFAULT) {
        hash = MurmurHash64A(&key_copy, sizeof(key_copy), qfdb->qf->metadata->seed);
        
        // For large qbits (≥18), constrain the hash to prevent out-of-bounds access
        if (qfdb->qf->metadata->quotient_bits >= 18) {
            // Calculate the maximum safe hash value based on filter parameters
            uint64_t total_bits = qfdb->qf->metadata->quotient_bits + qfdb->qf->metadata->bits_per_slot;
            uint64_t max_safe_hash;
            
            if (total_bits < 64) {
                max_safe_hash = (1ULL << total_bits) - 1;
                hash &= max_safe_hash;
            }
            
            // Additional safety: ensure hash_bucket_index is within bounds
            uint64_t hash_bucket_index = (hash >> qfdb->qf->metadata->bits_per_slot) & 
                                         BITMASK(qfdb->qf->metadata->quotient_bits);
            
            if (hash_bucket_index >= qfdb->qf->metadata->nslots) {
                hash = (hash & BITMASK(qfdb->qf->metadata->bits_per_slot)) | 
                       ((qfdb->qf->metadata->nslots - 1) << qfdb->qf->metadata->bits_per_slot);
            }
        }
    } else if (qfdb->qf->metadata->hash_mode == QF_HASH_INVERTIBLE) {
        hash = hash_64(key_copy, -1ULL);
    } else {
        hash = key_copy;
    }
    
    DEBUG_PRINT("Querying key: %lu, hash: %lu\n", key, hash);
    
    // Use the hash with QF_KEY_IS_HASH flag to skip rehashing
    uint64_t ret_hash = 0;
    int minirun_rank = qf_query_using_ll_table(qfdb->qf, hash, &ret_hash, QF_KEY_IS_HASH);
    
    
    DEBUG_PRINT("QF query result: minirun_rank=%d, ret_hash=%lu\n", minirun_rank, ret_hash);
    
    // If not found in QF, it's definitely not in the SplinterDB
    if (minirun_rank < 0) {
        DEBUG_PRINT("Not found in QF\n");
        return 0;
    }
    
    // IMPORTANT: Increment verified_queries only when a potential match is found in QF
    qfdb->verified_queries++;
    
    // Potential match found in QF - calculate minirun_id
    uint64_t minirun_id = 0;
    
    // Extract minirun_id from ret_hash
    if (qfdb->qf && qfdb->qf->metadata) {
        uint64_t q_bits = qfdb->qf->metadata->quotient_bits;
        uint64_t r_bits = qfdb->qf->metadata->bits_per_slot;
        
        // Calculate bitmask safely
        uint64_t mask = (q_bits + r_bits >= 64) ? 
            0xFFFFFFFFFFFFFFFF : ((1ULL << (q_bits + r_bits)) - 1);
            
        minirun_id = ret_hash & mask;
        DEBUG_PRINT("Potential match found in QF, minirun_id=%lu\n", minirun_id);
    } else {
        DEBUG_PRINT("Error: QF metadata is NULL\n");
        return -1;
    }
    
    // Rest of function remains the same...
    
    // Verify in SplinterDB
    slice key_slice;
    
    // Allocate memory for key
    void* key_buf = malloc(sizeof(uint64_t));
    if (!key_buf) {
        fprintf(stderr, "ERROR: Failed to allocate memory for key buffer\n");
        return -1;
    }
    
    // Copy the minirun_id to the buffer
    memcpy(key_buf, &minirun_id, sizeof(uint64_t));
    
    key_slice.data = key_buf;
    key_slice.length = sizeof(uint64_t);
    
    // Initialize lookup result
    splinterdb_lookup_result result;
    splinterdb_lookup_result_init(qfdb->ext_store, &result, 0, NULL);
    
    // Look up in SplinterDB
    DEBUG_PRINT("Looking up in SplinterDB, key=%lu\n", minirun_id);
    int db_ret = splinterdb_lookup(qfdb->ext_store, key_slice, &result);
    DEBUG_PRINT("SplinterDB lookup result: %d\n", db_ret);
    
    // Free the key buffer
    free(key_buf);
    
    // Check if key was found
    if (db_ret != 0 || !splinterdb_lookup_found(&result)) {
        DEBUG_PRINT("SplinterDB lookup failed or key not found\n");
        splinterdb_lookup_result_deinit(&result);
        return 0; // Not found
    }
    
    // Get the value
    slice val_slice;
    splinterdb_lookup_result_value(&result, &val_slice);
    DEBUG_PRINT("Value length from SplinterDB: %zu\n", val_slice.length);
    
    // Verify the key
    if (val_slice.length != sizeof(uint64_t)) {
        DEBUG_PRINT("Unexpected value length\n");
        splinterdb_lookup_result_deinit(&result);
        return 0; // Not found
    }
    
    // Extract the stored key
    uint64_t stored_key = 0;
    memcpy(&stored_key, val_slice.data, sizeof(uint64_t));
    DEBUG_PRINT("Stored key: %lu, query key: %lu\n", stored_key, key);
    
    // Check if this is a true match or false positive
    bool is_match = (stored_key == key);
    splinterdb_lookup_result_deinit(&result);  // Clean up the result
    
    if (is_match) {
        DEBUG_PRINT("True positive found\n");
        return 1; // Found
    } else {
        DEBUG_PRINT("False positive eliminated\n");
        qfdb->fp_rehashes++;
        return 0; // Not found (false positive)
    }
}

int qfdb_remove(QFDB *qfdb, uint64_t key) {
    // Hash the key
    uint64_t hash = MurmurHash64A(&key, sizeof(key), qfdb->qf->metadata->seed);
    uint64_t ret_hash;
    int ret_hash_len;
    
    // Remove from QF
    int ret = qf_remove(qfdb->qf, hash, &ret_hash, &ret_hash_len, QF_KEY_IS_HASH);
    
    if (ret > 0) {
        // Also remove from SplinterDB
        uint64_t minirun_id = ret_hash & BITMASK(qfdb->qf->metadata->quotient_bits + qfdb->qf->metadata->bits_per_slot);
        
        slice key_slice;
        char key_buf[sizeof(uint64_t)];
        
        memcpy(key_buf, &minirun_id, sizeof(uint64_t));
        key_slice.data = key_buf;
        key_slice.length = sizeof(uint64_t);
        
        splinterdb_delete(qfdb->ext_store, key_slice);
    }
    
    return ret;
}

int qfdb_resize(QFDB *qfdb, uint64_t new_qbits) {
    return qfdb->qf->runtimedata->container_resize(qfdb->qf, 1ULL << new_qbits);
}

void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries, 
                   uint64_t *fp_rehashes, double *false_positive_rate) {
    if (total_queries) *total_queries = qfdb->total_queries;
    if (verified_queries) *verified_queries = qfdb->verified_queries;
    if (fp_rehashes) *fp_rehashes = qfdb->fp_rehashes;
    
    if (false_positive_rate) {
        if (qfdb->verified_queries > 0) {
            *false_positive_rate = (double)qfdb->fp_rehashes / qfdb->verified_queries;
        } else {
            *false_positive_rate = 0.0;
        }
    }
}

int qfdb_rehash_bucket(QFDB *qfdb, uint64_t bucket_idx) {
    // Do rehahshing here???
    
    // Find all miniruns in this bucket
    uint64_t minirun_mask = bucket_idx << qfdb->qf->metadata->bits_per_slot;
    uint64_t minirun_mask_end = (bucket_idx + 1) << qfdb->qf->metadata->bits_per_slot;

    
    return 0; 
}
