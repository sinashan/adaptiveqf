#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h> 
#include <unistd.h>  
#include <sys/time.h> 
#include <errno.h>
#include "hashutil.h"
#include "include/qf_splinterdb.h"
#include "include/gqf_int.h"
#include "include/splinter_util.h" 
#include "splinterdb/splinterdb.h" 
#include "splinterdb/data.h"   
#define DEBUG_MODE 0
#define DEBUG_PRINT(fmt, ...) \
    do { if (DEBUG_MODE) fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, __FILE__, \
                                __LINE__, __func__, ##__VA_ARGS__); } while (0)

static double get_time_usec_internal() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}


static int
key_compare_func(const data_config *cfg, slice key1, slice key2)
{
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
    uint64_t k;
    if (length >= sizeof(uint64_t)) {
        memcpy(&k, key, sizeof(uint64_t));
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return (uint32_t)(k ^ seed);
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
    slice value_slice = message_slice(msg);

    size_t num_values = value_slice.length / MAX_VAL_SIZE;
    size_t printed_len = 0;
    printed_len += snprintf(str + printed_len, max_len - printed_len, "[");
    for (size_t i = 0; i < num_values && printed_len < max_len - 1; ++i) {
        uint64_t val;
        memcpy(&val, (char*)value_slice.data + i * MAX_VAL_SIZE, sizeof(uint64_t));
        printed_len += snprintf(str + printed_len, max_len - printed_len, "%s%lu",
                                (i > 0 ? ", " : ""), val);
    }
     if (printed_len < max_len - 1) {
        snprintf(str + printed_len, max_len - printed_len, "]");
     } else {
       
         if (max_len > 0) str[max_len - 1] = '\0';
         if (max_len > 3) { str[max_len - 2] = '.'; str[max_len - 3] = '.'; str[max_len - 4] = '.';}
     }
}


static int merge_tuples_func(const data_config *cfg,
                              slice key,           
                              message old_message,  
                              merge_accumulator *new_message) 
{

	int current_len = merge_accumulator_length(new_message);
	size_t new_len = current_len + message_length(old_message);

	if (!merge_accumulator_resize(new_message, new_len)) {
         fprintf(stderr, "ERROR: Failed to resize merge accumulator for key %.*s\n", (int)slice_length(key), (char*)slice_data(key));
         return -1; // Indicate failure
    }

	memcpy(merge_accumulator_data(new_message) + current_len,
          message_data(old_message),
          message_length(old_message));

	return 0;
}


static int merge_tuples_final_func(const data_config *cfg,
                                    slice key,
                                    merge_accumulator *merged_message)
{

   merge_accumulator_set_class(merged_message, MESSAGE_TYPE_INSERT);
   return 0;
}


static int bm_add_key(bm_entry_t **hashtable_head, uint64_t minirun_id, uint64_t original_key) {
    bm_entry_t *entry;
    HASH_FIND(hh, *hashtable_head, &minirun_id, sizeof(uint64_t), entry);

    if (entry == NULL) {
        // Create new entry
        entry = (bm_entry_t*)malloc(sizeof(bm_entry_t));
        if (!entry) return -ENOMEM;
        entry->minirun_id = minirun_id;
        entry->capacity = BM_ENTRY_INITIAL_CAPACITY;
        entry->original_keys = (uint64_t*)malloc(entry->capacity * sizeof(uint64_t));
        if (!entry->original_keys) { free(entry); return -ENOMEM; }
        entry->original_keys[0] = original_key;
        entry->num_keys = 1;
        HASH_ADD(hh, *hashtable_head, minirun_id, sizeof(uint64_t), entry);
    } else {
        
        bool key_present = false;
        for(size_t i=0; i < entry->num_keys; ++i) {
            if(entry->original_keys[i] == original_key) {
                key_present = true;
                break;
            }
        }
        if (!key_present) {
            if (entry->num_keys >= entry->capacity) {
                
                size_t new_capacity = entry->capacity * 2;
                uint64_t* new_keys = (uint64_t*)realloc(entry->original_keys, new_capacity * sizeof(uint64_t));
                if (!new_keys) return -ENOMEM; 
                entry->original_keys = new_keys;
                entry->capacity = new_capacity;
            }
            entry->original_keys[entry->num_keys] = original_key;
            entry->num_keys++;
        }
    }
    return 0; 
}


static bm_entry_t* bm_find_entry(bm_entry_t *hashtable_head, uint64_t minirun_id) {
    bm_entry_t *entry;
    HASH_FIND(hh, hashtable_head, &minirun_id, sizeof(uint64_t), entry);
    return entry;
}


static double get_time_usec() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}

static void bm_destroy(bm_entry_t **hashtable_head) {
    bm_entry_t *current_entry, *tmp_entry;
    HASH_ITER(hh, *hashtable_head, current_entry, tmp_entry) {
        HASH_DEL(*hashtable_head, current_entry);
        free(current_entry->original_keys);
        free(current_entry);
    }
    *hashtable_head = NULL; 
}
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits, const char* db_path) { 
    QFDB *qfdb = (QFDB*)calloc(1,sizeof(QFDB));
    if (!qfdb) {
        perror("Failed to allocate QFDB struct");
        return NULL;
    }


    qfdb->db_path_original = strdup(db_path);
    if (!qfdb->db_path_original) {
        perror("Failed to duplicate db path string using strdup");
        free(qfdb);
        return NULL;
    }


    qfdb->bm_hashtable = NULL;
    if (pthread_mutex_init(&qfdb->bm_mutex, NULL) != 0) {
        perror("Failed to initialize BM mutex");
        free(qfdb->db_path_original);
        free(qfdb);
        return NULL;
    }


    qfdb->qf = (QF*)malloc(sizeof(QF));
    if (!qfdb->qf) {
        perror("Failed to allocate QF struct");
        pthread_mutex_destroy(&qfdb->bm_mutex);
        free(qfdb->db_path_original);
        free(qfdb);
        return NULL;
    }
    qfdb->qf->runtimedata = NULL;


    uint32_t qf_seed = 10;
    if (!qf_malloc(qfdb->qf, 1ULL << qbits, qbits + rbits, 0,
                  QF_HASH_DEFAULT, qf_seed)) {
        fprintf(stderr, "ERROR: qf_malloc failed\n");
        pthread_mutex_destroy(&qfdb->bm_mutex);
        free(qfdb->db_path_original);
        free(qfdb->qf); 
        free(qfdb);
        return NULL;
    }

    qf_set_auto_resize(qfdb->qf,false);


    fprintf(stdout, "[QFDB Init] qf_new allocated. Metadata: nslots=%lu, key_bits=%u, value_bits=%u, range=%lu, noccupied_slots=%lu\n",
    qfdb->qf->metadata->nslots, qfdb->qf->metadata->key_bits, qfdb->qf->metadata->value_bits,
    qfdb->qf->metadata->range, qfdb->qf->metadata->noccupied_slots);

    if (qfdb->qf->runtimedata) {
        fprintf(stdout, "[QFDB Init] qf_new runtimedata: adaptive_threshold=%lu\n", qfdb->qf->runtimedata->adaptive_slot_threshold);
    } else {
        fprintf(stdout, "[QFDB Init] qf_new runtimedata is NULL.\n");
    }

    printf("slots %lu\n",qfdb->qf->metadata->noccupied_slots);
            


    data_config *data_cfg = (data_config *)malloc(sizeof(data_config));
    if (!data_cfg) {
        perror("Failed to allocate data_config");
        pthread_mutex_destroy(&qfdb->bm_mutex);
        free(qfdb->db_path_original);
        qf_free(qfdb->qf); 
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }
    memset(data_cfg, 0, sizeof(data_config));
    data_cfg->max_key_size = MAX_KEY_SIZE;
    data_cfg->key_compare = key_compare_func;
    data_cfg->key_hash = key_hash_func;
    data_cfg->key_to_string = key_to_string_func;
    data_cfg->message_to_string = message_to_string_func;
    data_cfg->merge_tuples = merge_tuples_func;
    data_cfg->merge_tuples_final = merge_tuples_final_func;
    qfdb->data_cfg = data_cfg;


    splinterdb_config db_cfg;
    memset(&db_cfg, 0, sizeof(splinterdb_config));
    db_cfg.filename   = qfdb->db_path_original; 
    db_cfg.cache_size = 1024 * 1024 * 128;
    db_cfg.disk_size  = 1024 * 1024 * 1024 * 1; // 1 GB 
    db_cfg.data_cfg   = qfdb->data_cfg;

    fprintf(stdout, "[QFDB Init] Creating primary DB at %s...\n", qfdb->db_path_original);
    int db_rc = splinterdb_create(&db_cfg, &qfdb->db);
    if (db_rc != 0) {
        fprintf(stderr, "ERROR: splinterdb_create for primary DB failed: %d (%s)\n", db_rc, strerror(db_rc));
        pthread_mutex_destroy(&qfdb->bm_mutex);
        free(qfdb->db_path_original);
        free(qfdb->data_cfg);
        qf_free(qfdb->qf); free(qfdb->qf); free(qfdb);
        return NULL;
    }
    fprintf(stdout, "[QFDB Init] Primary DB created successfully.\n");

    // Initialize counters
    qfdb->fp_rehashes = 0;
    qfdb->fp_retrievals = 0;
    qfdb->total_queries = 0;
    qfdb->verified_queries = 0;
    qfdb->adaptations_performed = 0;
    qfdb->space_errors = 0;

    // Initialize rehash mutex
    if (pthread_mutex_init(&qfdb->rehash_mutex, NULL) != 0) {
        perror("Failed to initialize rehash mutex");
        pthread_mutex_destroy(&qfdb->bm_mutex);
        splinterdb_close(&qfdb->db); 
        free(qfdb->db_path_original);
        free(qfdb->data_cfg);
        qf_free(qfdb->qf); free(qfdb->qf); free(qfdb);
        return NULL;
    }

    // Initialize rehash state
    qfdb->qf_new_pending = NULL;
    qfdb->bm_new_hashtable_pending = NULL;
    qfdb->rehash_seed = 0;
    qfdb->rehash_copy_complete = false;

   
    qfdb_set_rehash_threshold(qfdb, (uint64_t)(qfdb->qf->metadata->nslots * 0.15));

    return qfdb;
}

static double get_time_usec_rehash_task() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}


static int qfdb_rehash_in_place(QFDB* qfdb){
    if (pthread_mutex_lock(&qfdb->rehash_mutex) != 0) {
         return -EBUSY; 
    }

    double rehash_start_time = get_time_usec_rehash_task();
    int rc = 0;
    uint64_t items_processed = 0;
    uint64_t qf_insert_failures = 0;
    uint64_t bm_add_failures = 0;
    splinterdb_iterator *it_primary = NULL;

    // old QF parameters
    uint64_t old_nslots = qfdb->qf->metadata->nslots;
    uint64_t old_key_bits = qfdb->qf->metadata->key_bits;
    uint64_t old_value_bits = qfdb->qf->metadata->value_bits; // Be careful with this one
    enum qf_hashmode old_hash_mode = qfdb->qf->metadata->hash_mode;
    uint32_t old_seed = qfdb->qf->metadata->seed;
    uint32_t new_seed = old_seed + 1;

    bm_destroy(&qfdb->bm_hashtable); 
    qf_free(qfdb->qf);

    if (!qf_malloc(qfdb->qf, old_nslots, old_key_bits, old_value_bits,
                   old_hash_mode, new_seed)) {
        fprintf(stderr, "[Rehash In-Place] FATAL: qf_malloc failed during re-initialization.\n");
        rc = -ENOMEM;
        //  critical state.
        goto rehash_fail_unlock;
    }

    qf_set_auto_resize(qfdb->qf, false);
    qfdb->rehash_seed = new_seed;

    splinterdb_register_thread(qfdb->db);

    rc = splinterdb_iterator_init(qfdb->db, &it_primary, NULL_SLICE);
    if (rc != 0) {
        fprintf(stderr, "[Rehash In-Place] ERROR: Failed to init iterator for primary DB: %d (%s)\n", rc, strerror(rc));
        splinterdb_deregister_thread(qfdb->db);
        goto rehash_fail_unlock;
    }

    while (splinterdb_iterator_valid(it_primary)) {
        slice key_slice_orig, val_slice_orig;
        splinterdb_iterator_get_current(it_primary, &key_slice_orig, &val_slice_orig);

        if (slice_length(key_slice_orig) < sizeof(uint64_t)) {
             fprintf(stderr, "[Rehash In-Place] WARN: Skipping primary DB entry with unexpected key size %zu.\n", slice_length(key_slice_orig));
             splinterdb_iterator_next(it_primary);
             continue;
        }
        uint64_t original_key;
        memcpy(&original_key, slice_data(key_slice_orig), sizeof(uint64_t));

        
        uint64_t current_hash; 
        qf_insert_result qf_res;
        int qf_ret = qf_insert_using_ll_table(qfdb->qf, original_key, 1, &qf_res, QF_NO_LOCK); // NO_LOCK ok - we hold master lock
        if (qf_ret < 0) {
            fprintf(stderr, "[Rehash In-Place] ERROR: Failed insert into QF (orig key %lu, code %d).\n", original_key, qf_ret);
            qf_insert_failures++;
            if (qf_ret == QF_NO_SPACE) {
                  fprintf(stderr, "[Rehash In-Place] FATAL: QF ran out of space during rebuild. Aborting.\n");
                  rc = QF_NO_SPACE; 
                  goto rebuild_loop_fail;
            }
          
            rc = qf_ret;
            goto rebuild_loop_fail;
        }


        uint64_t new_minirun_id = qf_res.minirun_id;
        int bm_add_ret = bm_add_key(&qfdb->bm_hashtable, new_minirun_id, original_key);
        if (bm_add_ret != 0) {
            fprintf(stderr, "[Rehash In-Place] ERROR: Failed add to BM hash table (orig key %lu, new_id %lu, code %d).\n",
                    original_key, new_minirun_id, bm_add_ret);
            bm_add_failures++;
            fprintf(stderr, "[Rehash In-Place] FATAL: Memory allocation failed for BM. Aborting.\n");
            rc = bm_add_ret;
            goto rebuild_loop_fail;
        }

        items_processed++;
        splinterdb_iterator_next(it_primary);
    }

    rc = splinterdb_iterator_status(it_primary);
    splinterdb_iterator_deinit(it_primary); it_primary = NULL;
    splinterdb_deregister_thread(qfdb->db);

    if (rc != 0) {
        fprintf(stderr, "[Rehash In-Place] ERROR: Iterator failed during primary DB read: %d (%s)\n", rc, strerror(rc));
        goto rehash_fail_unlock; 
    }

    double rehash_end_time = get_time_usec_rehash_task();
    fprintf(stdout, "[Rehash In-Place] Rebuild successful. Processed %lu items in %.3f ms.\n",
            items_processed, (rehash_end_time - rehash_start_time) / 1000.0);
    if (qf_insert_failures > 0) printf("  WARNING: %lu QF insert failures occurred.\n", qf_insert_failures);
    if (bm_add_failures > 0) printf("  WARNING: %lu BM add/alloc failures occurred.\n", bm_add_failures);
    printf("  New QF Occupancy: %lu / %lu (%.2f%%)\n", qfdb->qf->metadata->noccupied_slots, qfdb->qf->metadata->nslots, (double)qfdb->qf->metadata->noccupied_slots * 100.0 / qfdb->qf->metadata->nslots);

    pthread_mutex_unlock(&qfdb->rehash_mutex);
    fprintf(stdout, "[Rehash In-Place] Released lock, rehash complete.\n");
    return 0; // Success

    // --- Failure Handling Sections ---
    rebuild_loop_fail:
        if (it_primary) splinterdb_iterator_deinit(it_primary);
        splinterdb_deregister_thread(qfdb->db);
        fprintf(stderr, "[Rehash In-Place] FATAL: Rebuild loop failed (rc=%d).\n", rc);
        fprintf(stderr, "[Rehash In-Place] Clearing potentially partial BM...\n");
        bm_destroy(&qfdb->bm_hashtable);

    rehash_fail_unlock:
        fprintf(stderr, "[Rehash In-Place] Rehash process failed.\n");
        pthread_mutex_unlock(&qfdb->rehash_mutex);
        fprintf(stderr, "[Rehash In-Place] Released lock after failure.\n");
        return rc; 

}


static void qfdb_initiate_rehash_if_needed(QFDB *qfdb) {
    if (!qfdb || !qfdb->qf || !qfdb->qf->runtimedata) {
        return;
    }

    QF *qf_active = qfdb->qf;

    pc_sync(&qf_active->runtimedata->pc_nadaptive_slots);
    uint64_t current_adaptive_slots = qf_active->runtimedata->nadaptive_slots;
    uint64_t threshold = qf_active->runtimedata->adaptive_slot_threshold;

    if (current_adaptive_slots >= threshold) {

        printf("Started rehashing\n");

        double rehash_start = get_time_usec();
        int rehash_rc = qfdb_rehash_in_place(qfdb);
        double rehash_end = get_time_usec();
        double rehash_duration = (rehash_end - rehash_start) / 1000000.0;

        if (rehash_rc == 0) {
            fprintf(stdout, "[QFDB] In-place rehash completed successfully in %.3f seconds.\n", rehash_duration);
        } else {
            fprintf(stderr, "[QFDB] In-place rehash failed with code %d.\n", rehash_rc);
        }
    }
}

int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count) {
    if (!qfdb || !qfdb->db) {
        fprintf(stderr, "ERROR: qfdb_insert called with invalid QFDB structure (null db).\n");
        return -EINVAL;
    }


    if (pthread_mutex_lock(&qfdb->rehash_mutex) != 0) {
         fprintf(stderr, "ERROR: qfdb_insert failed to acquire lock.\n");
         return -EBUSY;
    }

    if (!qfdb->qf) {
         fprintf(stderr, "ERROR: qfdb_insert called with invalid QF (null qf).\n");
         pthread_mutex_unlock(&qfdb->rehash_mutex);
         return -EINVAL;
    }

    QF *qf_active = qfdb->qf;
    int final_rc = 0; 
    qf_insert_result qf_res;
    int qf_ret = qf_insert_using_ll_table(qf_active, key, count, &qf_res, QF_NO_LOCK); // NO_LOCK ok

    if (qf_ret < 0) {
        if (qf_ret == QF_NO_SPACE) {
            qfdb->space_errors++;
            fprintf(stderr, "WARN: Active QF is full (key %lu).\n", key);
        } else {
            fprintf(stderr, "ERROR: qf_insert_using_ll_table failed for key %lu with code %d\n", key, qf_ret);
        }
        final_rc = qf_ret; 
        goto insert_cleanup; 
    }

    char primary_key_buffer[MAX_KEY_SIZE];
    char primary_val_buffer[MAX_VAL_SIZE];
    slice primary_key_slice = padded_slice(&key, MAX_KEY_SIZE, sizeof(key), primary_key_buffer, 0);
    slice primary_val_slice = padded_slice(&count, MAX_VAL_SIZE, sizeof(count), primary_val_buffer, 0);

    int primary_db_ret = splinterdb_insert(qfdb->db, primary_key_slice, primary_val_slice);
    if (primary_db_ret != 0) {
        fprintf(stderr, "ERROR: SplinterDB insert into primary DB failed for key %lu with code %d (%s)\n",
                key, primary_db_ret, strerror(primary_db_ret));
        final_rc = -EIO;
        goto insert_cleanup;
    }

    // --- Insert/Update Backing Map (BM) using uthash ---
    uint64_t bm_key = qf_res.minirun_id;
    uint64_t bm_val = key;

    // Lock for BM is already held by rehash_mutex
    int bm_add_ret = bm_add_key(&qfdb->bm_hashtable, bm_key, bm_val);

    if (bm_add_ret != 0) {
        fprintf(stderr, "ERROR: Failed to add key %lu to BM hash table for minirun_id %lu (code %d)\n",
                bm_val, bm_key, bm_add_ret);
        final_rc = bm_add_ret;
        goto insert_cleanup;
    }

    

insert_cleanup:
    pthread_mutex_unlock(&qfdb->rehash_mutex);
    qfdb_initiate_rehash_if_needed(qfdb);
    return final_rc; // Return 0 on success, or the error code
}

int qfdb_query_filter(QFDB *qfdb, uint64_t key){
    if (!qfdb || !qfdb->qf) {
        fprintf(stderr, "ERROR: qf_query_only called with invalid QFDB structure.\n");
        return -2;
    }
    if (pthread_mutex_lock(&qfdb->rehash_mutex) != 0) {
         fprintf(stderr, "ERROR: qfdb_query failed to acquire lock.\n");
         return -EBUSY;
    }
    QF *qf_to_query = qfdb->qf;
    uint64_t query_hash_result; 
    int minirun_rank;
    minirun_rank = qf_query_using_ll_table(qf_to_query, key, &query_hash_result, QF_WAIT_FOR_LOCK);
    pthread_mutex_unlock(&qfdb->rehash_mutex);
    qfdb_initiate_rehash_if_needed(qfdb);
    return minirun_rank;
}

int qfdb_query(QFDB *qfdb, uint64_t key) {
    if (!qfdb || !qfdb->db) { 
         fprintf(stderr, "ERROR: qfdb_query called with invalid QFDB structure (null db).\n");
         return -EINVAL; 
    }


    if (pthread_mutex_lock(&qfdb->rehash_mutex) != 0) {
         fprintf(stderr, "ERROR: qfdb_query failed to acquire lock.\n");
         return -EBUSY;
    }

     if (!qfdb->qf) {
         fprintf(stderr, "ERROR: qfdb_query called with invalid QF (null qf).\n");
         pthread_mutex_unlock(&qfdb->rehash_mutex);
         return -EINVAL;
     }

    QF *qf_active = qfdb->qf;
    int final_rc = 0; 

    uint64_t query_hash_result_active = 0;
    int minirun_rank = -1;
    minirun_rank = qf_query_using_ll_table(qf_active, key, &query_hash_result_active, QF_NO_LOCK); // NO_LOCK ok

    if (minirun_rank < 0) {
        final_rc = 0; 
        goto query_cleanup;
    }

    splinterdb_lookup_result primary_db_lookup_res;
    splinterdb_lookup_result_init(qfdb->db, &primary_db_lookup_res, 0, NULL);
    char primary_key_buffer[MAX_KEY_SIZE];
    slice primary_key_slice = padded_slice(&key, MAX_KEY_SIZE, sizeof(key), primary_key_buffer, 0);
    int primary_db_ret = splinterdb_lookup(qfdb->db, primary_key_slice, &primary_db_lookup_res);

    if (primary_db_ret != 0) {
        fprintf(stderr, "ERROR: SplinterDB lookup in primary DB failed for key %lu with code %d (%s)\n",
                key, primary_db_ret, strerror(primary_db_ret));
        splinterdb_lookup_result_deinit(&primary_db_lookup_res);
        final_rc = -EIO;
        goto query_cleanup;
    }

    bool found_in_primary_db = splinterdb_lookup_found(&primary_db_lookup_res);
    splinterdb_lookup_result_deinit(&primary_db_lookup_res);

    if (found_in_primary_db) {
        final_rc = 1; 
        goto query_cleanup;
    }

    DEBUG_PRINT("Query key %lu: NOT found in primary DB (QF False Positive).\n", key);
    qfdb->fp_rehashes++;
    final_rc = -1;
    bool adaptation_attempted = false;
    uint64_t bm_key = query_hash_result_active & BITMASK(qf_active->metadata->quotient_remainder_bits);
    uint64_t conflicting_orig_key = 0;
    bool found_conflicting_key_in_bm = false;

    if (qfdb->bm_hashtable) {
        bm_entry_t *entry = bm_find_entry(qfdb->bm_hashtable, bm_key);
        if (entry != NULL) {
            if (entry->num_keys > 0 && (size_t)minirun_rank < entry->num_keys) {
                conflicting_orig_key = entry->original_keys[minirun_rank];
                found_conflicting_key_in_bm = true;
            } else {
                 fprintf(stderr, "WARN: Invalid minirun_rank %d for BM entry (num_keys %zu) for minirun_id %lu.\n",
                              minirun_rank, entry->num_keys, bm_key);
            }
        } else {
             fprintf(stderr, "WARN: QF positive for key %lu (bm_key %lu, rank %d), but minirun_id NOT found in BM.\n",
                     key, bm_key, minirun_rank);
        }
    } else {
         fprintf(stderr, "WARN: Active BM hash table is NULL during query for key %lu.\n", key);
    }
 
    if (found_conflicting_key_in_bm) {
         if (conflicting_orig_key != key && key != 0 && conflicting_orig_key != 0) {
              DEBUG_PRINT("Query key %lu: Adapting QF against conflicting key %lu (rank %d).\n",
                         key, conflicting_orig_key, minirun_rank);
              adaptation_attempted = true;
              qfdb->adaptations_performed++; 

              int adapt_ret = qf_adapt_using_ll_table(qf_active,
                                                      conflicting_orig_key,
                                                      key,
                                                      minirun_rank,
                                                      QF_NO_LOCK);

              if (adapt_ret < 0) {
                   if (adapt_ret == QF_NO_SPACE) {
                        qfdb->space_errors++;
                   } else {
                        fprintf(stderr, "WARN: qf_adapt_using_ll_table failed for conflicting_orig %lu / fp_key %lu with code %d\n",
                                conflicting_orig_key, key, adapt_ret);
                   }
              }
         } else {
              DEBUG_PRINT("Query key %lu: Adaptation skipped (conflicting_key %lu == query_key %lu or zero).\n", key, conflicting_orig_key, key);
         }
    }

    

query_cleanup:
    pthread_mutex_unlock(&qfdb->rehash_mutex);
    if (adaptation_attempted) {
        qfdb_initiate_rehash_if_needed(qfdb);
    }
    return final_rc;
}

int qfdb_resize(QFDB *qfdb, uint64_t new_qbits) {
    if (!qfdb || !qfdb->qf || !qfdb->qf->runtimedata) return -1;
    fprintf(stderr, "WARN: qfdb_resize called. This only resizes the QF, not SplinterDB. Rehashing is preferred.\n");
   
    if (!qfdb->qf->runtimedata->container_resize) {
         fprintf(stderr, "ERROR: QF container_resize function not set.\n");
         return -1;
    }
    return qfdb->qf->runtimedata->container_resize(qfdb->qf, 1ULL << new_qbits);
}

void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries,
                   uint64_t *fp_rehashes, uint64_t *adaptations_performed, uint64_t *space_errors, double *false_positive_rate) {
    if (!qfdb) return;
   
    if (total_queries) *total_queries = qfdb->total_queries;
    if (verified_queries) *verified_queries = qfdb->verified_queries;
    if (fp_rehashes) *fp_rehashes = qfdb->fp_rehashes; 
    if (adaptations_performed) *adaptations_performed = qfdb->adaptations_performed;
    if (space_errors) *space_errors = qfdb->space_errors;
    if (false_positive_rate) {
        if (qfdb->verified_queries > 0) {
            *false_positive_rate = (double)qfdb->adaptations_performed / qfdb->verified_queries;
        } else {
            *false_positive_rate = 0.0;
        }
    }
}

void qfdb_set_rehash_threshold(QFDB *qfdb, uint64_t threshold) {
    if (qfdb && qfdb->qf && qfdb->qf->runtimedata) {
        qfdb->qf->runtimedata->adaptive_slot_threshold = threshold;
        printf("[QFDB] Set adaptive slot threshold to %lu\n", threshold);
    } else {
         fprintf(stderr, "WARN: Cannot set rehash threshold - invalid QFDB state.\n");
    }
}


uint64_t qfdb_get_adaptive_slots(QFDB *qfdb) {
    if (qfdb && qfdb->qf && qfdb->qf->runtimedata) {
        pc_sync(&qfdb->qf->runtimedata->pc_nadaptive_slots);
        return qfdb->qf->runtimedata->nadaptive_slots;
    }

    return 0; 
}



void qfdb_destroy(QFDB *qfdb) {
    if (!qfdb) return;


    if (qfdb->db) {
        splinterdb *tmp = qfdb->db;
        qfdb->db = NULL;
        fprintf(stdout, "[QFDB Destroy] Closing primary SplinterDB...\n");
        splinterdb_close(&tmp);
        fprintf(stdout, "[QFDB Destroy] Primary SplinterDB closed.\n");
    }
    fprintf(stdout, "[QFDB Destroy] Freeing active BM (uthash) table...\n");
    bm_destroy(&qfdb->bm_hashtable); 
     if (qfdb->bm_new_hashtable_pending != NULL) {
        fprintf(stdout, "[QFDB Destroy] Freeing pending BM (uthash) table...\n");
        bm_destroy(&qfdb->bm_new_hashtable_pending);
    }

    if (qfdb->qf_new_pending){
        fprintf(stdout, "[QFDB Destroy] Freeing pending new QF %p...\n", (void*)qfdb->qf_new_pending);
        qf_free(qfdb->qf_new_pending);
        free(qfdb->qf_new_pending);
        qfdb->qf_new_pending = NULL;
    }

    if (qfdb->qf) {
        fprintf(stdout, "[QFDB Destroy] Freeing active QF %p...\n", (void*)qfdb->qf);
        qf_free(qfdb->qf);
        free(qfdb->qf);
        qfdb->qf = NULL;
    }
    if (qfdb->data_cfg) {
        free(qfdb->data_cfg);
        qfdb->data_cfg = NULL;
    }
    if (qfdb->db_path_original) { free(qfdb->db_path_original); qfdb->db_path_original = NULL; }
    free(qfdb);
    fprintf(stdout, "[QFDB Destroy] QFDB structure freed.\n");
}