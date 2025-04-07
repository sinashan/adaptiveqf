#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <pthread.h> // Added for pthread types/functions
#include <unistd.h>  // For usleep
#include <sys/time.h> // For gettimeofday

#include "hashutil.h"
#include "qf_splinterdb.h"
#include "gqf_int.h"
#include "include/splinter_util.h" // Make sure path is correct
#include "splinterdb/splinterdb.h" // Include main SplinterDB header
#include "splinterdb/data.h"       // Include for slice

#define DEBUG_MODE 0


#define DEBUG_PRINT(fmt, ...) \
    do { if (DEBUG_MODE) fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, __FILE__, \
                                __LINE__, __func__, ##__VA_ARGS__); } while (0)

// Assume MAX_KEY_SIZE and MAX_VAL_SIZE are correctly defined in splinter_util.h
// Typically:
// #define MAX_KEY_SIZE sizeof(uint64_t)
// #define MAX_VAL_SIZE sizeof(uint64_t)

// Helper to get time (if not already available in main)
static double get_time_usec_internal() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}


// Define key comparison and related functions
static int
key_compare_func(const data_config *cfg, slice key1, slice key2)
{
    // Compare two keys (uint64_t values)
    // Handle potential padding differences if MAX_KEY_SIZE > sizeof(uint64_t)
    // but assuming MAX_KEY_SIZE == sizeof(uint64_t) for simplicity now.
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
    // Use the actual key size for hashing, ignore padding
    if (length >= sizeof(uint64_t)) {
        memcpy(&k, key, sizeof(uint64_t));
        // Simple hash, replace if needed
        k ^= k >> 33;
        k *= 0xff51afd7ed558ccdULL;
        k ^= k >> 33;
        k *= 0xc4ceb9fe1a85ec53ULL;
        k ^= k >> 33;
        return (uint32_t)(k ^ seed);
    } else {
        // Handle smaller keys if necessary, or return 0/error
        return 0;
    }
}

static void
key_to_string_func(const data_config *cfg, slice key, char *str, size_t max_len)
{
    uint64_t k;
    // Read only the uint64_t part, ignoring potential padding
    memcpy(&k, key.data, sizeof(uint64_t));
    snprintf(str, max_len, "%lu", k);
}

static void
message_to_string_func(const data_config *cfg, message msg, char *str, size_t max_len)
{
    slice value_slice = message_slice(msg);
    // The message can be a list of uint64_t values (MAX_VAL_SIZE chunks)
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
         // Ensure null termination if buffer filled up
         if (max_len > 0) str[max_len - 1] = '\0';
         if (max_len > 3) { str[max_len - 2] = '.'; str[max_len - 3] = '.'; str[max_len - 4] = '.';}
     }
}

// Merge function: Appends the old message (a single padded uint64_t)
// to the accumulator (which holds concatenated padded uint64_t values).
static int merge_tuples_func(const data_config *cfg,
                              slice key,            // The SplinterDB key (minirun_id)
                              message old_message,  // The new value being merged (original_key)
                              merge_accumulator *new_message) // Accumulator for the list
{

	int current_len = merge_accumulator_length(new_message);
	size_t new_len = current_len + message_length(old_message);

	// Resize the accumulator to hold the existing list plus the new value
	if (!merge_accumulator_resize(new_message, new_len)) {
         fprintf(stderr, "ERROR: Failed to resize merge accumulator for key %.*s\n", (int)slice_length(key), (char*)slice_data(key));
         return -1; // Indicate failure
    }

	// Append the new value (old_message) to the end of the accumulator
	memcpy(merge_accumulator_data(new_message) + current_len,
          message_data(old_message),
          message_length(old_message));

	return 0; // Success
}

// Merge finalizer: Simply ensures the final merged message is of type INSERT.
// (No actual data modification needed here for simple list append).
static int merge_tuples_final_func(const data_config *cfg,
                                    slice key,
                                    merge_accumulator *merged_message)
{
   // The data is already correctly formatted as a concatenated list.
   // Just set the type.
   merge_accumulator_set_class(merged_message, MESSAGE_TYPE_INSERT);
   return 0;
}


// --- qfdb_init ---
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits, const char* db_path) {
    QFDB *qfdb = (QFDB*)calloc(1, sizeof(QFDB));
    if (!qfdb) return NULL;

    // Initialize the QF
    qfdb->qf = (QF*)malloc(sizeof(QF));
    if (!qfdb->qf) {
        free(qfdb);
        return NULL;
    }
    qfdb->qf->runtimedata = NULL; // Ensure runtimedata is NULL before qf_malloc allocates it

    // Create QF with specified parameters
    // Use a non-zero seed for better hashing (e.g., 10 or time-based)
    uint32_t qf_seed = 10; // Or use time(NULL) or a fixed value
    if (!qf_malloc(qfdb->qf, 1ULL << qbits, qbits + rbits, 0,
                  QF_HASH_INVERTIBLE, qf_seed)) {
        fprintf(stderr, "ERROR: qf_malloc failed\n");
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }

    // Create data_config
    data_config *data_cfg = (data_config *)malloc(sizeof(data_config));
    if (!data_cfg) {
        qf_free(qfdb->qf);
        free(qfdb->qf);
        free(qfdb);
        return NULL;
    }
    memset(data_cfg, 0, sizeof(data_config));

    // *** FIX: Set key size to match the actual key type (uint64_t) ***
    data_cfg->max_key_size = MAX_KEY_SIZE;
    // Value size should match the padded size used by db_insert/padded_slice

    // Set callback functions
    data_cfg->key_compare = key_compare_func;
    data_cfg->key_hash = key_hash_func;
    data_cfg->key_to_string = key_to_string_func;
    data_cfg->message_to_string = message_to_string_func;
    // *** Assign correct merge functions ***
    data_cfg->merge_tuples = merge_tuples_func;
    data_cfg->merge_tuples_final = merge_tuples_final_func;


    // Save data_config
    qfdb->data_cfg = data_cfg;

    // Initialize SplinterDB configuration
    splinterdb_config cfg;
    memset(&cfg, 0, sizeof(splinterdb_config));
    cfg.filename = db_path;
    cfg.cache_size = 1024 * 1024 * 128; // 128MB cache
    cfg.disk_size = 1024 * 1024 * 256;  // 256MB disk size (REQUIRED)
    cfg.data_cfg = qfdb->data_cfg;      // Use the configured data_cfg

    // Create SplinterDB
    if (splinterdb_create(&cfg, &qfdb->ext_store) != 0) {
        fprintf(stderr, "ERROR: splinterdb_create failed\n");
        free(qfdb->data_cfg);
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
    qfdb->adaptations_performed = 0;
    qfdb->space_errors = 0; // Initialize space errors

    // Initialize rehash state
    if (pthread_mutex_init(&qfdb->rehash_mutex, NULL)!=0){
        perror("Failed to initialize rehash mutex");
        splinterdb_close(&qfdb->ext_store); // Close DB if open
        free(qfdb->data_cfg);
        qf_free(qfdb->qf); free(qfdb->qf); free(qfdb);
        return NULL;
    }

    qfdb->qf_new_pending = NULL;
    qfdb->rehash_seed = 0; // Will be set when rehash starts
    qfdb->rehash_copy_complete = false;

    return qfdb;
}
// Helper to get time (specific to this file to avoid conflicts if main has one)
static double get_time_usec_rehash_task() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000000.0 + tv.tv_usec;
}

// --- The Background Rehash Task ---
// This function reads all original keys from the current SplinterDB,
// inserts them into the new (pending) QF using the new seed,
// clears the old SplinterDB entries, and repopulates SplinterDB
// mapping the new minirun_ids to the original keys.
static void* qfdb_rehash_copy_task(void *qfdb_ptr) {
    QFDB *qfdb = (QFDB*)qfdb_ptr;
    int rc = 0;
    bool verbose = false;

    // --- Pre-checks ---
    // (Same as before)
    if (!qfdb || !qfdb->qf || !qfdb->qf_new_pending || !qfdb->ext_store || !qfdb->qf->runtimedata) {
         fprintf(stderr, "[Rehash Task] ERROR: Invalid QFDB state for rehash copy (NULL pointers).\n");
         if (qfdb && qfdb->qf && qfdb->qf->runtimedata) qfdb->qf->runtimedata->rehashing = false;
         if (qfdb) qfdb->rehash_copy_complete = true;
         return NULL;
    }
    if (!qfdb->qf->runtimedata->rehashing) {
         fprintf(stderr, "[Rehash Task] ERROR: Rehash flag is unexpectedly false. Aborting task.\n");
         if (qfdb) qfdb->rehash_copy_complete = true;
         return NULL;
    }

    fprintf(stdout, "[Rehash Task] Starting copy/rebuild. Old QF %p (seed %u) -> New QF %p (seed %u).\n",
            (void*)qfdb->qf, qfdb->qf->metadata->seed,
            (void*)qfdb->qf_new_pending, qfdb->rehash_seed);

    QF *qf_old = qfdb->qf;
    QF *qf_new = qfdb->qf_new_pending;
    splinterdb *db = qfdb->ext_store;

    splinterdb_register_thread(db);

    // Temporary buffers
    uint64_t *all_original_keys = NULL;
    uint64_t keys_capacity = 0;
    uint64_t keys_count = 0;
    const uint64_t keys_initial_capacity = 1000000;

    char **keys_to_delete = NULL; // Array of pointers to key data
    size_t *key_delete_lengths = NULL; // Array of lengths for keys_to_delete
    uint64_t delete_keys_capacity = 0;
    uint64_t delete_keys_count = 0;
    const uint64_t delete_keys_initial_capacity = 100000;


    // --- Phase 1: Read all original keys from SplinterDB into memory ---
    // (Same as before - assumed correct)
    fprintf(stdout, "[Rehash Task] Phase 1: Reading all original keys from SplinterDB...\n");
    double p1_start = get_time_usec_rehash_task();
    splinterdb_iterator *it_read = NULL;
    rc = splinterdb_iterator_init(db, &it_read, NULL_SLICE);
    if (rc != 0) { fprintf(stderr, "[Rehash Task] ERROR: Failed to init iterator for reading: %d\n", rc); goto rehash_fail; }
    keys_capacity = keys_initial_capacity;
    all_original_keys = (uint64_t *)malloc(keys_capacity * sizeof(uint64_t));
    if (!all_original_keys) { fprintf(stderr, "[Rehash Task] ERROR: Failed malloc for original keys buffer.\n"); splinterdb_iterator_deinit(it_read); goto rehash_fail; }
    while (splinterdb_iterator_valid(it_read)) {
        slice key_slice_old, val_slice;
        splinterdb_iterator_get_current(it_read, &key_slice_old, &val_slice);
        char *value_data = slice_data(val_slice); size_t value_len = slice_length(val_slice);
        if (value_len == 0 || value_len % MAX_VAL_SIZE != 0) { splinterdb_iterator_next(it_read); continue; }
        size_t num_keys_in_entry = value_len / MAX_VAL_SIZE;
        if (keys_count + num_keys_in_entry > keys_capacity) {
            keys_capacity = (keys_count + num_keys_in_entry) + keys_initial_capacity;
            uint64_t *new_buffer = (uint64_t *)realloc(all_original_keys, keys_capacity * sizeof(uint64_t));
            if (!new_buffer) { fprintf(stderr, "[Rehash Task] ERROR: Failed realloc for original keys buffer.\n"); free(all_original_keys); all_original_keys = NULL; splinterdb_iterator_deinit(it_read); goto rehash_fail; }
            all_original_keys = new_buffer;
        }
        for (size_t i = 0; i < num_keys_in_entry; ++i) { memcpy(&all_original_keys[keys_count + i], value_data + i * MAX_VAL_SIZE, sizeof(uint64_t)); }
        keys_count += num_keys_in_entry;
        splinterdb_iterator_next(it_read);
    }
    rc = splinterdb_iterator_status(it_read); splinterdb_iterator_deinit(it_read);
    if (rc != 0) { fprintf(stderr, "[Rehash Task] ERROR: Iterator failed during read: %d\n", rc); goto rehash_fail; }
    double p1_end = get_time_usec_rehash_task();
    printf("[Rehash Task] Phase 1 complete. Read %lu original keys into memory in %.3f ms.\n", keys_count, (p1_end - p1_start) / 1000.0);


    // --- Phase 2: Insert all original keys into the new QF ---
    // (Same as before - assumed correct)
     fprintf(stdout, "[Rehash Task] Phase 2: Inserting %lu keys into new QF (seed %u)...\n", keys_count, qf_new->metadata->seed);
    double p2_start = get_time_usec_rehash_task();
    uint64_t items_rehashed_qf = 0;
    for (uint64_t i = 0; i < keys_count; ++i) {
        uint64_t original_key = all_original_keys[i];
        qf_insert_result qf_res_new;
        int qf_new_ret = qf_insert_using_ll_table(qf_new, original_key, 1, &qf_res_new, QF_NO_LOCK);
        if (qf_new_ret < 0) { fprintf(stderr, "[Rehash Task] ERROR: Failed insert into new QF (key %lu, code %d).\n", original_key, qf_new_ret); goto rehash_fail; }
        items_rehashed_qf++;
    }
    double p2_end = get_time_usec_rehash_task(); assert(items_rehashed_qf == keys_count);
    printf("[Rehash Task] Phase 2 complete. Inserted %lu items into new QF in %.3f ms.\n", items_rehashed_qf, (p2_end - p2_start) / 1000.0);
    printf("  New QF Occupancy: %lu / %lu (%.2f%%)\n", qf_new->metadata->noccupied_slots, qf_new->metadata->nslots, (double)qf_new->metadata->noccupied_slots * 100.0 / qf_new->metadata->nslots);


    // --- Phase 3 (Revised): Collect Keys to Delete, then Delete ---

    // --- Phase 3a: Collect old minirun_id keys ---
    fprintf(stdout, "[Rehash Task] Phase 3a: Collecting keys to delete from SplinterDB...\n");
    double p3a_start = get_time_usec_rehash_task();
    splinterdb_iterator *it_collect = NULL;
    rc = splinterdb_iterator_init(db, &it_collect, NULL_SLICE);
    if (rc != 0) {
        fprintf(stderr, "[Rehash Task] ERROR: Failed to initialize iterator for delete collection: %d\n", rc);
        goto rehash_fail;
    }

    // Allocate initial buffer for delete keys
    delete_keys_capacity = delete_keys_initial_capacity;
    keys_to_delete = (char **)malloc(delete_keys_capacity * sizeof(char *));
    key_delete_lengths = (size_t*)malloc(delete_keys_capacity * sizeof(size_t));
    if (!keys_to_delete || !key_delete_lengths) {
        fprintf(stderr, "[Rehash Task] ERROR: Failed malloc for delete key buffer.\n");
        splinterdb_iterator_deinit(it_collect);
        goto rehash_fail;
    }

    while (splinterdb_iterator_valid(it_collect)) {
        slice key_slice_old, val_slice_ignored;
        splinterdb_iterator_get_current(it_collect, &key_slice_old, &val_slice_ignored);

        // Ensure capacity
        if (delete_keys_count >= delete_keys_capacity) {
            delete_keys_capacity *= 2;
            char **new_ptr_buffer = (char **)realloc(keys_to_delete, delete_keys_capacity * sizeof(char *));
            size_t *new_len_buffer = (size_t*)realloc(key_delete_lengths, delete_keys_capacity * sizeof(size_t));
            if (!new_ptr_buffer || !new_len_buffer) {
                fprintf(stderr, "[Rehash Task] ERROR: Failed realloc for delete key buffer.\n");
                // Free partially collected keys before failing
                for(uint64_t i=0; i<delete_keys_count; ++i) free(keys_to_delete[i]);
                free(keys_to_delete); keys_to_delete = NULL;
                free(key_delete_lengths); key_delete_lengths = NULL;
                splinterdb_iterator_deinit(it_collect);
                goto rehash_fail;
            }
            keys_to_delete = new_ptr_buffer;
            key_delete_lengths = new_len_buffer;
        }

        // Allocate memory and copy the key data
        size_t key_len = slice_length(key_slice_old);
        keys_to_delete[delete_keys_count] = (char *)malloc(key_len);
        if (!keys_to_delete[delete_keys_count]) {
             fprintf(stderr, "[Rehash Task] ERROR: Failed malloc for individual delete key.\n");
             for(uint64_t i=0; i<delete_keys_count; ++i) free(keys_to_delete[i]); free(keys_to_delete); keys_to_delete = NULL; free(key_delete_lengths); key_delete_lengths = NULL;
             splinterdb_iterator_deinit(it_collect); goto rehash_fail;
        }
        memcpy(keys_to_delete[delete_keys_count], slice_data(key_slice_old), key_len);
        key_delete_lengths[delete_keys_count] = key_len;
        delete_keys_count++;

        splinterdb_iterator_next(it_collect);
    }

    rc = splinterdb_iterator_status(it_collect);
    splinterdb_iterator_deinit(it_collect); // Deinitialize collection iterator *before* deleting
    if (rc != 0) {
        fprintf(stderr, "[Rehash Task] ERROR: Iterator failed during delete collection: %d\n", rc);
        // Free collected keys before failing
         for(uint64_t i=0; i<delete_keys_count; ++i) free(keys_to_delete[i]); free(keys_to_delete); keys_to_delete = NULL; free(key_delete_lengths); key_delete_lengths = NULL;
        goto rehash_fail;
    }
    double p3a_end = get_time_usec_rehash_task();
    printf("[Rehash Task] Phase 3a complete. Collected %lu keys to delete in %.3f ms.\n",
           delete_keys_count, (p3a_end - p3a_start) / 1000.0);


    // --- Phase 3b: Delete collected keys ---
    fprintf(stdout, "[Rehash Task] Phase 3b: Deleting %lu old entries from SplinterDB...\n", delete_keys_count);
    double p3b_start = get_time_usec_rehash_task();
    uint64_t items_deleted = 0;
    for (uint64_t i = 0; i < delete_keys_count; ++i) {
        slice key_to_delete = slice_create(key_delete_lengths[i], keys_to_delete[i]);
        rc = splinterdb_delete(db, key_to_delete);
        if (rc != 0) {
            fprintf(stderr, "[Rehash Task] WARNING: Failed to delete key (index %lu, len %zu) from SplinterDB: %d. Key might remain.\n",
                    i, key_delete_lengths[i], rc);
        } else {
            items_deleted++;
        }
        free(keys_to_delete[i]); // Free the copied key data
    }
    free(keys_to_delete); // Free the array of pointers
    keys_to_delete = NULL;
    free(key_delete_lengths); // Free the lengths array
    key_delete_lengths = NULL;

    double p3b_end = get_time_usec_rehash_task();
    printf("[Rehash Task] Phase 3b complete. Attempted deletion of %lu/%lu entries in %.3f ms.\n",
           items_deleted, delete_keys_count, (p3b_end - p3b_start) / 1000.0);


    // --- Phase 4: Re-populate SplinterDB with new minirun_ids ---
    // (Same as before - assumed correct)
    fprintf(stdout, "[Rehash Task] Phase 4: Re-populating SplinterDB with new minirun_ids (using seed %u)...\n", qf_new->metadata->seed);
    double p4_start = get_time_usec_rehash_task();
    uint64_t items_reinserted_db = 0;
    char db_key_buffer[MAX_KEY_SIZE]; char db_val_buffer[MAX_VAL_SIZE];
    for (uint64_t i = 0; i < keys_count; ++i) {
        uint64_t original_key = all_original_keys[i]; uint64_t new_hash;
        if (qf_new->metadata->hash_mode == QF_HASH_DEFAULT) { new_hash = MurmurHash64A(&original_key, sizeof(original_key), qf_new->metadata->seed); }
        else if (qf_new->metadata->hash_mode == QF_HASH_INVERTIBLE) { new_hash = hash_64(original_key, qf_new->metadata->seed); }
        else { fprintf(stderr, "[Rehash Task] ERROR: Unsupported hash mode (%d).\n", qf_new->metadata->hash_mode); goto rehash_fail; }
        uint64_t new_minirun_id = new_hash & BITMASK(qf_new->metadata->quotient_remainder_bits);
        slice db_key_slice = padded_slice(&new_minirun_id, MAX_KEY_SIZE, sizeof(new_minirun_id), db_key_buffer, 0);
        slice db_val_slice = padded_slice(&original_key, MAX_VAL_SIZE, sizeof(original_key), db_val_buffer, 0);
        rc = splinterdb_update(db, db_key_slice, db_val_slice);
        if (rc != 0) { fprintf(stderr, "[Rehash Task] ERROR: Failed re-insert key %lu (new_id %lu) code %d.\n", original_key, new_minirun_id, rc); goto rehash_fail; }
        items_reinserted_db++;
    }
    double p4_end = get_time_usec_rehash_task(); assert(items_reinserted_db == keys_count);
    printf("[Rehash Task] Phase 4 complete. Re-inserted %lu key mappings into SplinterDB in %.3f ms.\n", items_reinserted_db, (p4_end - p4_start) / 1000.0);

    // --- Success Path Cleanup ---
    if (all_original_keys) { free(all_original_keys); all_original_keys = NULL; }

    // --- Phase 5: Signal Completion ---
    fprintf(stdout, "[Rehash Task] Background copy and rebuild successful.\n");
    qfdb->rehash_copy_complete = true;
    splinterdb_deregister_thread(db);
    return NULL;

// --- Failure Path ---
rehash_fail:
    fprintf(stderr, "[Rehash Task] Rehash task failed.\n");
    if (all_original_keys) { free(all_original_keys); all_original_keys = NULL; }
    // Free delete key buffer if allocated
    if (keys_to_delete) {
        for(uint64_t i=0; i<delete_keys_count; ++i) { // Only free up to count
            if (keys_to_delete[i]) free(keys_to_delete[i]);
        }
        free(keys_to_delete);
    }
    if (key_delete_lengths) free(key_delete_lengths);

    if (qfdb && qfdb->qf && qfdb->qf->runtimedata) {
        if (qfdb->qf == qf_old) { qfdb->qf->runtimedata->rehashing = false; fprintf(stderr, "[Rehash Task] Reset rehashing flag on original QF.\n"); }
    }
    if (qfdb && qfdb->qf_new_pending) {
        fprintf(stderr, "[Rehash Task] Cleaning up partially built (failed) new QF %p.\n", (void*)qfdb->qf_new_pending);
        qf_free(qfdb->qf_new_pending); free(qfdb->qf_new_pending); qfdb->qf_new_pending = NULL;
    }
    qfdb->rehash_copy_complete = true; // Signal completion (failure)
    splinterdb_deregister_thread(db);
    return NULL;
}

// --- qfdb_is_rehashing ---
bool qfdb_is_rehashing(QFDB *qfdb) {
    if (qfdb && qfdb->qf && qfdb->qf->runtimedata) {
        // Read the volatile flag directly.
        // Volatile ensures the compiler doesn't optimize away the read.
        return qfdb->qf->runtimedata->rehashing;
    }
    // fprintf(stderr, "WARN: Cannot check rehashing status - invalid QFDB state.\n");
    return false; // Return false if QFDB or internal state is invalid
}


// --- qfdb_finalize_rehash_if_complete ---
void qfdb_finalize_rehash_if_complete(QFDB *qfdb) {
     // Basic checks first without locking
    if (!qfdb || !qfdb->qf || !qfdb->qf->runtimedata || !qfdb->qf->runtimedata->rehashing || !qfdb->rehash_copy_complete) {
        return;
    }

    // Try to acquire the lock non-blockingly
    if (pthread_mutex_trylock(&qfdb->rehash_mutex) == 0) {
         // Double-check conditions after acquiring lock (another thread might have finished it)
        if (qfdb->qf->runtimedata->rehashing && qfdb->rehash_copy_complete && qfdb->qf_new_pending)
        {
            fprintf(stdout, "[QFDB Finalize] Conditions met. Performing swap.\n");

            QF *qf_old = qfdb->qf;
            QF *qf_new = qfdb->qf_new_pending;

            // Perform the swap
            qfdb->qf = qf_new;

            // Reset state for the *new* active QF
            if (qf_new->runtimedata) {
                qf_new->runtimedata->rehashing = false;
                qf_new->runtimedata->nadaptive_slots = 0;
                // Sync the counter after resetting
                pc_sync(&qf_new->runtimedata->pc_nadaptive_slots);
            } else {
                // This should not happen if qf_malloc succeeded
                fprintf(stderr, "[QFDB Finalize] ERROR: New QF %p has no runtime data!\n", (void*)qf_new);
            }

            // Clear pending state in QFDB
            qfdb->qf_new_pending = NULL;
            qfdb->rehash_copy_complete = false;

            // Release the lock *before* freeing the old QF
            pthread_mutex_unlock(&qfdb->rehash_mutex);

            fprintf(stdout, "[QFDB Finalize] Swap complete. Active QF is now %p.\n", (void*)qfdb->qf);

            // Now, safely free the old QF structure and its data
            if (qf_old) {
                 fprintf(stdout, "[QFDB Cleanup] Freeing old QF %p.\n", (void*)qf_old);
                 qf_free(qf_old); // Free internal data allocated by qf_malloc
                 free(qf_old);     // Free the QF struct itself
            }

        } else {
            // Conditions no longer met (e.g., copy not complete yet, or another thread finished)
            pthread_mutex_unlock(&qfdb->rehash_mutex);
        }
    } else {
        // Could not acquire lock, another thread might be finalizing. Do nothing.
    }
}


// --- qfdb_initiate_rehash_if_needed ---
static void qfdb_initiate_rehash_if_needed(QFDB *qfdb) {
    if (!qfdb || !qfdb->qf || !qfdb->qf->runtimedata) {
        return; // Invalid state
    }

    QF *qf_active = qfdb->qf;

    // Sync counter before reading
    pc_sync(&qf_active->runtimedata->pc_nadaptive_slots);
    uint64_t current_adaptive_slots = qf_active->runtimedata->nadaptive_slots;
    uint64_t threshold = qf_active->runtimedata->adaptive_slot_threshold;

    // Check threshold and if rehashing is already in progress (read volatile flag)
    if (current_adaptive_slots >= threshold && !qf_active->runtimedata->rehashing) {

        // Acquire QFDB-level mutex - use trylock to avoid blocking if busy
        if (pthread_mutex_trylock(&qfdb->rehash_mutex) == 0) {

            // Double-check after acquiring the lock to prevent race condition
            if (qf_active->runtimedata->rehashing) {
                pthread_mutex_unlock(&qfdb->rehash_mutex);
                return; // Another thread initiated rehash between check and lock
            }

             // Check threshold again, adaptive slots might have decreased
             pc_sync(&qf_active->runtimedata->pc_nadaptive_slots);
             current_adaptive_slots = qf_active->runtimedata->nadaptive_slots;
             if (current_adaptive_slots < threshold) {
                  pthread_mutex_unlock(&qfdb->rehash_mutex);
                  return; // Threshold no longer met
             }


            fprintf(stdout, "[QFDB] Adaptive slot threshold (%lu/%lu) met. Initiating background rehash.\n",
                    current_adaptive_slots, threshold);

            // Mark as rehashing *under the lock*
            qf_active->runtimedata->rehashing = true;
            qfdb->rehash_copy_complete = false; // Reset copy flag

            // Choose a new seed (simple increment for now)
            qfdb->rehash_seed = qfdb->qf->metadata->seed + 1; // Store the seed to be used

            // Allocate the new QF structure
            QF *qf_new = (QF*)malloc(sizeof(QF));
            if (!qf_new) {
                perror("[QFDB] Failed to allocate QF struct for rehashing");
                qf_active->runtimedata->rehashing = false; // Revert flag
                pthread_mutex_unlock(&qfdb->rehash_mutex);
                return; // Abort
            }
             qf_new->runtimedata = NULL; // Ensure runtimedata is NULL before qf_malloc

            // Allocate internal data for the new QF using the new seed
            bool malloc_ok = qf_malloc(qf_new,
                                       qf_active->metadata->nslots, // Same size for now
                                       qf_active->metadata->key_bits,
                                       qf_active->metadata->value_bits,
                                       qf_active->metadata->hash_mode, // Keep same hash mode
                                       qfdb->rehash_seed);             // Use the new seed

            if (!malloc_ok) {
                fprintf(stderr, "[QFDB] ERROR: qf_malloc failed for qf_new.\n");
                free(qf_new); // Free the allocated struct
                qf_active->runtimedata->rehashing = false; // Revert flag
                pthread_mutex_unlock(&qfdb->rehash_mutex);
                return; // Abort
            }

            // Store pointer to the pending new QF
            qfdb->qf_new_pending = qf_new;

            // Start the background copy task
            pthread_t tid;
            int thread_err = pthread_create(&tid, NULL, qfdb_rehash_copy_task, (void*)qfdb);
            if (thread_err != 0) {
                fprintf(stderr, "[QFDB] ERROR: Failed to create background rehash thread: %s\n", strerror(thread_err));
                // Cleanup the allocated new QF
                qf_free(qfdb->qf_new_pending);
                free(qfdb->qf_new_pending);
                qfdb->qf_new_pending = NULL;
                qf_active->runtimedata->rehashing = false; // Revert flag
                pthread_mutex_unlock(&qfdb->rehash_mutex);
                return; // Abort
            }

            pthread_detach(tid); // Don't wait for the thread to join
            fprintf(stdout, "[QFDB] Background rehash thread started (TID: %lu).\n", (unsigned long)tid);

            // Release the QFDB mutex
            pthread_mutex_unlock(&qfdb->rehash_mutex);

        } else {
            // Could not acquire QFDB lock, another thread is likely initiating. Do nothing.
        }
    }
}
// --- qfdb_insert ---
int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count) {
    if (!qfdb || !qfdb->qf || !qfdb->ext_store) {
        fprintf(stderr, "ERROR: qfdb_insert called with invalid QFDB structure.\n");
        return -1; // Indicate invalid arguments
    }

    qf_insert_result qf_res;
    int qf_ret = qf_insert_using_ll_table(qfdb->qf, key, count, &qf_res, QF_NO_LOCK);

    if (qf_ret < 0) {
        if (qf_ret == QF_NO_SPACE) {
            qfdb->space_errors++;
        } else {
            fprintf(stderr, "ERROR: qf_insert_using_ll_table failed for key %lu with code %d\n", key, qf_ret);
        }
        return qf_ret;
    }

    // Prepare for SplinterDB update
    uint64_t db_key = qf_res.minirun_id;
    uint64_t db_val = key; // The original key is the "message" to merge

    // Prepare slices using buffers for padding
    char db_key_buffer[MAX_KEY_SIZE];
    char db_val_buffer[MAX_VAL_SIZE];
    slice db_key_slice = padded_slice(&db_key, MAX_KEY_SIZE, sizeof(db_key), db_key_buffer, 0);
    slice db_val_slice = padded_slice(&db_val, MAX_VAL_SIZE, sizeof(db_val), db_val_buffer, 0);

    // *** CALL splinterdb_update DIRECTLY ***
    int db_ret = splinterdb_update(qfdb->ext_store, db_key_slice, db_val_slice);

    if (db_ret != 0) {
        fprintf(stderr, "ERROR: SplinterDB update failed for minirun_id %lu (orig key %lu) with code %d\n",
                db_key, key, db_ret);
         if (db_ret > 0) { // Positive values often correspond to errno
             fprintf(stderr, "  System error: %s (%d)\n", strerror(db_ret), db_ret);
         }
        return db_ret;
    }

    // Check/trigger rehash logic
    qfdb_initiate_rehash_if_needed(qfdb);
    qfdb_finalize_rehash_if_complete(qfdb);

    return 0; // Success
}

// --- qf_query_only ---
int qf_query_only(QFDB *qfdb, uint64_t key){
    if (!qfdb || !qfdb->qf) {
        fprintf(stderr, "ERROR: qf_query_only called with invalid QFDB structure.\n");
        return -1; // Indicate invalid arguments or error
    }

    QF *qf_to_query = qfdb->qf;
    // Simplification: Query only the active QF, even during rehash.
    // A full implementation might query qf_new_pending if active fails.

    uint64_t query_hash_result; // To store the full hash details if needed
    int minirun_rank;

    // Use QF_NO_LOCK assuming single-threaded test or external locking
    minirun_rank = qf_query_using_ll_table(qf_to_query, key, &query_hash_result, QF_NO_LOCK);

    // Return 1 if found, 0 otherwise (consistent with test expectation)
    return (minirun_rank >= 0) ? 1 : 0;
}

// --- qfdb_query ---
int qfdb_query(QFDB *qfdb, uint64_t key) {
    if (!qfdb || !qfdb->qf || !qfdb->ext_store) {
        fprintf(stderr, "ERROR: qfdb_query called with invalid QFDB structure.\n");
        return -2; // Error code for invalid setup
    }

    qfdb->total_queries++; // Increment total query counter

    QF *qf_active = qfdb->qf;
    uint64_t query_hash_result;
    int minirun_rank;

    // 1. Query the Active QF Filter
    minirun_rank = qf_query_using_ll_table(qf_active, key, &query_hash_result, QF_NO_LOCK);

    if (minirun_rank < 0) {
        // Not found in the QF filter, definitely absent.
        return 0;
    }

    // 2. Found in QF - Potential Positive. Verify against SplinterDB.
    qfdb->verified_queries++; // Increment verification counter

    uint64_t db_key = query_hash_result & BITMASK(qf_active->metadata->quotient_remainder_bits);

    splinterdb_lookup_result db_lookup_res;
    splinterdb_lookup_result_init(qfdb->ext_store, &db_lookup_res, 0, NULL);
    char key_buffer[MAX_KEY_SIZE]; // Use local buffer for padding
    slice key_slice = padded_slice(&db_key, MAX_KEY_SIZE, sizeof(db_key), key_buffer, 0);

    int db_ret = splinterdb_lookup(qfdb->ext_store, key_slice, &db_lookup_res);

    if (db_ret != 0) {
        fprintf(stderr, "ERROR: SplinterDB lookup failed for minirun_id %lu (query key %lu) with code %d\n",
                db_key, key, db_ret);
        splinterdb_lookup_result_deinit(&db_lookup_res);
        qfdb_finalize_rehash_if_complete(qfdb);
        return -2; // Error during DB lookup
    }

    bool verified = false;
    uint64_t first_orig_key_in_slot = 0; // Store the first key found in DB for adaptation
    bool db_entry_found = splinterdb_lookup_found(&db_lookup_res);

    if (db_entry_found) {
        slice value_slice;
        splinterdb_lookup_result_value(&db_lookup_res, &value_slice);
        char *value_data = slice_data(value_slice);
        size_t value_len = slice_length(value_slice);

        if (value_len == 0) {
             // DB entry exists but has empty value - inconsistency? Treat as not verified.
             fprintf(stderr, "WARN: Found minirun_id %lu in SplinterDB but value is empty.\n", db_key);
        } else if (value_len % MAX_VAL_SIZE != 0) {
             fprintf(stderr, "WARN: Corrupted value length %zu for minirun_id %lu in SplinterDB.\n", value_len, db_key);
             // Treat as verification failed
        } else {
            size_t num_keys_in_entry = value_len / MAX_VAL_SIZE;
            if (num_keys_in_entry > 0) {
                 // Store the first key found in the DB entry for potential adaptation
                 memcpy(&first_orig_key_in_slot, value_data, sizeof(uint64_t));
                 if (first_orig_key_in_slot == 0) {
                     // If the stored key is somehow 0, adaptation might behave strangely.
                     // Log a warning maybe, but proceed.
                     // fprintf(stderr, "WARN: Stored original key is 0 for minirun_id %lu\n", db_key);
                 }
            }
             // Check if our queried 'key' exists in this list.
            for (size_t i = 0; i < num_keys_in_entry; ++i) {
                uint64_t stored_key;
                memcpy(&stored_key, value_data + i * MAX_VAL_SIZE, sizeof(uint64_t));
                if (stored_key == key) { // 'key' is the key passed to qfdb_query
                    verified = true;
                    break; // Found the key in the list
                }
            }
        }
    }
    // Deinit lookup result object regardless of found status
    splinterdb_lookup_result_deinit(&db_lookup_res);


    if (verified) {
        // Found in QF and verified in SplinterDB
        qfdb_finalize_rehash_if_complete(qfdb);
        return 1; // Definitely present
    } else {
        // NOT VERIFIED. If QF hit, this is a False Positive (or DB inconsistency).
        qfdb->adaptations_performed++; // Count this as an FP/adaptation trigger

        // Attempt Adaptation only if we found a valid DB entry for the minirun
        // and retrieved a valid original key to adapt against.
        if (db_entry_found && first_orig_key_in_slot != 0) {
            // Check if adaptation is feasible (avoid adapting against key 0 or itself)
            if (first_orig_key_in_slot != key && key != 0) {
                 int adapt_ret = qf_adapt_using_ll_table(qf_active,
                                                         first_orig_key_in_slot, // The key that *should* be there
                                                         key,                    // The key that caused the FP
                                                         minirun_rank,           // The rank within the run where FP occurred
                                                         QF_NO_LOCK);            // Flags (use original keys)

                 if (adapt_ret < 0) {
                     // Only log space errors as warnings, other errors might be more serious
                     if (adapt_ret == QF_NO_SPACE) {
                          // Don't spam logs for expected space errors during high load/adapt
                          qfdb->space_errors++;
                     } else {
                         fprintf(stderr, "WARN: qf_adapt_using_ll_table failed for orig %lu / fp %lu with code %d\n",
                                 first_orig_key_in_slot, key, adapt_ret);
                     }
                 }
                 // No need to print verbose messages inside library func
            }
        } else if(minirun_rank >= 0) {
             // QF hit, but no corresponding entry/key found in DB. This indicates an inconsistency.
             fprintf(stderr, "WARN: QF positive for key %lu (minirun_id %lu, rank %d), but no valid DB entry found for verification/adaptation.\n",
                     key, db_key, minirun_rank);
             // Cannot adapt in this case.
        }

        // Check if rehash needs to be initiated *after* potential adaptation attempt
        qfdb_initiate_rehash_if_needed(qfdb);

        // Check if a pending rehash can be finalized *after* potential adaptation attempt
        qfdb_finalize_rehash_if_complete(qfdb);

        return -1; // Indicate False Positive / Verification Failed
    }
}


// --- qfdb_remove ---
// Removes a key from the QF. SplinterDB removal is NOT implemented.
int qfdb_remove(QFDB *qfdb, uint64_t key) {
     if (!qfdb || !qfdb->qf || !qfdb->ext_store) {
        fprintf(stderr, "ERROR: qfdb_remove called with invalid QFDB structure.\n");
        return -2; // Error code for invalid setup
    }

     // Removing during rehash adds significant complexity. Ignoring for now.
     if (qfdb_is_rehashing(qfdb)) {
         fprintf(stderr, "WARN: qfdb_remove called during rehash. Operation might be inconsistent.\n");
         // Proceed with removing from active QF, but DB state will diverge.
     }

     // 1. Remove from QF Filter
     uint64_t removed_hash = 0; // Placeholder, not used further here
     int removed_hash_len = 0; // Placeholder
     int qf_ret = qf_remove(qfdb->qf, key, &removed_hash, &removed_hash_len, QF_NO_LOCK);

     if (qf_ret < 0) {
         fprintf(stderr, "ERROR: qf_remove failed for key %lu with code %d\n", key, qf_ret);
         return qf_ret; // Return QF error code
     } else if (qf_ret == 0) {
         // Key was not found in the QF to begin with.
         return 0; // Indicate key wasn't present
     }

     // 2. SplinterDB Removal (Skipped)
     // Implementing the read-modify-write logic is complex.
     // fprintf(stdout, "INFO: Key %lu removed from QF (freed %d slots). SplinterDB removal skipped.\n", key, qf_ret);


     // Check for rehash finalization after removal
     qfdb_finalize_rehash_if_complete(qfdb);

     return qf_ret; // Return the number of slots freed by QF remove
}

// --- qfdb_resize ---
// Note: This uses the internal QF resize, which doesn't handle SplinterDB.
// Rehashing mechanism is preferred over this for QFDB.
int qfdb_resize(QFDB *qfdb, uint64_t new_qbits) {
    if (!qfdb || !qfdb->qf || !qfdb->qf->runtimedata) return -1;
    fprintf(stderr, "WARN: qfdb_resize called. This only resizes the QF, not SplinterDB. Rehashing is preferred.\n");
    // The container_resize function pointer needs to be set correctly during init if this is intended.
    // Assuming it points to qf_resize_malloc or similar.
    if (!qfdb->qf->runtimedata->container_resize) {
         fprintf(stderr, "ERROR: QF container_resize function not set.\n");
         return -1;
    }
    return qfdb->qf->runtimedata->container_resize(qfdb->qf, 1ULL << new_qbits);
}

// --- qfdb_get_stats ---
void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries,
                   uint64_t *fp_rehashes, uint64_t *adaptations_performed, uint64_t *space_errors, double *false_positive_rate) {
    if (!qfdb) return;
    // Use fp_rehashes for FPs that triggered rehash attempt (not implemented directly)
    // Use adaptations_performed for total FPs encountered
    if (total_queries) *total_queries = qfdb->total_queries;
    if (verified_queries) *verified_queries = qfdb->verified_queries;
    if (fp_rehashes) *fp_rehashes = qfdb->fp_rehashes; // Currently unused, use adaptations_performed
    if (adaptations_performed) *adaptations_performed = qfdb->adaptations_performed;
    if (space_errors) *space_errors = qfdb->space_errors;

    if (false_positive_rate) {
        // Calculate FPR based on FPs encountered during verified queries
        if (qfdb->verified_queries > 0) {
            // adaptations_performed counts the FPs found during verification
            *false_positive_rate = (double)qfdb->adaptations_performed / qfdb->verified_queries;
        } else {
            *false_positive_rate = 0.0;
        }
    }
}

// --- qfdb_set_rehash_threshold ---
void qfdb_set_rehash_threshold(QFDB *qfdb, uint64_t threshold) {
    if (qfdb && qfdb->qf && qfdb->qf->runtimedata) {
        qfdb->qf->runtimedata->adaptive_slot_threshold = threshold;
        printf("[QFDB] Set adaptive slot threshold to %lu\n", threshold);
    } else {
         fprintf(stderr, "WARN: Cannot set rehash threshold - invalid QFDB state.\n");
    }
}

// --- qfdb_get_adaptive_slots ---
uint64_t qfdb_get_adaptive_slots(QFDB *qfdb) {
    if (qfdb && qfdb->qf && qfdb->qf->runtimedata) {
        // Sync before reading for accuracy across threads
        pc_sync(&qfdb->qf->runtimedata->pc_nadaptive_slots);
        return qfdb->qf->runtimedata->nadaptive_slots;
    }
    // fprintf(stderr, "WARN: Cannot get adaptive slots - invalid QFDB state.\n");
    return 0; // Return 0 if QFDB or internal state is invalid
}



// --- qfdb_destroy ---
void qfdb_destroy(QFDB *qfdb) {
    if (!qfdb) return;

    // Ensure mutex is handled cleanly if destroyed mid-rehash
    // This is tricky; ideally, ensure rehash completes or is cancelled first.
    // Forcing unlock might be unsafe if the thread is still running.
    // pthread_mutex_destroy(&qfdb->rehash_mutex); // Destroy mutex

    // Close SplinterDB first
    if (qfdb->ext_store) {
        splinterdb *tmp = qfdb->ext_store;
        qfdb->ext_store = NULL; // Nullify before close to prevent double close
        fprintf(stdout, "[QFDB Destroy] Closing SplinterDB...\n");
        splinterdb_close(&tmp);
        fprintf(stdout, "[QFDB Destroy] SplinterDB closed.\n");
    }

    // Clean up pending QF if rehash was interrupted
    if (qfdb->qf_new_pending){
        fprintf(stdout, "[QFDB Destroy] Freeing pending new QF %p...\n", (void*)qfdb->qf_new_pending);
        qf_free(qfdb->qf_new_pending); // Free internal data
        free(qfdb->qf_new_pending);    // Free struct
        qfdb->qf_new_pending = NULL;
    }

    // Free the currently active QF
    if (qfdb->qf) {
        fprintf(stdout, "[QFDB Destroy] Freeing active QF %p...\n", (void*)qfdb->qf);
        qf_free(qfdb->qf); // Free internal data
        free(qfdb->qf);    // Free struct
        qfdb->qf = NULL;
    }

    // Free the data config
    if (qfdb->data_cfg) {
        free(qfdb->data_cfg);
        qfdb->data_cfg = NULL;
    }

    // Finally, free the QFDB struct itself
    free(qfdb);
}