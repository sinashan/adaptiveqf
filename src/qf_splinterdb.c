#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include "hashutil.h"

#include "qf_splinterdb.h"
#include "gqf_int.h"

#define HEAP_IMPLEMENTATION
#include "sc_min_heap.h"

#define DEBUG_MODE 0
#define BROOM_FILTER_REHASH_CNT 1000
#define HEAP_SIZE_INIT 100000
#define MAX_MINIRUN_KEY(result_array, length, max_key_out)      \
    do {                                                        \
        if ((length) > 0) {                                     \
            (max_key_out) = (result_array)[0];             \
            for (int _i = 1; _i < (length); _i++) {             \
                if ((result_array)[_i] > (max_key_out)) {  \
                    (max_key_out) = (result_array)[_i];    \
                }                                               \
            }                                                   \
        }                                                       \
    } while (0)

#define MAX(a, b) ({            \
    __typeof__(a) _a = (a);     \
    __typeof__(b) _b = (b);     \
    _a > _b ? _a : _b;          \
})

#define DEBUG_PRINT(fmt, ...) \
do { if (DEBUG_MODE) fprintf(stderr, "DEBUG: %s:%d:%s(): " fmt, __FILE__, \
                             __LINE__, __func__, ##__VA_ARGS__); } while (0)

int broom_cnt = 0;

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

// Hash function for QFDB
static inline uint64_t qfdb_hash(QFDB *qfdb, const void *key, size_t key_size) {
  if (!qfdb || !qfdb->qf || !qfdb->qf->metadata) return 0;

  quotient_filter_metadata *m = qfdb->qf->metadata;

  // Check if we need to use alternate hash function for frontier
  if (m->frontier != 0 && *(uint64_t*)key <= (uint64_t)m->frontier) {
    return MurmurHash64A(key, key_size, m->seed_b);
  }

  return MurmurHash64A(key, key_size, m->seed);
}

// Initialize the combined QF+SplinterDB structure
QFDB* qfdb_init(uint64_t qbits, uint64_t rbits, const char* db_path) {
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

  // Initialize hashmap to NULL (empty)
  qfdb->hashmap = NULL;

  // Initialize counters
  qfdb->fp_rehashes = 0;
  qfdb->total_queries = 0;
  qfdb->verified_queries = 0;
  qfdb->adaptations_performed = 0;
  qfdb->space_errors = 0;

  qfdb->heap = malloc(sizeof(struct sc_heap));
  qfdb->heap_copy = malloc(sizeof(struct sc_heap));
  if (!sc_heap_init(qfdb->heap, HEAP_SIZE_INIT)) {
    printf("ERROR: could not initalize heap");
  }

  return qfdb;
}

// Free resources used by the combined structure
void qfdb_destroy(QFDB *qfdb) {
  if (!qfdb) return;

  // Free the hashmap entries
  minirun_entry *current, *tmp;
  HASH_ITER(hh, qfdb->hashmap, current, tmp) {
    HASH_DEL(qfdb->hashmap, current);
    free(current);
  }
  qfdb->hashmap = NULL;

  // Close and free the SplinterDB
  if (qfdb->ext_store) {
    splinterdb *tmp = qfdb->ext_store;
    qfdb->ext_store = NULL;
    splinterdb_close(&tmp);
  }

  // Free the QF
  if (qfdb->qf) {
    qf_free(qfdb->qf);
    free(qfdb->qf);
    qfdb->qf = NULL;
  }

  // Free the data_config
  if (qfdb->data_cfg) {
    free(qfdb->data_cfg);
    qfdb->data_cfg = NULL;
  }

  sc_heap_term(qfdb->heap);

  // Free the QFDB structure itself
  free(qfdb);
}

int qfdb_get_occ_slots(QFDB *qfdb) {
  return qf_get_num_occupied_slots(qfdb->qf);
}

// Insert an item into QFDB
int qfdb_insert(QFDB *qfdb, uint64_t key, uint64_t count) {
  if (!qfdb || !qfdb->qf) {
    return -1; // Invalid parameters
  }

  qfdb->max_key = MAX(key, qfdb->max_key);

  // Create a copy of the key for hashing
  uint64_t key_copy = key;

  // Hash the key
  uint64_t hash = qfdb_hash(qfdb, &key_copy, sizeof(key_copy));
  DEBUG_PRINT("Inserting key: %lu, hash: %lu\n", key, hash);

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
    new_entry->next = NULL;

    // Check if this minirun_id already exists in hashmap
    minirun_entry *existing_entry = NULL;
    HASH_FIND(hh, qfdb->hashmap, &result.minirun_id, sizeof(uint64_t), existing_entry);

    if (existing_entry) {
      // If it exists, update it in the linked list.
      minirun_entry* tmp = existing_entry->next;
      while(tmp != NULL) tmp = tmp->next;
      tmp = new_entry;
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

// Returns -1 if error, 0 if not found, 1 if found and minirun_rank if collision.
int _qfdb_query(QFDB *qfdb, uint64_t key, minirun_entry* original_entry) {
  if (!qfdb || !qfdb->qf) {
    return -1; // Invalid parameters
  }

  // Increment total queries counter
  qfdb->total_queries++;

  // Create a copy of the key for hashing
  uint64_t key_copy = key;

  // Compute hash
  uint64_t hash = qfdb_hash(qfdb, &key_copy, sizeof(key_copy));

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

  DEBUG_PRINT("Querying key: %lu, hash: %lu\n", key, hash);

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
  uint64_t q_bits = qfdb->qf->metadata->quotient_bits;
  uint64_t r_bits = qfdb->qf->metadata->bits_per_slot;

  // Calculate bitmask safely
  uint64_t mask = (q_bits + r_bits >= 64) ?
    0xFFFFFFFFFFFFFFFF : ((1ULL << (q_bits + r_bits)) - 1);

  uint64_t minirun_id = ret_hash & mask;
  DEBUG_PRINT("Potential match found in QF, minirun_id=%lu\n", minirun_id);

  minirun_entry* entry = NULL;
  // Use uthash to find the entry
  HASH_FIND(hh, qfdb->hashmap, &minirun_id, sizeof(uint64_t), entry);

  if (!entry) {
    DEBUG_PRINT("Minirun ID not found in hashmap (hash collision) %d\n", minirun_id);
    qfdb->fp_rehashes++;
    return 0; // Not found
  }

  for(minirun_entry* tmp = entry; tmp != NULL ; tmp = tmp->next) {
    if (tmp->original_key == key) {
      // memset(original_entry, 0, sizeof(minirun_entry));
      DEBUG_PRINT("True positive found\n");
      return 1; // Found
    }
  }

  memcpy(original_entry, entry, sizeof(minirun_entry));
  qfdb->fp_rehashes++;
  return minirun_rank; // False positive

}

void find_x_smallest_entries_greater_than_z(QFDB* qfdb, int x, uint64_t z,
                                            uint64_t *result, int
                                            *result_len) {
    int count = 0;
    while (count < x) {
        struct sc_heap_data *top = sc_heap_pop(qfdb->heap);
        if (!top) break;
        result[count++] = top->key;
    }

    *result_len = count;
}

/*WARNING: This function assumes the heap and filter have the same data.
 * Make sure to initalize the heap with the correct data if you are using this.
 * */
void broom(QFDB *qfdb) {

  // uint64_t before = qf_get_num_occupied_slots(qfdb->qf);
  int count = 0;
  quotient_filter_metadata *m = qfdb->qf->metadata;
  uint64_t* rehash_candidates[BROOM_FILTER_REHASH_CNT];
  find_x_smallest_entries_greater_than_z(qfdb, BROOM_FILTER_REHASH_CNT,
                                      m->frontier,
                                      rehash_candidates, &count);

  for(int i = 0; i < count; i++) {
    qfdb_remove(qfdb, rehash_candidates[i]);
  }

  uint64_t max_key = 0;
  MAX_MINIRUN_KEY(rehash_candidates, count, max_key);

  if (max_key >= qfdb->max_key) {
    m->seed = m->seed_b;
    m->seed_b++;
    m->frontier = 0;
    sc_heap_copy(qfdb->heap, qfdb->heap_copy);
  } else {
    m->frontier = max_key;
  }

  for(int i = 0; i < count; i++) {
    qfdb_insert(qfdb, rehash_candidates[i], 1);
  }

  // uint64_t diff = before - qf_get_num_occupied_slots(qfdb->qf);
  // if (diff > 0) {
  //   printf("WE cleared %ld slots\n", diff);
  // }

  broom_cnt += count;

}

void process_adapt_result(QFDB *qfdb, int adapt_ret) {

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

}

int _qfdb_adapt(QFDB *qfdb, uint64_t key, minirun_entry* original_entry, int minirun_rank) {

  // Attempt adaptation
  DEBUG_PRINT("Attempting adaptation\n");


  broom(qfdb);

  int adapt_ret = qf_adapt_using_ll_table(qfdb->qf,
                                          original_entry->original_key, // key that should be present
                                          key,                // key that caused false positive
                                          minirun_rank,       // rank of the item
                                          QF_KEY_IS_HASH);                 // flags

  // if (adapt_ret == -1) {
  //   broom(qfdb);
  //   adapt_ret = qf_adapt_using_ll_table(qfdb->qf,
  //                                         original_entry->original_key, // key that should be present
  //                                         key,                // key that caused false positive
  //                                         minirun_rank,       // rank of the item
  //                                         QF_KEY_IS_HASH);
  // }

  process_adapt_result(qfdb, adapt_ret);

  return 0;
}


// Query for an item in QFDB
int qfdb_query(QFDB *qfdb, uint64_t key) {
  minirun_entry *original_entry = malloc(sizeof(minirun_entry));
  memset(original_entry, 0, sizeof(minirun_entry));
  int minirun_rank = _qfdb_query(qfdb, key, original_entry);
  if (original_entry->original_key != 0) {
   _qfdb_adapt(qfdb, key, original_entry, minirun_rank);
    free(original_entry);
    return 1; // A false positive was found so technically the query was successful.
  }
  free(original_entry);
  return minirun_rank;
}

// Remove an item from QFDB
int qfdb_remove(QFDB *qfdb, uint64_t key) {
  if (!qfdb || !qfdb->qf) {
    return -1;
  }

  // Hash the key
  uint64_t hash = qfdb_hash(qfdb, &key, sizeof(key));
  uint64_t ret_hash;
  int ret_hash_len;

  // Remove from QF
  int ret = qf_remove(qfdb->qf, hash, &ret_hash, &ret_hash_len, QF_KEY_IS_HASH);

  if (ret > 0) {
    // Calculate minirun_id
    uint64_t minirun_id = ret_hash & BITMASK(qfdb->qf->metadata->quotient_bits +
                                             qfdb->qf->metadata->bits_per_slot);

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

  // Store the current hashmap
  minirun_entry *old_hashmap = qfdb->hashmap;
  qfdb->hashmap = NULL;

  // Resize the QF
  int ret = qfdb->qf->runtimedata->container_resize(qfdb->qf, 1ULL << new_qbits);

  if (ret < 0) {
    // Restore the hashmap on failure
    qfdb->hashmap = old_hashmap;
    return ret;
  }

  // Free the old hashmap since resize was successful
  // (minirun_ids will change after resize)
  minirun_entry *current, *tmp;
  HASH_ITER(hh, old_hashmap, current, tmp) {
    HASH_DEL(old_hashmap, current);
    free(current);
  }

  return ret;
}

// Get statistics about the QFDB
void qfdb_get_stats(QFDB *qfdb, uint64_t *total_queries, uint64_t *verified_queries,
                    uint64_t *fp_rehashes, uint64_t *adaptations_performed,
                    uint64_t *space_errors, double *false_positive_rate) {
  if (!qfdb) return;

  printf("BROOM_CNT: %d", broom_cnt);
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

// Rehash items in a high false positive bucket
int qfdb_rehash_bucket(QFDB *qfdb, uint64_t bucket_idx) {
  if (!qfdb || !qfdb->qf) {
    return -1;
  }

  // Implementation depends on specific requirements
  // This is a placeholder
  return 0;
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
