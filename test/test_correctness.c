#include <stdio.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <openssl/rand.h>

#include "include/hashutil.h"
#include "include/rand_util.h"
#include "include/splinter_util.h"
#include "include/test_driver.h"

int main(int argc, char **argv)
{
	int seeded = 0;
    long user_seed = 0; // Store the seed if provided

	if (argc < 4) {
		fprintf(stderr, "Usage: %s <qbits> <rbits> <num_keys> [seed]\n", argv[0]);
        fprintf(stderr, "  qbits: log2(number of slots)\n");
        fprintf(stderr, "  rbits: number of remainder bits\n");
        fprintf(stderr, "  num_keys: number of keys to insert/query\n");
        fprintf(stderr, "  seed: optional random seed (integer)\n");
		exit(1);
	}
	if (argc > 4) { // optional seed
        user_seed = strtol(argv[4], NULL, 10);
		srand(user_seed);
		printf("Running test with provided seed: %ld\n", user_seed);
		seeded = 1;
	}
	else {
		time_t seed = time(NULL);
        user_seed = (long)seed; // Cast for srand
		srand(user_seed);
		printf("Running test with generated seed: %ld\n", user_seed);
	}
	size_t qbits = atoi(argv[1]);
	size_t rbits = atoi(argv[2]);

	// Use the same number for inserts and queries in this micro_test
	size_t num_keys = strtoull(argv[3], NULL, 10);
    size_t num_inserts = num_keys;
    size_t num_queries = num_keys; // Will actually query num_inserts_to_attempt times

    printf("Parameters: q=%zu, r=%zu, num_keys=%zu\n", qbits, rbits, num_keys);
    size_t num_slots = 1ULL << qbits;
   // printf("Filter size: %zu slots (%.2f MB)\n", num_slots, qf_estimate_size(qbits, rbits) / (1024.0 * 1024.0));


	test_results_t ret;

    // Generate insert set (these will be treated as hashes)
	uint64_t *insert_set = malloc(num_inserts * sizeof(uint64_t));
    if (!insert_set) { perror("Failed to allocate insert_set"); exit(EXIT_FAILURE); }

	printf("Generating %zu insert keys...\n", num_inserts);
	if (seeded) {
        for (size_t i = 0; i < num_inserts; i++) insert_set[i] = rand_uniform((uint64_t)-1); // Use custom random if needed
    }
	else {
        if (RAND_bytes((unsigned char*)insert_set, num_inserts * sizeof(uint64_t)) != 1) {
            fprintf(stderr, "Error generating random bytes with OpenSSL.\n");
            free(insert_set);
            exit(EXIT_FAILURE);
        }
    }
    printf("Insert keys generated.\n");

	// Note: query_set is not used by the corrected run_micro_test, so we don't need to generate it.
    // uint64_t *query_set = NULL; // Or keep generation if other tests use it

	ret = run_correctness_test(qbits, rbits, insert_set, num_inserts, NULL /*query_set*/, 0 /*num_queries*/, 1 /* verbose */);

	if (ret.exit_code != 0) {
		printf("\nTest returned with error code: %d\n", ret.exit_code);
	} else {
        printf("\nTest completed.\n");
    }

    free(insert_set);
    // free(query_set) if it was allocated

	return ret.exit_code;
}

