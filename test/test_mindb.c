#include<stdio.h>
#include<string.h>
#include<stdlib.h>

#include<mini_transaction_engine.h>
#include<callbacks_tupleindexer.h>
#include<test_tuple_infos.h>

#include<bplus_tree.h>
#include<hash_table.h>

mini_transaction_engine mte;

#define SYSTEM_PAGE_SIZE 1024
#define PAGE_ID_WIDTH 4
#define LSN_WIDTH 4

#define BUFFERPOOL_BUFFERS 100
#define WALE_BUFFERS 30

#define LATCH_WAIT_TIMEOUT_US     100 // 100 microseconds
#define LOCK_WAIT_TIMEOUT_US   500000 // 0.5 second
#define CHECKPOINT_PERIOD_US (20 * 1000000) // 20 seconds
#define CHECKPOINT_PERIOD_SIZE (1000000) // 1 MB
#define MAX_WAL_FILE_SIZE (2 * 1000000) // 2 MB

#define FLUSH_ON_COMPLETION 1

const char* db_filename = "test.db";

uint64_t root_page_id;

#define JOBS_COUNT 100000
uint64_t input[JOBS_COUNT];

// tests for bplus tree

bplus_tree_tuple_defs bpttd;

void create_uint_bplus_tree(mini_transaction* mt)
{
	int abort_error = 0;
	root_page_id = get_new_bplus_tree(&bpttd, &pam, &pmm, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while creating\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}
}

int insert_uint_bplus_tree(mini_transaction* mt, uint64_t x, char* value)
{
	int abort_error = 0;

	char record[BUFFER_SIZE];
	construct_record(record, x, 0, value);
	int res = insert_in_bplus_tree(root_page_id, record, &bpttd, &pam, &pmm, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while inserting\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}

	return res;
}

int delete_uint_bplus_tree(mini_transaction* mt, uint64_t x)
{
	int abort_error = 0;

	char key[BUFFER_SIZE];
	construct_key(key, x, 0);
	int res = delete_from_bplus_tree(root_page_id, key, &bpttd, &pam, &pmm, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while deleting\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}

	return res;
}

void print_uint_bplus_tree(mini_transaction* mt)
{
	int abort_error = 0;
	print_bplus_tree(root_page_id, 1, &bpttd, &pam, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while printing\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}
}

void destroy_uint_bplus_tree(mini_transaction* mt)
{
	int abort_error = 0;
	destroy_bplus_tree(root_page_id, &bpttd, &pam, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while destroying\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}
}

int main1()
{
	initialize_tuple_defs();
	if(!init_bplus_tree_tuple_definitions(&bpttd, &(pam.pas), &record_def, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT))
	{
		printf("failed to initialize bplus tree tuple definitions\n");
		exit(-1);
	}

	for(uint32_t i = 0; i < JOBS_COUNT; i++)
		input[i] = i;

	for(uint32_t i = 0; i < JOBS_COUNT; i++)
	{
		uint32_t i1 = (((uint32_t)rand()) % (JOBS_COUNT));
		uint32_t i2 = (((uint32_t)rand()) % (JOBS_COUNT));
		memory_swap(input + i1, input + i2, sizeof(input[i1]));
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_bplus_tree(mt);

		print_uint_bplus_tree(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "creation_done", strlen("creation_done"));
		printf("completed creation at "); print_uint256(cLSN); printf("\n");
	}

	{
		uint256 uLSN = append_user_info_log_record_for_mini_transaction_engine(&mte, 1, "started insertions", strlen("started insertions"));
		printf("user log at "); print_uint256(uLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint32_t i = 0; i < JOBS_COUNT; i++)
		{
			insert_uint_bplus_tree(mt, input[i], NULL);

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_bplus_tree(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "insertions_done", strlen("insertions_done"));
		printf("completed insertions at "); print_uint256(cLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "printing_done", strlen("printing_done"));
		printf("completed printing at "); print_uint256(cLSN); printf("\n");
	}

	{
		uint256 uLSN = append_user_info_log_record_for_mini_transaction_engine(&mte, 1, "started deletions", strlen("started deletions"));
		printf("user log at "); print_uint256(uLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 0; i < JOBS_COUNT; i++)
		{
			delete_uint_bplus_tree(mt, input[i]);

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_bplus_tree(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "deletions_done", strlen("deletions_done"));
		printf("completed deletions at "); print_uint256(cLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "printing_done", strlen("printing_done"));
		printf("completed printing at "); print_uint256(cLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		destroy_uint_bplus_tree(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "destruction_done", strlen("destruction_done"));
		printf("completed destruction at "); print_uint256(cLSN); printf("\n");
	}

	/*{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "printing_done", strlen("printing_done"));
		printf("completed printing at "); print_uint256(cLSN); printf("\n");
	}*/

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_bplus_tree(mt);

		print_uint_bplus_tree(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "creation_done", strlen("creation_done"));
		printf("completed creation at "); print_uint256(cLSN); printf("\n");
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

	deinitialize_tuple_defs();
	return 0;
}

hash_table_tuple_defs httd;

void create_uint_hash_table(mini_transaction* mt, uint64_t bucket_count)
{
	int abort_error = 0;
	root_page_id = get_new_hash_table(bucket_count, &httd, &pam, &pmm, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while creating\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}
}

int insert_uint_hash_table(mini_transaction* mt, uint64_t x, char* value, int allow_vaccum)
{
	int abort_error = 0;

	char key[BUFFER_SIZE];
	construct_key(key, x, 0);

	char record[BUFFER_SIZE];
	construct_record(record, x, 0, value);

	hash_table_iterator* hti = get_new_hash_table_iterator(root_page_id, WHOLE_BUCKET_RANGE, key, &httd, &pam, &pmm, mt, &abort_error);
	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while inserting\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}

	int res = 0;
	int found = 0;

	if(!is_curr_bucket_empty_for_hash_table_iterator(hti))
	{
		const void* curr = get_tuple_hash_table_iterator(hti);
		while(1)
		{
			if(curr != NULL) // i.e. key matches
			{
				found = 1;
				break;
			}
			else
			{
				int next_res = next_hash_table_iterator(hti, 0, mt, &abort_error);
				if(is_aborted_for_mini_tx(&mte, mt))
				{
					printf("aborted %d while going next for inserting\n", get_abort_error_for_mini_tx(&mte, mt));
					exit(-1);
				}

				if(next_res == 0)
					break;
			}

			curr = get_tuple_hash_table_iterator(hti);
		}
	}

	// insert only if not found
	if(!found)
	{
		res = insert_in_hash_table_iterator(hti, record, mt, &abort_error);
		if(is_aborted_for_mini_tx(&mte, mt))
		{
			printf("aborted %d while inserting\n", get_abort_error_for_mini_tx(&mte, mt));
			exit(-1);
		}
	}

	hash_table_vaccum_params htvp;
	delete_hash_table_iterator(hti, &htvp, mt, &abort_error);
	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while inserting\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}

	if(allow_vaccum)
	{
		perform_vaccum_hash_table(root_page_id, &htvp, 1, &httd, &pam, &pmm, mt, &abort_error);
		if(is_aborted_for_mini_tx(&mte, mt))
		{
			printf("aborted %d while vaccumming after insert\n", get_abort_error_for_mini_tx(&mte, mt));
			exit(-1);
		}
	}

	return res;
}

int delete_uint_hash_table(mini_transaction* mt, uint64_t x, int allow_vaccum)
{
	int abort_error = 0;

	char key[BUFFER_SIZE];
	construct_key(key, x, 0);

	hash_table_iterator* hti = get_new_hash_table_iterator(root_page_id, WHOLE_BUCKET_RANGE, key, &httd, &pam, &pmm, mt, &abort_error);
	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while deleting\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}

	int res = 0;

	if(!is_curr_bucket_empty_for_hash_table_iterator(hti))
	{
		const void* curr = get_tuple_hash_table_iterator(hti);
		while(1)
		{
			if(curr != NULL) // i.e. key matches
			{
				res += remove_from_hash_table_iterator(hti, mt, &abort_error);
				if(is_aborted_for_mini_tx(&mte, mt))
				{
					printf("aborted %d while deleting\n", get_abort_error_for_mini_tx(&mte, mt));
					exit(-1);
				}

				// remove completed so we break
				break;
			}
			else
			{
				int next_res = next_hash_table_iterator(hti, 0, mt, &abort_error);
				if(is_aborted_for_mini_tx(&mte, mt))
				{
					printf("aborted %d while goinf next for deleting\n", get_abort_error_for_mini_tx(&mte, mt));
					exit(-1);
				}

				if(next_res == 0)
					break;
			}

			curr = get_tuple_hash_table_iterator(hti);
		}
	}

	hash_table_vaccum_params htvp;
	delete_hash_table_iterator(hti, &htvp, mt, &abort_error);
	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while deleting\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}

	if(allow_vaccum)
	{
		perform_vaccum_hash_table(root_page_id, &htvp, 1, &httd, &pam, &pmm, mt, &abort_error);
		if(is_aborted_for_mini_tx(&mte, mt))
		{
			printf("aborted %d while vaccumming after delete\n", get_abort_error_for_mini_tx(&mte, mt));
			exit(-1);
		}
	}

	return res;
}

void print_uint_hash_table(mini_transaction* mt)
{
	int abort_error = 0;
	print_hash_table(root_page_id, &httd, &pam, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while printing\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}
}

void destroy_uint_hash_table(mini_transaction* mt)
{
	int abort_error = 0;
	destroy_hash_table(root_page_id, &httd, &pam, mt, &abort_error);

	if(is_aborted_for_mini_tx(&mte, mt))
	{
		printf("aborted %d while destroying\n", get_abort_error_for_mini_tx(&mte, mt));
		exit(-1);
	}
}

int main2(uint64_t bucket_count)
{
	initialize_tuple_defs();
	if(!init_hash_table_tuple_definitions(&httd, &(pam.pas), &record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, FNV_64_TUPLE_HASHER))
	{
		printf("failed to initialize hash table tuple definitions\n");
		exit(-1);
	}

	for(uint32_t i = 0; i < JOBS_COUNT; i++)
		input[i] = i;

	for(uint32_t i = 0; i < JOBS_COUNT; i++)
	{
		uint32_t i1 = (((uint32_t)rand()) % (JOBS_COUNT));
		uint32_t i2 = (((uint32_t)rand()) % (JOBS_COUNT));
		memory_swap(input + i1, input + i2, sizeof(input[i1]));
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_hash_table(mt, bucket_count);

		print_uint_hash_table(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "creation_done", strlen("creation_done"));
		printf("completed creation at "); print_uint256(cLSN); printf("\n");
	}

	{
		uint256 uLSN = append_user_info_log_record_for_mini_transaction_engine(&mte, 1, "started insertions", strlen("started insertions"));
		printf("user log at "); print_uint256(uLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint32_t i = 0; i < JOBS_COUNT; i++)
		{
			insert_uint_hash_table(mt, input[i], NULL, 1); // allowing cleanup

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_hash_table(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "insertions_done", strlen("insertions_done"));
		printf("completed insertions at "); print_uint256(cLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "printing_done", strlen("printing_done"));
		printf("completed printing at "); print_uint256(cLSN); printf("\n");
	}

	{
		uint256 uLSN = append_user_info_log_record_for_mini_transaction_engine(&mte, 1, "started deletions", strlen("started deletions"));
		printf("user log at "); print_uint256(uLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint32_t i = 0; i < JOBS_COUNT; i++)
		{
			delete_uint_hash_table(mt, input[i], 1); // allowing cleanup

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_hash_table(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "deletions_done", strlen("deletions_done"));
		printf("completed deletions at "); print_uint256(cLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "printing_done", strlen("printing_done"));
		printf("completed printing at "); print_uint256(cLSN); printf("\n");
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		destroy_uint_hash_table(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "destruction_done", strlen("destruction_done"));
		printf("completed destruction at "); print_uint256(cLSN); printf("\n");
	}

	/*{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "printing_done", strlen("printing_done"));
		printf("completed printing at "); print_uint256(cLSN); printf("\n");
	}*/

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_hash_table(mt, bucket_count);

		print_uint_hash_table(mt);

		uint256 cLSN = mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, "creation_done", strlen("creation_done"));
		printf("completed creation at "); print_uint256(cLSN); printf("\n");
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

	deinitialize_tuple_defs();
	return 0;
}

#include<executor.h>

#define WORKER_COUNT 30

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
int duplicates_encountered = 0;
int writable_aborts_done = 0;

void* perform_insert_bplus_tree(void* param)
{
	uint64_t p = *(uint64_t*)(param);

	mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

	int res = insert_uint_bplus_tree(mt, p, "concurrent-inserts");

	if(res == 0)
	{
		if(!are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
		{
			printf("Bug in bplus tree insert, insert failed but mini_transaction_id is assigned\n");
			exit(-1);
		}
		pthread_mutex_lock(&mtx);
		duplicates_encountered++;
		pthread_mutex_unlock(&mtx);
	}

	if((9 <= (p % 23)) && ((p % 23) <= 12))
	{
		int aborted = mark_aborted_for_mini_tx(&mte, mt, -55);
		if(!aborted)
			printf("Abortion failed\n");
		pthread_mutex_lock(&mtx);
		writable_aborts_done += (res && aborted);
		pthread_mutex_unlock(&mtx);
	}

	mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);

	return NULL;
}

int main3()
{
	initialize_tuple_defs();
	if(!init_bplus_tree_tuple_definitions(&bpttd, &(pam.pas), &record_def, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT))
	{
		printf("failed to initialize bplus tree tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		create_uint_bplus_tree(mt);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}
	/*{
		root_page_id = 1;
	}*/

	executor* exe = new_executor(FIXED_THREAD_COUNT_EXECUTOR, WORKER_COUNT, JOBS_COUNT + 32, 1000000, NULL, NULL, NULL);

	int failed_job_submissions = 0;
	for(uint32_t i = 0; i < JOBS_COUNT; i++)
	{
		input[i] = (((uint64_t)rand()) % (5*(JOBS_COUNT+13)));
		failed_job_submissions += (0 == submit_job_executor(exe, perform_insert_bplus_tree, input+i, NULL, NULL, 1000000));
	}

	shutdown_executor(exe, 0);
	wait_for_all_executor_workers_to_complete(exe);
	delete_executor(exe);

	pthread_mutex_lock(&mtx);
	printf("jobs_count = %d\n", JOBS_COUNT);
	printf("failed_job_submissions = %d\n", failed_job_submissions);
	printf("duplicates_encountered = %d\n", duplicates_encountered);
	printf("writable_aborts_done = %d\n", writable_aborts_done);
	pthread_mutex_unlock(&mtx);

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		print_uint_bplus_tree(mt);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

	deinitialize_tuple_defs();
	return 0;
}

void* perform_insert_hash_table(void* param)
{
	uint64_t p = *(uint64_t*)(param);

	mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

	int res = insert_uint_hash_table(mt, p, "concurrent-inserts", 0); // do not allow cleanup

	if(res == 0)
	{
		if(!are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
		{
			printf("Bug in bplus tree insert, insert failed but mini_transaction_id is assigned\n");
			exit(-1);
		}
		pthread_mutex_lock(&mtx);
		duplicates_encountered++;
		pthread_mutex_unlock(&mtx);
	}

	if((9 <= (p % 23)) && ((p % 23) <= 12))
	{
		int aborted = mark_aborted_for_mini_tx(&mte, mt, -55);
		if(!aborted)
			printf("Abortion failed\n");
		pthread_mutex_lock(&mtx);
		writable_aborts_done += (res && aborted);
		pthread_mutex_unlock(&mtx);
	}

	mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);

	return NULL;
}

int main4(uint64_t bucket_count)
{
	initialize_tuple_defs();
	if(!init_hash_table_tuple_definitions(&httd, &(pam.pas), &record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, FNV_64_TUPLE_HASHER))
	{
		printf("failed to initialize hash table tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		create_uint_hash_table(mt, bucket_count);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}
	/*{
		root_page_id = 1;
	}*/

	executor* exe = new_executor(FIXED_THREAD_COUNT_EXECUTOR, WORKER_COUNT, JOBS_COUNT + 32, 1000000, NULL, NULL, NULL);

	int failed_job_submissions = 0;
	for(uint32_t i = 0; i < JOBS_COUNT; i++)
	{
		input[i] = (((uint64_t)rand()) % (5*(JOBS_COUNT+13)));
		failed_job_submissions += (0 == submit_job_executor(exe, perform_insert_hash_table, input+i, NULL, NULL, 1000000));
	}

	shutdown_executor(exe, 0);
	wait_for_all_executor_workers_to_complete(exe);
	delete_executor(exe);

	pthread_mutex_lock(&mtx);
	printf("jobs_count = %d\n", JOBS_COUNT);
	printf("failed_job_submissions = %d\n", failed_job_submissions);
	printf("duplicates_encountered = %d\n", duplicates_encountered);
	printf("writable_aborts_done = %d\n", writable_aborts_done);
	pthread_mutex_unlock(&mtx);

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		print_uint_hash_table(mt);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

	deinitialize_tuple_defs();
	return 0;
}

#include<string.h>
#include<page_layout.h>

void construct_tuple(char* tuple, const tuple_def* tpl_def, const char* a, uint64_t b)
{
	init_tuple(tpl_def, tuple);
	if(a != NULL) // if NULL leave it NULL
		set_element_in_tuple(tpl_def, STATIC_POSITION(0), tuple, &(user_value){.string_value = a, .string_size = strlen(a)}, UINT32_MAX);
	if(b != -1) // if -1 leave it NULL
		set_element_in_tuple(tpl_def, STATIC_POSITION(1), tuple, &(user_value){.uint_value = b}, UINT32_MAX);
}

void main0()
{
	data_type_info str = get_variable_length_string_type("", SYSTEM_PAGE_SIZE);
	data_type_info* tup = malloc(sizeof_tuple_data_type_info(2));
	initialize_tuple_data_type_info((tup), "tuple", 1, SYSTEM_PAGE_SIZE, 2);
	strcpy(tup->containees[0].field_name, "a");
	tup->containees[0].type_info = &str;
	strcpy(tup->containees[1].field_name, "b");
	tup->containees[1].type_info = UINT_NULLABLE[5];

	initialize_tuple_def(&record_def, tup);

	uint64_t page_id = 0;
	void* page = NULL;
	char tuple[SYSTEM_PAGE_SIZE];

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		page = get_new_page_with_write_latch_for_mini_tx(&mte, mt, &page_id);

		init_page_for_mini_tx(&mte, mt, page, 5, &(record_def.size_def));

		construct_tuple(tuple, &record_def, "Rohan Vipulkumar Dvivedi", 1996);
		append_tuple_on_page_for_mini_tx(&mte, mt, page, &(record_def.size_def), tuple);

		construct_tuple(tuple, &record_def, "Rupa Vipulkumar Dvivedi", 1966);
		append_tuple_on_page_for_mini_tx(&mte, mt, page, &(record_def.size_def), tuple);

		printf("intialization\n");
		print_page(page, mte.user_stats.page_size, &record_def);

		release_writer_latch_on_page_for_mini_tx(&mte, mt, page, 0);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}
	printf("-x-x-x-x- tx1 complete\n");

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		page = acquire_page_with_writer_latch_for_mini_tx(&mte, mt, page_id);

		set_element_in_tuple_in_place_on_page_for_mini_tx(&mte, mt, page, &record_def, 0, STATIC_POSITION(0), &(user_value){.string_value = "Rohan Dvivedi", .string_size = strlen("Rohan Dvivedi")});
		set_element_in_tuple_in_place_on_page_for_mini_tx(&mte, mt, page, &record_def, 0, STATIC_POSITION(1), &(user_value){.uint_value = (2024 - 1996)});
		set_element_in_tuple_in_place_on_page_for_mini_tx(&mte, mt, page, &record_def, 1, STATIC_POSITION(0), &(user_value){.string_value = "Rupa Dvivedi", .string_size = strlen("Rupa Dvivedi")});
		set_element_in_tuple_in_place_on_page_for_mini_tx(&mte, mt, page, &record_def, 1, STATIC_POSITION(1), &(user_value){.uint_value = (2024 - 1966)});

		printf("printing after update\n");
		print_page(page, mte.user_stats.page_size, &record_def);


		printf("aborting\n");
		mark_aborted_for_mini_tx(&mte, mt, -55);
		release_writer_latch_on_page_for_mini_tx(&mte, mt, page, 0);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}
	printf("-x-x-x-x- tx2 complete\n");

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		page = acquire_page_with_reader_latch_for_mini_tx(&mte, mt, page_id);

		print_page(page, mte.user_stats.page_size, &record_def);

		release_reader_latch_on_page_for_mini_tx(&mte, mt, page, 0);
		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}
	printf("-x-x-x-x- tx3 complete\n");

	free(tup);
}

#define ACTIVE_MINI_TRANSACTIONS_TO_TEST 10

#include<unistd.h>

void main_1()
{
	data_type_info str = get_variable_length_string_type("", SYSTEM_PAGE_SIZE);
	data_type_info* tup = malloc(sizeof_tuple_data_type_info(2));
	initialize_tuple_data_type_info((tup), "tuple", 1, SYSTEM_PAGE_SIZE, 2);
	strcpy(tup->containees[0].field_name, "a");
	tup->containees[0].type_info = &str;
	strcpy(tup->containees[1].field_name, "b");
	tup->containees[1].type_info = UINT_NULLABLE[5];

	initialize_tuple_def(&record_def, tup);

	uint64_t page_id[ACTIVE_MINI_TRANSACTIONS_TO_TEST] = {};

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		void* page[ACTIVE_MINI_TRANSACTIONS_TO_TEST] = {};

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
			page[i] = get_new_page_with_write_latch_for_mini_tx(&mte, mt, &(page_id[i]));

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
			init_page_for_mini_tx(&mte, mt, page[i], 5, &(record_def.size_def));

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
		{
			release_writer_latch_on_page_for_mini_tx(&mte, mt, page[i], 0);
			page[i] = NULL;
		}

		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
		mt = NULL;
	}
	printf("-x-x-x-x- init completed\n");

	char tuple[SYSTEM_PAGE_SIZE];

	{
		mini_transaction* mt[ACTIVE_MINI_TRANSACTIONS_TO_TEST] = {};
		void* page[ACTIVE_MINI_TRANSACTIONS_TO_TEST] = {};

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
			mt[i] = mte_allot_mini_tx(&mte, 1000000);

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
			page[i] = acquire_page_with_writer_latch_for_mini_tx(&mte, mt[i], page_id[i]);

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
		{
			construct_tuple(tuple, &record_def, "Rohan Vipulkumar Dvivedi", 1996 + i);
			append_tuple_on_page_for_mini_tx(&mte, mt[i], page[i], &(record_def.size_def), tuple);
		}

		// sleep for a checkpoint to pass by
		sleep((CHECKPOINT_PERIOD_US/1000000) + 1);

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
		{
			printf("page_id = %"PRIu64"\n", page_id[i]);
			print_page(page[i], mte.user_stats.page_size, &record_def);
			printf("\n");
		}

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
		{
			release_writer_latch_on_page_for_mini_tx(&mte, mt[i], page[i], 0);
			page[i] = NULL;
		}

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
		{
			if((i % 2) == 1)
				mark_aborted_for_mini_tx(&mte, mt[i], -55);

			mte_complete_mini_tx(&mte, mt[i], FLUSH_ON_COMPLETION, NULL, 0);
			mt[i] = NULL;
		}
	}
	printf("-x-x-x-x- writes completed\n");

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(int i = 0; i < ACTIVE_MINI_TRANSACTIONS_TO_TEST; i++)
		{
			void* page = acquire_page_with_reader_latch_for_mini_tx(&mte, mt, page_id[i]);

			printf("page_id = %"PRIu64"\n", page_id[i]);
			print_page(page, mte.user_stats.page_size, &record_def);
			printf("\n");

			release_reader_latch_on_page_for_mini_tx(&mte, mt, page, 0);
			page = NULL;
		}

		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
		mt = NULL;
	}
	printf("-x-x-x-x- read completed");

	printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);

	free(tup);
}

void main_2()
{
	printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);
}

void main_3()
{
	printf("PRINTING LOGS\n");
	uint256 LSN = INVALID_LOG_SEQUENCE_NUMBER;
	LSN = get_next_LSN_of_LSN_for_mini_transaction_engine(&mte, LSN);
	while(1)
	{
		if(are_equal_uint256(LSN, INVALID_LOG_SEQUENCE_NUMBER))
			break;

		log_record lr;
		int lr_read = get_log_record_at_LSN_for_mini_transaction_engine(&mte, LSN, &lr);
		if(!lr_read)
			break;

		printf("LSN : "); print_uint256(LSN); printf("\n");
		print_log_record(&lr, &(mte.stats));printf("\n");

		destroy_and_free_parsed_log_record(&lr);

		LSN = get_next_LSN_of_LSN_for_mini_transaction_engine(&mte, LSN);
	}
}

void main5(uint64_t _root_page_id)
{
	root_page_id = _root_page_id;

	initialize_tuple_defs();
	if(!init_bplus_tree_tuple_definitions(&bpttd, &(pam.pas), &record_def, KEY_POS, CMP_DIR, RECORD_S_KEY_ELEMENT_COUNT))
	{
		printf("failed to initialize bplus tree tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}


	printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);

	deinitialize_tuple_defs();
}

void main6(uint64_t _root_page_id)
{
	root_page_id = _root_page_id;

	initialize_tuple_defs();
	if(!init_hash_table_tuple_definitions(&httd, &(pam.pas), &record_def, KEY_POS, RECORD_S_KEY_ELEMENT_COUNT, FNV_64_TUPLE_HASHER))
	{
		printf("failed to initialize hash table tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		mte_complete_mini_tx(&mte, mt, FLUSH_ON_COMPLETION, NULL, 0);
	}

	printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);

	deinitialize_tuple_defs();
}

int main()
{
	if(!initialize_mini_transaction_engine(&mte, db_filename, SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US, CHECKPOINT_PERIOD_SIZE, MAX_WAL_FILE_SIZE))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	init_pmm_for_mini_tx_engine(&mte);

	// seed random number generator
	srand(time(NULL));

	//sleep((CHECKPOINT_PERIOD_US / 1000000) + 1);		// stay alive for 1 checkpoint
	//main_3(); 										// prints logs and exits
	//main_2(); 										// prints logs and exits
	//main_1();
	//main0();
	//main1();											// bplus_tree
	//main2(100);  										// linked_page_list heavy hash_table
	//main2(300);										// sweet spot
	//main2(2000);										// array_table heavy hash_table
	main3();											// concurrent test for bplus tree insertion
	//main4(1000);										// concurrent test for hash table insertion
	//main5(1);											// prints bplus tree at root page id = 1
	//main6(1); 										// prints hash table at root page_id = 1
	printf("total pages used = %"PRIu64"\n", mte.database_page_count);

	deinitialize_mini_transaction_engine(&mte);
}