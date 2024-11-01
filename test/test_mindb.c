#include<stdio.h>
#include<stdlib.h>

#include<mini_transaction_engine.h>
#include<callbacks_tupleindexer.h>

#include<bplus_tree.h>
#include<hash_table.h>

mini_transaction_engine mte;

#define SYSTEM_PAGE_SIZE 1024
#define PAGE_ID_WIDTH 4
#define LSN_WIDTH 4

#define BUFFERPOOL_BUFFERS 100
#define WALE_BUFFERS 10

#define LATCH_WAIT_TIMEOUT_US     300
#define LOCK_WAIT_TIMEOUT_US  2000000
#define CHECKPOINT_PERIOD_US (5 * 60 * 1000000) // 5 minutes

const char* db_filename = "test.db";

uint64_t root_page_id;

tuple_def record_def;

positional_accessor KEY_POS[1] = {SELF};
compare_direction CMP_DIR[1] = {ASC};

// tests for bplus tree

bplus_tree_tuple_defs bpttd;

void create_uint_bplus_tree(mini_transaction* mt)
{
	root_page_id = get_new_bplus_tree(&bpttd, &pam, &pmm, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while creating\n", mt->abort_error);
		exit(-1);
	}
}

int insert_uint_bplus_tree(mini_transaction* mt, uint64_t x)
{
	int res = insert_in_bplus_tree(root_page_id, &x, &bpttd, &pam, &pmm, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while inserting\n", mt->abort_error);
		exit(-1);
	}

	return res;
}

int delete_uint_bplus_tree(mini_transaction* mt, uint64_t x)
{
	int res = delete_from_bplus_tree(root_page_id, &x, &bpttd, &pam, &pmm, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while deleting\n", mt->abort_error);
		exit(-1);
	}

	return res;
}

void print_uint_bplus_tree(mini_transaction* mt)
{
	print_bplus_tree(root_page_id, 1, &bpttd, &pam, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while printing\n", mt->abort_error);
		exit(-1);
	}
}

void destroy_uint_bplus_tree(mini_transaction* mt)
{
	destroy_bplus_tree(root_page_id, &bpttd, &pam, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while destroying\n", mt->abort_error);
		exit(-1);
	}
}

int main1()
{
	if(!initialize_mini_transaction_engine(&mte, db_filename, SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	init_pmm_for_mini_tx_engine(&mte);
	initialize_tuple_def(&record_def, UINT_NON_NULLABLE[8]);
	if(!init_bplus_tree_tuple_definitions(&bpttd, &(pam.pas), &record_def, KEY_POS, CMP_DIR, 1))
	{
		printf("failed to initialize bplus tree tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_bplus_tree(mt);

		print_uint_bplus_tree(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 10000; i >= 100; i--)
		{
			insert_uint_bplus_tree(mt, i);

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_bplus_tree(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 10000; i >= 100; i--)
		{
			delete_uint_bplus_tree(mt, i);

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_bplus_tree(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		destroy_uint_bplus_tree(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	/*{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}*/

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_bplus_tree(mt);

		print_uint_bplus_tree(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

	return 0;
}

hash_table_tuple_defs httd;

uint64_t hash_func(const void* data, uint32_t data_size)
{
	uint64_t res;
	memory_move(&res, data, 4);
	return res;
}

void create_uint_hash_table(mini_transaction* mt, uint64_t bucket_count)
{
	root_page_id = get_new_hash_table(1000, &httd, &pam, &pmm, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while creating\n", mt->abort_error);
		exit(-1);
	}
}

int insert_uint_hash_table(mini_transaction* mt, uint64_t x)
{
	hash_table_iterator* hti = get_new_hash_table_iterator(root_page_id, WHOLE_BUCKET_RANGE, &x, &httd, &pam, &pmm, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while inserting\n", mt->abort_error);
		exit(-1);
	}

	int res = insert_in_hash_table_iterator(hti, &x, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while inserting\n", mt->abort_error);
		exit(-1);
	}

	hash_table_vaccum_params htvp;
	delete_hash_table_iterator(hti, &htvp, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while inserting\n", mt->abort_error);
		exit(-1);
	}

	perform_vaccum_hash_table(root_page_id, &htvp, 1, &httd, &pam, &pmm, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while vaccumming after insert\n", mt->abort_error);
		exit(-1);
	}

	return res;
}

int delete_uint_hash_table(mini_transaction* mt, uint64_t x)
{
	hash_table_iterator* hti = get_new_hash_table_iterator(root_page_id, WHOLE_BUCKET_RANGE, &x, &httd, &pam, &pmm, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while deleting\n", mt->abort_error);
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
				res += remove_from_hash_table_iterator(hti, mt, &(mt->abort_error));
				if(mt->abort_error)
				{
					printf("aborted %d while deleting\n", mt->abort_error);
					exit(-1);
				}
			}
			else
			{
				int next_res = next_hash_table_iterator(hti, 0, mt, &(mt->abort_error));
				if(mt->abort_error)
				{
					printf("aborted %d while deleting\n", mt->abort_error);
					exit(-1);
				}

				if(next_res == 0)
					break;
			}

			curr = get_tuple_hash_table_iterator(hti);
		}
	}

	hash_table_vaccum_params htvp;
	delete_hash_table_iterator(hti, &htvp, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while deleting\n", mt->abort_error);
		exit(-1);
	}

	perform_vaccum_hash_table(root_page_id, &htvp, 1, &httd, &pam, &pmm, mt, &(mt->abort_error));
	if(mt->abort_error)
	{
		printf("aborted %d while vaccumming after delete\n", mt->abort_error);
		exit(-1);
	}

	return res;
}

void print_uint_hash_table(mini_transaction* mt)
{
	print_hash_table(root_page_id, &httd, &pam, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while printing\n", mt->abort_error);
		exit(-1);
	}
}

void destroy_uint_hash_table(mini_transaction* mt)
{
	destroy_hash_table(root_page_id, &httd, &pam, mt, &(mt->abort_error));

	if(mt->abort_error)
	{
		printf("aborted %d while destroying\n", mt->abort_error);
		exit(-1);
	}
}

int main2(uint64_t bucket_count)
{
	if(!initialize_mini_transaction_engine(&mte, db_filename, SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	init_pmm_for_mini_tx_engine(&mte);
	initialize_tuple_def(&record_def, UINT_NON_NULLABLE[8]);
	if(!init_hash_table_tuple_definitions(&httd, &(pam.pas), &record_def, KEY_POS, 1, hash_func))
	{
		printf("failed to initialize hash table tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_hash_table(mt, bucket_count);

		print_uint_hash_table(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 10000; i >= 100; i--)
		{
			insert_uint_hash_table(mt, i);

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_hash_table(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 10000; i >= 100; i--)
		{
			delete_uint_hash_table(mt, i);

			/*if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);*/
		}

		print_uint_hash_table(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		destroy_uint_hash_table(mt);

		// abort here
		//mark_aborted_for_mini_tx(&mte, mt, -55);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	/*{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		print_uint_hash_table(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}*/

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

		create_uint_hash_table(mt, bucket_count);

		print_uint_hash_table(mt);

		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

	return 0;
}

#include<executor.h>

#define JOBS_COUNT 10000
#define WORKER_COUNT 30

pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;
int duplicates_encountered = 0;
int aborts_done = 0;

void* perform_insert(void* param)
{
	uint64_t p = *(uint64_t*)(param);

	mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);

	int res = insert_uint_bplus_tree(mt, p);

	if(res == 0)
	{
		pthread_mutex_lock(&mtx);
		duplicates_encountered++;
		pthread_mutex_unlock(&mtx);
	}

	if((9 <= (p % 23)) && ((p % 23) <= 12))
	{
		pthread_mutex_lock(&mtx);
		aborts_done++;
		pthread_mutex_unlock(&mtx);
		mark_aborted_for_mini_tx(&mte, mt, -55);
	}

	mte_complete_mini_tx(&mte, mt, NULL, 0);

	return NULL;
}

int main3()
{
	if(!initialize_mini_transaction_engine(&mte, db_filename, SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	init_pmm_for_mini_tx_engine(&mte);
	initialize_tuple_def(&record_def, UINT_NON_NULLABLE[8]);
	if(!init_bplus_tree_tuple_definitions(&bpttd, &(pam.pas), &record_def, KEY_POS, CMP_DIR, 1))
	{
		printf("failed to initialize bplus tree tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		create_uint_bplus_tree(mt);
		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	executor* exe = new_executor(FIXED_THREAD_COUNT_EXECUTOR, WORKER_COUNT, JOBS_COUNT + 32, 1000000, NULL, NULL, NULL);
	uint64_t input[JOBS_COUNT];

	for(uint32_t i = 0; i < JOBS_COUNT; i++)
	{
		input[i] = (((uint64_t)rand()) % (JOBS_COUNT+13));
		submit_job_executor(exe, perform_insert, input+i, NULL, NULL, 1000000);
	}

	shutdown_executor(exe, 0);
	wait_for_all_executor_workers_to_complete(exe);
	delete_executor(exe);

	printf("duplicates_encountered = %d\n", duplicates_encountered);
	printf("aborts_done = %d\n", aborts_done);

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		print_uint_bplus_tree(mt);
		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}

	/*printf("PRINTING LOGS\n");
	debug_print_wal_logs_for_mini_transaction_engine(&mte);*/

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
	if(!initialize_mini_transaction_engine(&mte, db_filename, SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	init_pmm_for_mini_tx_engine(&mte);

	data_type_info str = get_variable_length_string_type("", 100);
	data_type_info* tup = malloc(sizeof_tuple_data_type_info(2));
	initialize_tuple_data_type_info((tup), "tuple", 1, 128, 2);
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
		mte_complete_mini_tx(&mte, mt, NULL, 0);
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
		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}
	printf("-x-x-x-x- tx2 complete\n");

	{
		mini_transaction* mt = mte_allot_mini_tx(&mte, 1000000);
		page = acquire_page_with_reader_latch_for_mini_tx(&mte, mt, page_id);

		print_page(page, mte.user_stats.page_size, &record_def);

		release_reader_latch_on_page_for_mini_tx(&mte, mt, page, 0);
		mte_complete_mini_tx(&mte, mt, NULL, 0);
	}
	printf("-x-x-x-x- tx3 complete\n");

	free(tup);
}

int main()
{
	//main0();
	//main1();
	//main2(5);  	// linked_page_list heavy hash_table
	//main2(2000);	// array_table heavy hash_table
	main3();
}