#include<stdio.h>
#include<stdlib.h>

#include<mini_transaction_engine.h>
#include<callbacks_tupleindexer.h>

#include<bplus_tree.h>

mini_transaction_engine mte;

#define SYSTEM_PAGE_SIZE 1024
#define PAGE_ID_WIDTH 4
#define LSN_WIDTH 4

#define BUFFERPOOL_BUFFERS 100
#define WALE_BUFFERS 10

#define LATCH_WAIT_TIMEOUT_US 3000
#define LOCK_WAIT_TIMEOUT_US 30000
#define CHECKPOINT_PERIOD_US (5 * 60 * 1000000) // 5 minutes

bplus_tree_tuple_defs bpttd;
uint64_t root_page_id;

tuple_def record_def;

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

int main()
{
	if(!initialize_mini_transaction_engine(&mte, "testbpt.db", SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	init_pmm_for_mini_tx_engine(&mte);
	initialize_tuple_def(&record_def, UINT_NON_NULLABLE[8]);
	if(!init_bplus_tree_tuple_definitions(&bpttd, &(pam.pas), &record_def, (positional_accessor []){SELF}, (compare_direction[]){ASC}, 1))
	{
		printf("failed to initialize bplus tree tuple definitions\n");
		exit(-1);
	}

	{
		mini_transaction* mt1 = mte_allot_mini_tx(&mte, 1000000);

		create_uint_bplus_tree(mt1);

		print_uint_bplus_tree(mt1);

		mte_complete_mini_tx(&mte, mt1, NULL, 0);
	}

	{
		mini_transaction* mt2 = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 10000; i >= 100; i--)
		{
			insert_uint_bplus_tree(mt2, i);

			if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);
		}

		print_uint_bplus_tree(mt2);

		mte_complete_mini_tx(&mte, mt2, NULL, 0);
	}

	{
		mini_transaction* mt3 = mte_allot_mini_tx(&mte, 1000000);

		for(uint64_t i = 10000; i >= 100; i--)
		{
			delete_uint_bplus_tree(mt3, i);

			if(i % 500 == 0)
				intermediate_wal_flush_for_mini_transaction_engine(&mte);
		}

		print_uint_bplus_tree(mt3);

		// abort here
		mark_aborted_for_mini_tx(&mte, mt3, -55);

		mte_complete_mini_tx(&mte, mt3, NULL, 0);
	}

	{
		mini_transaction* mt4 = mte_allot_mini_tx(&mte, 1000000);

		print_uint_bplus_tree(mt4);

		destroy_uint_bplus_tree(mt4);

		mte_complete_mini_tx(&mte, mt4, NULL, 0);
	}

	{
		mini_transaction* mt5 = mte_allot_mini_tx(&mte, 1000000);

		create_uint_bplus_tree(mt5);

		print_uint_bplus_tree(mt5);

		mte_complete_mini_tx(&mte, mt5, NULL, 0);
	}

	return 0;
}