#include<mini_transaction_engine_wale_only_functions.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

// the below function does the following
/*
	1. logs log record to latest wale, gets the log_record_LSN for this record
	2. if mt->mini_transaction_id == INVALID, then assigns it and moves the mt to writer_mini_transactions and assigns its mini_transaction_id to log_record_LSN
	3. mt->lastLSN = log_record_LSN
	4. page->pageLSN = log_record_LSN
	5. if the page_id is not that of free space mapper page, then page->writerLSN = mt->mini_transaction_id
	6. mark it dirty in dirty page table and bufferpool both
	7. returns log_record_LSN of the log record we just logged
*/
static uint256 log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mini_transaction_engine* mte, const void* log_record, uint32_t log_record_size, mini_transaction* mt, void* page, uint64_t page_id)
{
	wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

	int wal_error = 0;
	uint256 log_record_LSN = append_log_record(wale_p, log_record, log_record_size, 0, &wal_error);
	if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		exit(-1);

	if(are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
	{
		mt->mini_transaction_id = log_record_LSN;

		// remove mt from mte->reader_mini_transactions
		remove_from_linkedlist(&(mte->reader_mini_transactions), mt);

		// insert it to mte->writer_mini_transactions
		insert_in_hashmap(&(mte->writer_mini_transactions), mt);
	}

	mt->lastLSN = log_record_LSN;

	set_pageLSN_for_page(page, log_record_LSN, &(mte->stats));

	// set the writer LSN to the mini_transaction_id, (marking it as write locked) only if it is not a free space mapper page
	if(!is_free_space_mapper_page(page_id, &(mte->stats)))
		set_writerLSN_for_page(page, mt->mini_transaction_id, &(mte->stats));

	// mark the page as dirty in the bufferpool and dirty page table
	mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);

	return log_record_LSN;
}

int init_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, uint32_t page_header_size, const tuple_size_def* tpl_sz_d)
{
	// TODO
}

void set_page_header_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const void* hdr, int* abort_error)
{
	// TODO
}

int append_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	// TODO
}

int insert_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	// TODO
}

int update_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	// TODO
}

int discard_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index)
{
	// TODO
}

void discard_all_tuples_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d)
{
	// TODO
}

uint32_t discard_trailing_tomb_stones_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d)
{
	// TODO
}

int swap_tuples_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2)
{
	// TODO
}

int set_element_in_tuple_in_place_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const user_value* value)
{
	// TODO
}

void clone_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, const void* page_contents_src)
{
	// TODO
}

int run_page_compaction_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, uint32_t page_size, const tuple_size_def* tpl_sz_d)
{
	// TODO
}