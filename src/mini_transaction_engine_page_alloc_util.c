#include<mini_transaction_engine_page_alloc_util.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

#include<bitmap.h>

int free_write_latched_page_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, void* page, uint64_t page_id)
{
	if(is_free_space_mapper_page(page_id, &(mte->stats)))
	{
		mt->state = MIN_TX_ABORTED;
		mt->abort_error = ILLEGAL_PAGE_ID;
		return 0;
	}

	// fetch the free space mapper page an bit position that we need to flip
	uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));
	uint64_t free_space_mapper_bit_pos = get_is_valid_bit_position_for_page(page_id, &(mte->stats));
	pthread_mutex_lock(&(mte->global_lock));
	void* free_space_mapper_page = acquire_page_with_writer_lock(&(mte->bufferpool_handle), free_space_mapper_page_id, mte->latch_wait_timeout_in_microseconds, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
	pthread_mutex_unlock(&(mte->global_lock));
	if(free_space_mapper_page == NULL) // could not lock free_space_mapper_page, so abort
	{
		mt->state = MIN_TX_ABORTED;
		mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
		return 0;
	}

	// perform full page writes for both the pages, if necessary
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, free_space_mapper_page, free_space_mapper_page_id);

	// construct page_deallocation log record
	log_record act_lr = {
		.type = PAGE_DEALLOCATION,
		.palr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
		exit(-1);

	// reset the free_space_mapper_bit_pos on the free_space_mapper_page
	{
		void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
		reset_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
	}

	// log the page deallocation log record and manage state
	pthread_mutex_lock(&(mte->global_lock));
	{
		wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_act_lr, serialized_act_lr_size, 0, &wal_error);
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

		// since this log record modified both the pages, set page lsn on both of them
		set_pageLSN_for_page(page, log_record_LSN, &(mte->stats));
		set_pageLSN_for_page(free_space_mapper_page, log_record_LSN, &(mte->stats));

		// set the writer LSN to the mini_transaction_id for the page, (marking it as write locked)
		set_writerLSN_for_page(page, mt->mini_transaction_id, &(mte->stats));
		// since we got the write latch on the page, either we ourselves have locked the page OR it was not persistently locked by any one

		// mark the page and free_space_mapper_page as dirty in the bufferpool and dirty page table
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, free_space_mapper_page, free_space_mapper_page_id);

		// this has to succeed, we already marked it dirty, so was_modified can be set to 0
		release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // was_modified = 0, force_flush = 0
		release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // was_modified = 0, force_flush = 0
	}
	pthread_mutex_unlock(&(mte->global_lock));

	// free the actual change log record
	free((void*)serialized_act_lr);

	return 1;
}