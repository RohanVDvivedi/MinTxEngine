#include<mini_transaction_engine_util.h>

#include<system_page_header_util.h>

void mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mini_transaction_engine* mte, void* page, uint64_t page_id)
{
	notify_modification_for_write_locked_page(&(mte->bufferpool_handle), page);

	dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(mte->dirty_page_table), &((dirty_page_table_entry){.page_id = page_id}));
	
	// if it is already present in dirty page table then nothing needs to be done
	if(dpte != NULL)
		return ;

	// else create or get one from free list
	dpte = (dirty_page_table_entry*)get_head_of_linkedlist(&(mte->free_dirty_page_entries_list));
	if(dpte != NULL)
		remove_head_from_linkedlist(&(mte->free_dirty_page_entries_list));
	else
		dpte = get_new_dirty_page_table_entry();

	// set appropriate parameters and insert it to dirty page table
	dpte->page_id = page_id;
	dpte->recLSN = get_pageLSN_for_page(page, &(mte->stats));
	insert_in_hashmap(&(mte->dirty_page_table), dpte);
}

mini_transaction* get_mini_transaction_that_last_persistent_write_locked_this_page_UNSAFE(mini_transaction_engine* mte, void* page)
{
	uint256 writerLSN = get_writerLSN_for_page(page, &(mte->stats));

	if(are_equal_uint256(writerLSN, INVALID_LOG_SEQUENCE_NUMBER))
		return NULL;

	mini_transaction* mt = (mini_transaction*)find_equals_in_hashmap(&(mte->writer_mini_transactions), &(mini_transaction){.mini_transaction_id = writerLSN});
	if(mt == NULL)
		return NULL;

	return mt;
}

void decrement_mini_transaction_reference_counter_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt)
{
	mt->reference_counter--;

	// if the reference count is non zero, nothign needs to be done
	if(mt->reference_counter > 0)
		return;

	if(are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER)) // it is still a reader
		remove_from_linkedlist(&(mte->reader_mini_transactions), mt);
	else // it is a writer
		remove_from_hashmap(&(mte->writer_mini_transactions), mt);

	// add it to free_list
	insert_head_in_linkedlist(&(mte->free_mini_transactions_list), mt);

	// wake up anyone waiting for execution slot
	pthread_cond_signal(&(mte->conditional_to_wait_for_execution_slot));
}

int wait_for_mini_transaction_completion_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt)
{
	// no one will make this mini transaction free as you just incremented it reference counter
	mt->reference_counter++;

	int wait_error = 0;
	uint64_t write_lock_wait_timeout_in_microseconds_LEFT = mte->write_lock_wait_timeout_in_microseconds;
	while(mt->state != MIN_TX_COMPLETED && !wait_error)
	{
		struct timespec current_time;
		clock_gettime(CLOCK_REALTIME, &current_time);

		// make an attempt for atleast write_lock_wait_timeout_in_microseconds_LEFT
		{
			struct timespec diff = {.tv_sec = (write_lock_wait_timeout_in_microseconds_LEFT / 1000000LL), .tv_nsec = (write_lock_wait_timeout_in_microseconds_LEFT % 1000000LL) * 1000LL};
			struct timespec stop_at = {.tv_sec = current_time.tv_sec + diff.tv_sec, .tv_nsec = current_time.tv_nsec + diff.tv_nsec};
			stop_at.tv_sec += stop_at.tv_nsec / 1000000000LL;
			stop_at.tv_nsec = stop_at.tv_nsec % 1000000000LL;

			// do timedwait
			wait_error = pthread_cond_timedwait(&(mt->write_lock_wait), &(mte->global_lock), &stop_at);
		}

		// substract elapsed time from write_lock_wait_timeout_in_microseconds_LEFT
		{
			// calculate current time after wait
			struct timespec current_time_after_wait;
			clock_gettime(CLOCK_REALTIME, &current_time_after_wait);

			// now calculate the time elapsed
			uint64_t microseconds_elapsed = (((int64_t)current_time_after_wait.tv_sec - (int64_t)current_time.tv_sec) * INT64_C(1000000))
			+ (((int64_t)current_time_after_wait.tv_nsec - (int64_t)current_time_after_wait.tv_nsec) / INT64_C(1000));

			// discard the time elapsed
			if(microseconds_elapsed > write_lock_wait_timeout_in_microseconds_LEFT)
				write_lock_wait_timeout_in_microseconds_LEFT = 0;
			else
				write_lock_wait_timeout_in_microseconds_LEFT -= microseconds_elapsed;
		}
	}

	// collect the result as, decrement_mini_transaction_reference_counter_UNSAFE() may destroy it
	int success = (mt->state == MIN_TX_COMPLETED);

	decrement_mini_transaction_reference_counter_UNSAFE(mte, mt);

	return success;
}

uint256 perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, void* page, uint64_t page_id)
{
	// if page size is same as block size, no full page write is required
	if(mte->stats.page_size == get_block_size_for_block_file(&(mte->database_block_file)))
		return INVALID_LOG_SEQUENCE_NUMBER;

	// a full page write is necessary only if pageLSN == INVALID_LOG_SEQUENCE_NUMBER OR pageLSN < checkpointLSN
	if(!(
		are_equal_uint256(get_pageLSN_for_page(page, &(mte->stats)), INVALID_LOG_SEQUENCE_NUMBER) ||
		compare_uint256(get_pageLSN_for_page(page, &(mte->stats)), mte->checkpointLSN) < 0)
	)
		return INVALID_LOG_SEQUENCE_NUMBER;

	// construct full page write log record, with writerLSN if it is not a free space mapper page
	log_record fpw_lr = {
		.type = FULL_PAGE_WRITE,
		.fpwlr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.writerLSN = INVALID_LOG_SEQUENCE_NUMBER,
			.page_contents = get_page_contents_for_page(page, page_id, &(mte->stats)),
		}
	};
	if(!is_free_space_mapper_page(page_id, &(mte->stats)))
		fpw_lr.fpwlr.writerLSN = get_writerLSN_for_page(page, &(mte->stats));

	// serialize full page write log record
	uint32_t serialized_fpw_lr_size = 0;
	const void* serialized_fpw_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &fpw_lr, &serialized_fpw_lr_size);
	if(serialized_fpw_lr == NULL)
		exit(-1);

	pthread_mutex_lock(&(mte->global_lock));

		wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_fpw_lr, serialized_fpw_lr_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
			exit(-1);

		// if mt->mini_transaction_id is INVALID, then assign it log_record_LSN. and make it a writer_mini_transaction
		if(are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
		{
			mt->mini_transaction_id = log_record_LSN;

			// remove mt from mte->reader_mini_transactions
			remove_from_linkedlist(&(mte->reader_mini_transactions), mt);

			// insert it to mte->writer_mini_transactions
			insert_in_hashmap(&(mte->writer_mini_transactions), mt);
		}

		// update lastLSN of the mini transaction
		mt->lastLSN = log_record_LSN;

		// set pageLSN of the page to the log_record_LSN
		set_pageLSN_for_page(page, log_record_LSN, &(mte->stats));

		// do not set writerLSN as we did not modify the contents of the page (we only modified pageLSN of the page)
		// so we are not entitled to take writerLSN on the page

		// mark the page as dirty in the bufferpool and dirty page table
		// we updated its pageLSN, this page is now considered dirty
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);

	pthread_mutex_unlock(&(mte->global_lock));

	// free full page write log record
	free((void*)serialized_fpw_lr);

	return log_record_LSN;
}