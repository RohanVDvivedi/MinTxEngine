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
	const uint64_t write_lock_wait_timeout_in_microseconds = 300;

	// no one will make this mini transaction free as you just incremented it reference counter
	mt->reference_counter++;

	int wait_error = 0;
	uint64_t write_lock_wait_timeout_in_microseconds_LEFT = write_lock_wait_timeout_in_microseconds;
	while(mt->state != MIN_TX_COMPLETED && !wait_error)
	{
		struct timespec current_time;
		clock_gettime(CLOCK_REALTIME, &current_time);

		unsigned long long int secs = write_lock_wait_timeout_in_microseconds_LEFT / 1000000;
		unsigned long long int nano_secs_extra = (write_lock_wait_timeout_in_microseconds_LEFT % 1000000) * 1000;

		struct timespec wait_till = {.tv_sec = (current_time.tv_sec + secs), .tv_nsec = (current_time.tv_nsec + nano_secs_extra)};

		// do timedwait on job_queue_empty_wait, while releasing job_queue_mutex, while we wait
		wait_error = pthread_cond_timedwait(&(mt->write_lock_wait), &(mte->global_lock), &wait_till);

		// calculate current time after wait
		struct timespec current_time_after_wait;
		clock_gettime(CLOCK_REALTIME, &current_time_after_wait);

		// now calculate the time elapsed
		uint64_t microseconds_elapsed = (((int64_t)current_time_after_wait.tv_sec - (int64_t)current_time.tv_sec) * INT64_C(1000000))
		+ (((int64_t)current_time_after_wait.tv_nsec - (int64_t)current_time_after_wait.tv_nsec) / INT64_C(1000));

		// discard the time used
		if(microseconds_elapsed > write_lock_wait_timeout_in_microseconds_LEFT)
			write_lock_wait_timeout_in_microseconds_LEFT = 0;
		else
			write_lock_wait_timeout_in_microseconds_LEFT -= microseconds_elapsed;
	}

	// collect the result as, decrement_mini_transaction_reference_counter_UNSAFE() may destroy it
	int success = (mt->state == MIN_TX_COMPLETED);

	decrement_mini_transaction_reference_counter_UNSAFE(mte, mt);

	return success;
}