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

		// mark the page and free_space_mapper_page as dirty in the bufferpool and dirty page table
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, free_space_mapper_page, free_space_mapper_page_id);

		// recalculate page checksums, prior to releasing the latches
		pthread_mutex_unlock(&(mte->global_lock));
		recalculate_page_checksum(free_space_mapper_page, &(mte->stats));
		recalculate_page_checksum(page, &(mte->stats));
		pthread_mutex_lock(&(mte->global_lock));

		// this has to succeed, we already marked it dirty, so was_modified can be set to 0
		release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // was_modified = 0, force_flush = 0
		release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // was_modified = 0, force_flush = 0
	}
	pthread_mutex_unlock(&(mte->global_lock));

	// free the actual change log record
	free((void*)serialized_act_lr);

	return 1;
}


// below function always succeeds or exit(-1)
// it returns pointer to the page after success
// after this function returns you will still hold write latch on the page, but the write latch on the free space mapper page will be released
// this function does not check if the given page is free or not
// we also do not check if the page is write locked by self or none
// you must do the above two checks in your calling code
static void* allocate_page_holding_write_latch_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, void* free_space_mapper_page, uint64_t free_space_mapper_page_id, void* page, uint64_t page_id)
{
	// ensure correctness of the parameters
	if(!is_free_space_mapper_page(free_space_mapper_page_id, &(mte->stats)))
		exit(-1);
	if(is_free_space_mapper_page(page_id, &(mte->stats)))
		exit(-1);
	if(free_space_mapper_page_id != get_is_valid_bit_page_id_for_page(page_id, &(mte->stats)))
		exit(-1);

	// perform full page writes if necessary
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, free_space_mapper_page, free_space_mapper_page_id);

	// construct page_allocation log record
	log_record act_lr = {
		.type = PAGE_ALLOCATION,
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

	// set to 1 the free_space_mapper_bit_pos on the free_space_mapper_page
	{
		uint64_t free_space_mapper_bit_pos = get_is_valid_bit_position_for_page(page_id, &(mte->stats));
		void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
		set_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
	}

	// log the page allocation log record and manage state
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

		// mark the page and free_space_mapper_page as dirty in the bufferpool and dirty page table
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, free_space_mapper_page, free_space_mapper_page_id);

		// recalculate page checksums, prior to releasing the latches
		pthread_mutex_unlock(&(mte->global_lock));
		recalculate_page_checksum(free_space_mapper_page, &(mte->stats));
		pthread_mutex_lock(&(mte->global_lock));

		// this has to succeed, we already marked it dirty, so was_modified can be set to 0
		release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // was_modified = 0, force_flush = 0
	}
	pthread_mutex_unlock(&(mte->global_lock));

	// free the actual change log record
	free((void*)serialized_act_lr);

	return page;
}

void* allocate_page_without_database_expansion_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, uint64_t* page_id)
{
	// we are calling a free spacemapper page and group of pages following it an extent for the context of this function
	const uint64_t data_pages_per_extent = is_valid_bits_count_on_free_space_mapper_page(&(mte->stats));
	const uint64_t total_pages_per_extent = data_pages_per_extent + 1;

	uint64_t free_space_mapper_page_id = 0;
	while(free_space_mapper_page_id < mte->database_page_count)
	{
		{
			// write latch free space mapper page
			pthread_mutex_lock(&(mte->global_lock));
			void* free_space_mapper_page = acquire_page_with_writer_lock(&(mte->bufferpool_handle), free_space_mapper_page_id, mte->latch_wait_timeout_in_microseconds, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
			pthread_mutex_unlock(&(mte->global_lock));
			if(free_space_mapper_page == NULL) // could not lock free_space_mapper_page, so abort
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
				return NULL;
			}

			uint64_t free_space_mapper_bit_index = 0;
			while(free_space_mapper_bit_index < data_pages_per_extent)
			{
				// calculate respective page_id, and ensure that it does not overflow
				if(will_unsigned_sum_overflow(uint64_t, free_space_mapper_page_id, (free_space_mapper_bit_index + 1)))
					break;
				(*page_id) = free_space_mapper_page_id + (free_space_mapper_bit_index + 1);
				if((*page_id) >= mte->database_page_count)
					break;

				// if the free_space_mapper_bit_index is set, continue
				{
					const void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
					if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_index))
						continue;
				}

				{
					// write latch page at page_id
					pthread_mutex_lock(&(mte->global_lock));
					void* page = acquire_page_with_writer_lock(&(mte->bufferpool_handle), (*page_id), mte->latch_wait_timeout_in_microseconds, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
					if(page == NULL) // could not lock page at page_id, so abort
					{
						// no modifications were done, so no need to recalculate_checksum
						release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // was_modified = 0, force_flush = 0
						pthread_mutex_unlock(&(mte->global_lock));
						mt->state = MIN_TX_ABORTED;
						mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
						return NULL;
					}

					// if write locked by NULL or SELF, we are done
					mini_transaction* mt_locked_by = get_mini_transaction_that_last_persistent_write_locked_this_page_UNSAFE(mte, page);
					if(mt_locked_by == NULL || mt_locked_by == mt)
					{
						pthread_mutex_unlock(&(mte->global_lock));
						// allocate page and quit
						return allocate_page_holding_write_latch_INTERNAL(mte, mt, free_space_mapper_page, free_space_mapper_page_id, page, (*page_id));
					}

					// unlatch page at page_id
					// no modifications were done, so no need to recalculate_checksum
					release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // was_modified = 0, force_flush = 0
					pthread_mutex_unlock(&(mte->global_lock));
				}

				free_space_mapper_bit_index++;
			}

			// unlatch free space mapper page
			pthread_mutex_lock(&(mte->global_lock));
			// no modifications were done, so no need to recalculate_checksum
			release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // was_modified = 0, force_flush = 0
			pthread_mutex_unlock(&(mte->global_lock));
		}

		// check for overflow and increment
		if(will_unsigned_sum_overflow(uint64_t, free_space_mapper_page_id, total_pages_per_extent))
			break;
		free_space_mapper_page_id += total_pages_per_extent;
	}

	// we iterated through the entire database and found no page that can be safely allocated
	return NULL;
}

// either succeeds or aborts
// must be called with exclusive lock on manager lock held, global lock must not be held while calling this function
// it primarily appends a zero page to the database and appends a FULL_PAGE_WRITE log record for that page
// the initial contents of the page are set to content_template, if the content_template is NULL, we reset all bits on the page
// this function also fails if you have reached max_page_count available to the user
static int add_new_page_to_database_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, const void* content_template)
{
	// TODO
}

void* allocate_page_with_database_expansion_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, uint64_t* page_id)
{
	// TODO
}