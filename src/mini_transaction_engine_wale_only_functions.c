#include<mini_transaction_engine_wale_only_functions.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

#include<page_layout.h>

// the below function does the following for only a data page modification log record
/*
	1. logs log record to latest wale, gets the log_record_LSN for this record
	2. if mt->mini_transaction_id == INVALID, then assigns it and moves the mt to writer_mini_transactions and assigns its mini_transaction_id to log_record_LSN
	3. mt->lastLSN = log_record_LSN
	4. page->pageLSN = log_record_LSN
	5. page->writerLSN = mt->mini_transaction_id -> taking persistent write lock on the modified page
	6. mark it dirty in dirty page table and bufferpool both
	7. returns log_record_LSN of the log record we just logged
*/
// this function must be called with global lock and manager_lock held
static uint256 log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mini_transaction_engine* mte, const void* log_record, uint32_t log_record_size, mini_transaction* mt, void* page, uint64_t page_id)
{
	wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

	int wal_error = 0;
	uint256 log_record_LSN = append_log_record(wale_p, log_record, log_record_size, 0, &wal_error);
	if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
	{
		printf("ISSUE :: unable to append log record\n");
		exit(-1);
	}

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

	// set the writer LSN to the mini_transaction_id, (marking it as write locked)
	set_writerLSN_for_page(page, mt->mini_transaction_id, &(mte->stats));
	// since we got the write latch on the page, either we ourselves have locked the page OR it was not persistently locked by any one

	// mark the page as dirty in the bufferpool and dirty page table
	mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);

	return log_record_LSN;
}

int init_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, uint32_t page_header_size, const tuple_size_def* tpl_sz_d)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = PAGE_INIT,
		.pilr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.old_page_contents = page_contents,
			.new_page_header_size = page_header_size,
			.new_size_def = *tpl_sz_d,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = init_page(page_contents, mte->user_stats.page_size, page_header_size, tpl_sz_d);
	if(result)
		zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d); // doing this resets all free space bits on the page, this will help with compression

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

void set_page_header_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const void* hdr)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return ;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return ;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	uint32_t page_header_size = get_page_header_size(page_contents, mte->user_stats.page_size);
	void* old_page_header_contents = get_page_header(page_contents, mte->user_stats.page_size);

	// construct log record object
	log_record act_lr = {
		.type = PAGE_SET_HEADER,
		.pshlr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.old_page_header_contents = old_page_header_contents,
			.new_page_header_contents = hdr,
			.page_header_size = page_header_size,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	memory_move(old_page_header_contents, hdr, page_header_size);

	if(1)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return ;
}

int append_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, const void* external_tuple)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_APPEND,
		.talr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.new_tuple = external_tuple,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = append_tuple_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, external_tuple);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

int insert_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_INSERT,
		.tilr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.insert_index = index,
			.new_tuple = external_tuple,
		}
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = insert_tuple_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, index, external_tuple);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

int update_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_UPDATE,
		.tulr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.update_index = index,
			.old_tuple = get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, index),
			.new_tuple = external_tuple,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = update_tuple_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, index, external_tuple);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

int discard_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_DISCARD,
		.tdlr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.discard_index = index,
			.old_tuple = get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, index),
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = discard_tuple_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, index);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

void discard_all_tuples_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return ;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return ;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_DISCARD_ALL,
		.tdalr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.old_page_contents = page_contents,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	discard_all_tuples_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d);

	if(1)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return;
}

uint32_t discard_trailing_tomb_stones_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_DISCARD_TRAILING_TOMB_STONES,
		.tdttlr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.discarded_trailing_tomb_stones_count = get_trailing_tomb_stones_count_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d),
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	uint32_t result = discard_trailing_tomb_stones_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

int swap_tuples_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_SWAP,
		.tslr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.swap_index1 = i1,
			.swap_index2 = i2,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = swap_tuples_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d, i1, i2);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

int set_element_in_tuple_in_place_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const user_value* value)
{
	// if the tuple to be updated in place is NULL, fail
	const void* tuple_to_update_in_place = get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, &(tpl_d->size_def), tuple_index);
	if(tuple_to_update_in_place == NULL)
		return 0;
	user_value old_element = get_value_from_element_from_tuple(tpl_d, element_index, tuple_to_update_in_place);
	if(is_user_value_OUT_OF_BOUNDS(&old_element))
		return 0;

	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = TUPLE_UPDATE_ELEMENT_IN_PLACE,
		.tueiplr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.tpl_def = *tpl_d,
			.tuple_index = tuple_index,
			.element_index = element_index,
			.old_element = old_element,
			.new_element = *value,
		}
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int result = set_element_in_tuple_in_place_on_page(page_contents, mte->user_stats.page_size, tpl_d, tuple_index, element_index, value);

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

void clone_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, const void* page_contents_src)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return ;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return ;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = PAGE_CLONE,
		.pclr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
			.old_page_contents = page_contents,
			.new_page_contents = page_contents_src,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	clone_page(page_contents, mte->user_stats.page_size, tpl_sz_d, page_contents_src);

	if(1)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return ;
}

int run_page_compaction_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d)
{
	// grab manager_lock so manager threads do not enter while we are working
	// this must be a data page (as it is given by the user), so grab the page_id and actual page pointer
	pthread_mutex_lock(&(mte->global_lock));
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}
	pthread_mutex_unlock(&(mte->global_lock));

	// perform full page write if necessary, it will take global lock as it deems necessary considering manager_lock is held
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

	// construct log record object
	log_record act_lr = {
		.type = PAGE_COMPACTION,
		.pcptlr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.page_id = page_id,
			.size_def = *tpl_sz_d,
		},
	};

	// serialize log record object
	uint32_t serialized_act_lr_size = 0;
	const void* serialized_act_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &act_lr, &serialized_act_lr_size);
	if(serialized_act_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	// apply the actual operation
	int memory_allocation_error = 0;
	int result = run_page_compaction(page_contents, mte->user_stats.page_size, tpl_sz_d, &memory_allocation_error);
	if(memory_allocation_error)
	{
		printf("ISSUE :: memory allocation error while compacting the page\n");
		exit(-1);
	}
	if(result)
		zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, tpl_sz_d); // doing this resets all free space bits on the page, this will help with compression

	if(result)
	{
		// log the actual change log record
		pthread_mutex_lock(&(mte->global_lock));
			log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mte, serialized_act_lr, serialized_act_lr_size, mt, page, page_id);
		pthread_mutex_unlock(&(mte->global_lock));
	}

	// free the actual change log record
	free((void*)serialized_act_lr);

	// release manager lock and quit
	pthread_mutex_lock(&(mte->global_lock));
		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}