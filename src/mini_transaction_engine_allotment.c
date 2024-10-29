#include<mini_transaction_engine_allotment.h>

mini_transaction* mte_allot_mini_tx(mini_transaction_engine* mte, uint64_t wait_timeout_in_microseconds)
{
	pthread_mutex_lock(&(mte->global_lock));

	int wait_error = 0;
	while(is_empty_linkedlist(&(mte->free_mini_transactions_list)) && !wait_error) // and not a shutdown
	{
		// get current time
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);

		{
			// compute the time to stop at
			struct timespec diff = {.tv_sec = (wait_timeout_in_microseconds / 1000000LL), .tv_nsec = (wait_timeout_in_microseconds % 1000000LL) * 1000LL};
			struct timespec stop_at = {.tv_sec = now.tv_sec + diff.tv_sec, .tv_nsec = now.tv_nsec + diff.tv_nsec};
			stop_at.tv_sec += stop_at.tv_nsec / 1000000000LL;
			stop_at.tv_nsec = stop_at.tv_nsec % 1000000000LL;

			// wait until atmost stop_at
			pthread_cond_timedwait(&(mte->conditional_to_wait_for_execution_slot), &(mte->global_lock), &stop_at);
		}

		{
			// compute the current time after wait is over
			struct timespec then;
			clock_gettime(CLOCK_REALTIME, &then);

			uint64_t microsecond_elapsed = ((int64_t)then.tv_sec - (int64_t)now.tv_sec) * 1000000LL + (((int64_t)then.tv_nsec - (int64_t)now.tv_nsec) / 1000LL);

			if(microsecond_elapsed > wait_timeout_in_microseconds)
				wait_timeout_in_microseconds = 0;
			else
				wait_timeout_in_microseconds -= microsecond_elapsed;
		}
	}

	mini_transaction* mt = NULL;
	if(!is_empty_linkedlist(&(mte->free_mini_transactions_list))) // and not a shutdown
	{
		// if there is a free mini_transaction then grab it
		mt = (mini_transaction*) get_head_of_linkedlist(&(mte->free_mini_transactions_list));
		remove_head_from_linkedlist(&(mte->free_mini_transactions_list));

		mt->mini_transaction_id = INVALID_LOG_SEQUENCE_NUMBER;
		mt->lastLSN = INVALID_LOG_SEQUENCE_NUMBER;
		mt->state = MIN_TX_IN_PROGRESS;
		mt->abort_error = 0;

		// in the begining every mini transaction is a reader_mini_transaction
		insert_head_in_linkedlist(&(mte->reader_mini_transactions), mt);
	}

	pthread_mutex_unlock(&(mte->global_lock));

	return mt;
}

static void append_abortion_log_record_and_flush_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt)
{
	wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

	{
		log_record lr = {
			.type = ABORT_MINI_TX,
			.amtlr = {
				.mini_transaction_id = mt->mini_transaction_id,
				.prev_log_record_LSN = mt->lastLSN,
			},
		};

		// serialize log record
		uint32_t serialized_lr_size = 0;
		const void* serialized_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_lr_size);
		if(serialized_lr == NULL)
			exit(-1);

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_lr, serialized_lr_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
			exit(-1);

		free((void*)serialized_lr);

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
	}

	{
		int wal_error = 0;
		uint256 flushedLSN = flush_all_log_records(wale_p, &wal_error);
		if(are_equal_uint256(flushedLSN, INVALID_LOG_SEQUENCE_NUMBER))
			exit(-1);

		mte->flushedLSN = max_uint256(mte->flushedLSN, flushedLSN);
	}
}

static void append_completion_log_record_and_flush_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt, const void* complete_info, uint32_t complete_info_size)
{
	wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

	{
		log_record lr = {
			.type = COMPLETE_MINI_TX,
			.cmtlr = {
				.mini_transaction_id = mt->mini_transaction_id,
				.prev_log_record_LSN = mt->lastLSN,
				.info = complete_info,
				.info_size = complete_info_size,
			},
		};

		// serialize log record
		uint32_t serialized_lr_size = 0;
		const void* serialized_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_lr_size);
		if(serialized_lr == NULL)
			exit(-1);

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_lr, serialized_lr_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
			exit(-1);

		free((void*)serialized_lr);

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
	}

	{
		int wal_error = 0;
		uint256 flushedLSN = flush_all_log_records(wale_p, &wal_error);
		if(are_equal_uint256(flushedLSN, INVALID_LOG_SEQUENCE_NUMBER))
			exit(-1);

		mte->flushedLSN = max_uint256(mte->flushedLSN, flushedLSN);
	}
}

#include<mini_transaction_engine_util.h>

void mte_complete_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, const void* complete_info, uint32_t complete_info_size)
{
	pthread_mutex_lock(&(mte->global_lock));

	if(mt->state == MIN_TX_IN_PROGRESS)
	{
		// if it is a successfull writer mini transaction (i.e. has a mini transaction id), then append a complete mini transaction log record and flush all log records to make them persistent
		if(!are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
			append_completion_log_record_and_flush_UNSAFE(mte, mt, complete_info, complete_info_size);

		mt->state = MIN_TX_COMPLETED;
		decrement_mini_transaction_reference_counter_UNSAFE(mte, mt);
		pthread_mutex_unlock(&(mte->global_lock));
		return ;
	}



	pthread_mutex_unlock(&(mte->global_lock));
}