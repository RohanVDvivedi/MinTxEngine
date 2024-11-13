#include<mini_transaction_engine_checkpointer_util.h>

#include<mini_transaction_engine_util.h>

#include<log_record.h>

void print_checkpoint(const checkpoint* ckpt)
{
	printf("mini_transaction_table : \n");
	for(const mini_transaction* mt = get_first_of_in_hashmap(&(ckpt->mini_transaction_table), FIRST_OF_HASHMAP); mt != NULL; mt = get_next_of_in_hashmap(&(ckpt->mini_transaction_table), mt, ANY_IN_HASHMAP))
	{
		printf("mini_transaction_id : ");
		print_uint256(mt->mini_transaction_id);
		printf(" \t lastLSN : ");
		print_uint256(mt->lastLSN);
		printf(" \t state : %d \t abort_error : %d\n", mt->state, mt->abort_error);
	}
	printf("\n");

	printf("dirty_page_table : \n");
	for(const dirty_page_table_entry* dpte = get_first_of_in_hashmap(&(ckpt->dirty_page_table), FIRST_OF_HASHMAP); dpte != NULL; dpte = get_next_of_in_hashmap(&(ckpt->dirty_page_table), dpte, ANY_IN_HASHMAP))
	{
		printf("page_id : %"PRIu64" \t recLSN : ", dpte->page_id);
		print_uint256(dpte->recLSN);
		printf("\n");
	}
	printf("\n");
}

uint256 read_checkpoint_from_wal_UNSAFE(mini_transaction_engine* mte, uint256 checkpointLSN, checkpoint* ckpt)
{
	{
		if(!initialize_mini_transaction_table(&(ckpt->mini_transaction_table), 3))
		{
			printf("ISSUE :: unable to initialize an checkpoint hashmap\n");
			exit(-1);
		}

		if(!initialize_dirty_page_table(&(ckpt->dirty_page_table), 3))
		{
			printf("ISSUE :: unable to initialize an checkpoint hashmap\n");
			exit(-1);
		}
	}

	uint256 read_at = checkpointLSN;
	uint256 begin_LSN = INVALID_LOG_SEQUENCE_NUMBER;

	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, read_at, &lr))
		{
			printf("ISSUE :: could not read checkpoint end log record\n");
			exit(-1);
		}

		if(lr.type != CHECKPOINT_END)
		{
			printf("ISSUE :: expected a checkpoint end log record type, but found something else, possibly a bug in mini transaction engine\n");
			exit(-1);
		}

		read_at = get_prev_log_record_LSN_for_log_record(&lr);

		// initialize begin_LSN
		begin_LSN = lr.ckptelr.begin_LSN;

		destroy_and_free_parsed_log_record(&lr);
	}

	// read while read_at != INVALID_LOG_SEQUENCE_NUMBER
	while(!are_equal_uint256(read_at, INVALID_LOG_SEQUENCE_NUMBER))
	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, read_at, &lr))
		{
			printf("ISSUE :: could not read checkpoint end log record\n");
			exit(-1);
		}

		if(lr.type == CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY)
		{
			// checkpoint must never contain a completed mini transaction
			if(lr.ckptmttelr.mt.state == MIN_TX_COMPLETED)
			{
				printf("ISSUE :: checkpoint being read contains a completed mini transaction entry\n");
				exit(-1);
			}

			mini_transaction* mt = get_new_mini_transaction();

			mt->mini_transaction_id = lr.ckptmttelr.mt.mini_transaction_id;
			mt->lastLSN = lr.ckptmttelr.mt.lastLSN;
			mt->state = lr.ckptmttelr.mt.state;
			mt->abort_error = lr.ckptmttelr.mt.abort_error;

			insert_in_hashmap(&(ckpt->mini_transaction_table), mt);

			if(get_element_count_hashmap(&(ckpt->mini_transaction_table)) / 3 > get_bucket_count_hashmap(&(ckpt->mini_transaction_table)))
				expand_hashmap(&(ckpt->mini_transaction_table), 1.5);
		}
		else if(lr.type == CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY)
		{
			dirty_page_table_entry* dpte = get_new_dirty_page_table_entry();

			dpte->page_id = lr.ckptdptelr.dpte.page_id;
			dpte->recLSN = lr.ckptdptelr.dpte.recLSN;

			insert_in_hashmap(&(ckpt->dirty_page_table), dpte);

			if(get_element_count_hashmap(&(ckpt->dirty_page_table)) / 3 > get_bucket_count_hashmap(&(ckpt->dirty_page_table)))
				expand_hashmap(&(ckpt->dirty_page_table), 1.5);
		}
		else
		{
			printf("ISSUE :: expected a checkpoint entry log record type, but found something else, possibly a bug in mini transaction engine\n");
			exit(-1);
		}

		// if begin_LSN == read_at -> i.e. it is the oldest log record of the checkpoint
		// and the previous log record of this checkpoint is not INVALID, then the checkpoint is probably corrupted
		if(are_equal_uint256(begin_LSN, read_at) && !are_equal_uint256(get_prev_log_record_LSN_for_log_record(&lr), INVALID_LOG_SEQUENCE_NUMBER))
		{
			printf("ISSUE :: begin_LSN not the checkpoint's first log record, probably a bug in mini transaction engine\n");
			exit(-1);
		}

		read_at = get_prev_log_record_LSN_for_log_record(&lr);

		destroy_and_free_parsed_log_record(&lr);
	}

	return begin_LSN;
}

// NOT will not release global lock while serializing checkpoint log record, I thing it it use less
// there will not be a single thread acting on the mini transaction engine while we are checkpointing
static uint256 append_checkpoint_to_wal_UNSAFE(mini_transaction_engine* mte, const checkpoint* ckpt, uint256* begin_LSN)
{
	(*begin_LSN) = INVALID_LOG_SEQUENCE_NUMBER;
	uint256 lastLSN = INVALID_LOG_SEQUENCE_NUMBER;

	wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

	for(const mini_transaction* mt = get_first_of_in_hashmap(&(ckpt->mini_transaction_table), FIRST_OF_HASHMAP); mt != NULL; mt = get_next_of_in_hashmap(&(ckpt->mini_transaction_table), mt, ANY_IN_HASHMAP))
	{
		// skip all completed mini transactions
		if(mt->state == MIN_TX_COMPLETED)
			continue;

		log_record lr = {
			.type = CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY,
			.ckptmttelr = {
				.prev_log_record_LSN = lastLSN,
				.mt = (*mt),
			},
		};

		uint32_t serialized_log_record_size = 0;
		const void* serialized_log_record = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_log_record_size);
		if(serialized_log_record == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_log_record, serialized_log_record_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		if(are_equal_uint256((*begin_LSN), INVALID_LOG_SEQUENCE_NUMBER))
			(*begin_LSN) = log_record_LSN;
		lastLSN = log_record_LSN;

		free((void*)serialized_log_record);
	}

	for(const dirty_page_table_entry* dpte = get_first_of_in_hashmap(&(ckpt->dirty_page_table), FIRST_OF_HASHMAP); dpte != NULL; dpte = get_next_of_in_hashmap(&(ckpt->dirty_page_table), dpte, ANY_IN_HASHMAP))
	{
		log_record lr = {
			.type = CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY,
			.ckptdptelr = {
				.prev_log_record_LSN = lastLSN,
				.dpte = (*dpte),
			},
		};

		uint32_t serialized_log_record_size = 0;
		const void* serialized_log_record = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_log_record_size);
		if(serialized_log_record == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_log_record, serialized_log_record_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		if(are_equal_uint256((*begin_LSN), INVALID_LOG_SEQUENCE_NUMBER))
			(*begin_LSN) = log_record_LSN;
		lastLSN = log_record_LSN;

		free((void*)serialized_log_record);
	}

	uint256 checkpointLSN = INVALID_LOG_SEQUENCE_NUMBER;
	{
		log_record lr = {
			.type = CHECKPOINT_END,
			.ckptelr = {
				.prev_log_record_LSN = lastLSN,
				.begin_LSN = (*begin_LSN),
			},
		};

		uint32_t serialized_log_record_size = 0;
		const void* serialized_log_record = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_log_record_size);
		if(serialized_log_record == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		int wal_error = 0;
		checkpointLSN = append_log_record(wale_p, serialized_log_record, serialized_log_record_size, 1, &wal_error); // only this log record is marked as checkpoint log record
		if(are_equal_uint256(checkpointLSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		if(are_equal_uint256((*begin_LSN), INVALID_LOG_SEQUENCE_NUMBER))
			(*begin_LSN) = checkpointLSN;
		lastLSN = checkpointLSN;

		free((void*)serialized_log_record);
	}

	return checkpointLSN;
}

#include<wal_list_utils.h>

static void perform_checkpoint_UNSAFE(mini_transaction_engine* mte)
{
	// flush wal logs, no one will be woken up, as no one is waiting, since we are here with an exclusive lock
	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);

	// flushedLSN - checkpointLSN < mte->checkpointing_LSN_diff_in_bytes
	// then there are not enough log records to be checkpointed
	{
		uint256 lsn_diff;
		sub_uint256(&lsn_diff, mte->flushedLSN, mte->checkpointLSN);
		if(compare_uint256(lsn_diff, get_uint256(mte->checkpointing_LSN_diff_in_bytes)) < 0)
			return;
	}

	// get old status of the periodic flush job to be reverted to
	// and then shut periodic flush job, and wait for it to finish
	periodic_flush_job_status old_state = get_periodic_flush_job_status(&(mte->bufferpool_handle));
	modify_periodic_flush_job_status(&(mte->bufferpool_handle), STOP_PERIODIC_FLUSH_JOB_STATUS);
	wait_for_periodic_flush_job_to_stop(&(mte->bufferpool_handle));

	// flush bufferpool, reducing the number of dirty pages
	flush_all_possible_dirty_pages(&(mte->bufferpool_handle));

	// -------------- MANAGEMENT TASK : create a new wal file if the last one exceed the max allowable size
	{
		if(get_total_size_for_block_file(&(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_block_file)) > mte->max_wal_file_size_in_bytes)
		{
			if(!create_newest_in_wal_list_UNSAFE(mte))
			{
				printf("ISSUE :: failed to create a new wal file while checkpointing\n");
				exit(-1);
			}
		}
	}

	// appending checkpoint log record
	uint256 checkpoint_begin_LSN;
	mte->checkpointLSN = append_checkpoint_to_wal_UNSAFE(mte, &(const checkpoint){.mini_transaction_table = mte->writer_mini_transactions, .dirty_page_table = mte->dirty_page_table}, &checkpoint_begin_LSN);

	// flush checkpointer wal logs, no one will be woken up, as no one is waiting, since we are here with an exclusive lock
	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);

	// -------------- MANAGEMENT TASK : destroy the old wale files, which are no longer being referenced
	{
		// calculate oldest LSN that is visible, begin LSN of the checkpoint is ofcourse visible and is never INVALID
		uint256 oldest_visible_LSN = checkpoint_begin_LSN;

		{
			uint256 min_mini_transaction_id = get_minimum_mini_transaction_id_for_mini_transaction_table(&(mte->writer_mini_transactions));
			if(!are_equal_uint256(min_mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
				oldest_visible_LSN = min_uint256(oldest_visible_LSN, min_mini_transaction_id);
		}

		{
			uint256 min_recLSN = get_minimum_recLSN_for_dirty_page_table(&(mte->dirty_page_table));
			if(!are_equal_uint256(min_recLSN, INVALID_LOG_SEQUENCE_NUMBER))
				oldest_visible_LSN = min_uint256(oldest_visible_LSN, min_recLSN);
		}

		// get the index of wa_list where the oldest visible LSN resides
		cy_uint wa_list_index = find_relevant_from_wal_list_UNSAFE(&(mte->wa_list), oldest_visible_LSN);
		if(wa_list_index != INVALID_INDEX)
		{
			// discard all old wal files
			cy_uint wals_to_discard = wa_list_index;
			while(wals_to_discard > 0)
			{
				if(!drop_oldest_from_wal_list_UNSAFE(mte))
				{
					printf("ISSUE :: failed to discard an old wal file while checkpointing\n");
					exit(-1);
				}
				wals_to_discard--;
			}
		}
		else  // oldest visible LSN belongs to a non existing wal file, this is possibly a bug but only the user needs to care
		{
			printf("ISSUE :: oldest visible lsn calcualted for the checkpoint is already discarded, terrible implementation by the user\n");
		}
	}

	// -------------- MANAGEMENT TASK : find all trailing database file pages that are not allocated OR are free space mapper pages and truncate the database file
	{

	}

	// bufferpool may still have these trailing pages that we just discarded using the truncate call, but since they were already flushed to disk WE DO NOT CARE

	// start the periodic flush job at the prior state
	modify_periodic_flush_job_status(&(mte->bufferpool_handle), old_state);
}

#include<errno.h>

void* checkpointer(void* mte_vp)
{
	mini_transaction_engine* mte = mte_vp;

	pthread_mutex_lock(&(mte->global_lock));
	mte->is_checkpointer_running = 1;
	pthread_mutex_unlock(&(mte->global_lock));

	while(1)
	{
		pthread_mutex_lock(&(mte->global_lock));

		while(!mte->shutdown_called)
		{
			struct timespec now;
			clock_gettime(CLOCK_REALTIME, &now);
			struct timespec diff = {.tv_sec = (mte->checkpointing_period_in_microseconds / 1000000LL), .tv_nsec = (mte->checkpointing_period_in_microseconds % 1000000LL) * 1000LL};
			struct timespec stop_at = {.tv_sec = now.tv_sec + diff.tv_sec, .tv_nsec = now.tv_nsec + diff.tv_nsec};
			stop_at.tv_sec += stop_at.tv_nsec / 1000000000LL;
			stop_at.tv_nsec = stop_at.tv_nsec % 1000000000LL;
			if(ETIMEDOUT == pthread_cond_timedwait(&(mte->wait_for_checkpointer_period), &(mte->global_lock), &stop_at))
				break;
		}

		if(mte->shutdown_called)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			break;
		}

		// perform checkpoint
		exclusive_lock(&(mte->manager_lock), BLOCKING);

		perform_checkpoint_UNSAFE(mte);

		exclusive_unlock(&(mte->manager_lock));

		pthread_mutex_unlock(&(mte->global_lock));
	}

	pthread_mutex_lock(&(mte->global_lock));
	mte->is_checkpointer_running = 0;
	pthread_cond_broadcast(&(mte->wait_for_checkpointer_to_stop));
	pthread_mutex_unlock(&(mte->global_lock));
	return NULL;
}