#include<mini_transaction_engine.h>

#include<mini_transaction_engine_util.h>

#include<dirty_page_table_entry.h>

#include<callbacks_bufferpool.h>
#include<callbacks_wale.h>
#include<wal_list_utils.h>
#include<mini_transaction_engine_checkpointer_util.h>
#include<mini_transaction_engine_recovery_util.h>

#define MINIMUM_CHECKPOINTER_PERIOD 5000000 // checkpoint period no lesser than 5 seconds

int initialize_mini_transaction_engine(mini_transaction_engine* mte, const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint32_t bufferpool_frame_count, uint32_t wale_append_only_buffer_block_count, uint64_t latch_wait_timeout_in_microseconds, uint64_t write_lock_wait_timeout_in_microseconds, uint64_t checkpointing_period_in_microseconds)
{
	// initialize everything that does not need resource allocation first
	mte->database_file_name = database_file_name;
	pthread_mutex_init(&(mte->global_lock), NULL);
	mte->bufferpool_frame_count = bufferpool_frame_count;
	mte->wale_append_only_buffer_block_count = wale_append_only_buffer_block_count;
	mte->flushedLSN = INVALID_LOG_SEQUENCE_NUMBER;
	mte->checkpointLSN = INVALID_LOG_SEQUENCE_NUMBER;
	initialize_rwlock(&(mte->manager_lock), &(mte->global_lock));
	pthread_cond_init(&(mte->conditional_to_wait_for_execution_slot), NULL);
	mte->latch_wait_timeout_in_microseconds = latch_wait_timeout_in_microseconds;
	mte->write_lock_wait_timeout_in_microseconds = write_lock_wait_timeout_in_microseconds;
	mte->checkpointing_period_in_microseconds = checkpointing_period_in_microseconds;
	pthread_cond_init(&(mte->wait_for_checkpointer_period), NULL);
	pthread_cond_init(&(mte->wait_for_checkpointer_to_stop), NULL);
	mte->is_checkpointer_running = 0;
	mte->shutdown_called = 0;
	pthread_mutex_init(&(mte->database_expansion_lock), NULL);
	mte->is_in_recovery_mode = 0;
	pthread_mutex_init(&(mte->recovery_mode_lock), NULL);

	// with less than 2 buffers in bufferpool you can not redo all types of log records
	// with less than 1 buffer in append only buffers of writer WALe, no log records can be appended
	if(mte->bufferpool_frame_count < 2 || mte->wale_append_only_buffer_block_count < 1)
		return 0;

	// latch and lock wait timeouts can not be 0, checkpointing period must be more than a second
	if(mte->latch_wait_timeout_in_microseconds == 0 || mte->write_lock_wait_timeout_in_microseconds == 0 || mte->checkpointing_period_in_microseconds < MINIMUM_CHECKPOINTER_PERIOD)
		return 0;

	int recovery_required = 0;

	if(open_block_file(&(mte->database_block_file), mte->database_file_name, O_DIRECT))
	{
		if(!read_from_first_block(&(mte->database_block_file), &(mte->stats)))
		{
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		if(mte->stats.page_size % get_block_size_for_block_file(&(mte->database_block_file)) ||
			(page_size != 0 && page_size != mte->stats.page_size) || (page_id_width != 0 && page_id_width != mte->stats.page_id_width) || (log_sequence_number_width != 0 && log_sequence_number_width != mte->stats.log_sequence_number_width))
		{
			printf("illegal parameters supplied\n");
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		// initialize user_stats
		mte->database_page_count = (get_total_size_for_block_file(&(mte->database_block_file)) - get_block_size_for_block_file(&(mte->database_block_file))) / mte->stats.page_size;
		mte->user_stats = get_mini_transaction_engine_user_stats(&(mte->stats), get_block_size_for_block_file(&(mte->database_block_file)));
		if(mte->database_page_count > mte->user_stats.max_page_count)
		{
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		// initialize bufferpool
		if(!initialize_bufferpool(&(mte->bufferpool_handle), mte->bufferpool_frame_count, &(mte->global_lock), get_page_io_ops_for_bufferpool(mte), can_be_flushed_to_disk_for_bufferpool, was_flushed_to_disk_for_bufferpool, mte, (periodic_flush_job_status){.frames_to_flush = (mte->bufferpool_frame_count * 0.3), .period_in_microseconds = (mte->latch_wait_timeout_in_microseconds * 50)}))
		{
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		// initialize wa_list
		if(!initialize_wal_list(mte))
		{
			deinitialize_bufferpool(&(mte->bufferpool_handle));
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		recovery_required = 1;
	}
	else if(create_and_open_block_file(&(mte->database_block_file), mte->database_file_name, O_DIRECT))
	{
		if(page_size % get_block_size_for_block_file(&(mte->database_block_file)) ||
			page_id_width == 0 || page_id_width > 8 || log_sequence_number_width == 0 || log_sequence_number_width > 32)
		{
			printf("illegal parameters supplied\n");
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		mte->stats.page_size = page_size;
		mte->stats.page_id_width = page_id_width;
		mte->stats.log_sequence_number_width = log_sequence_number_width;
		if(!write_to_first_block(&(mte->database_block_file), &(mte->stats)))
		{
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		// initialize user_stats
		mte->database_page_count = 0;
		mte->user_stats = get_mini_transaction_engine_user_stats(&(mte->stats), get_block_size_for_block_file(&(mte->database_block_file)));

		// initialize bufferpool
		if(!initialize_bufferpool(&(mte->bufferpool_handle), mte->bufferpool_frame_count, &(mte->global_lock), get_page_io_ops_for_bufferpool(mte), can_be_flushed_to_disk_for_bufferpool, was_flushed_to_disk_for_bufferpool, mte, (periodic_flush_job_status){.frames_to_flush = (mte->bufferpool_frame_count * 0.3), .period_in_microseconds = (mte->latch_wait_timeout_in_microseconds * 50)}))
		{
			close_block_file(&(mte->database_block_file));
			return 0;
		}

		// initialize wa_list
		if(!create_new_wal_list(mte))
		{
			deinitialize_bufferpool(&(mte->bufferpool_handle));
			close_block_file(&(mte->database_block_file));
			return 0;
		}
	}
	else
		return 0;

	if(!initialize_mini_transaction_table(&(mte->writer_mini_transactions), mte->bufferpool_frame_count))
	{
		printf("ISSUE :: unable to initialize an internal hashmap\n");
		exit(-1);
	}
	initialize_linkedlist(&(mte->reader_mini_transactions), offsetof(mini_transaction, enode));
	initialize_linkedlist(&(mte->free_mini_transactions_list), offsetof(mini_transaction, enode));

	if(!initialize_dirty_page_table(&(mte->dirty_page_table), mte->bufferpool_frame_count))
	{
		printf("ISSUE :: unable to initialize an internal hashmap\n");
		exit(-1);
	}
	initialize_linkedlist(&(mte->free_dirty_page_entries_list), offsetof(dirty_page_table_entry, enode));

	initialize_log_record_tuple_defs(&(mte->lrtd), &(mte->stats));

	if(recovery_required)
	{
		// put the mini transaction engine in recovery mode
		pthread_mutex_lock(&(mte->recovery_mode_lock));
		mte->is_in_recovery_mode = 1;
		pthread_mutex_unlock(&(mte->recovery_mode_lock));

		// all recovery function here
		recover(mte);

		// recovery may lead to creation of new pages so we update the database_page_count
		mte->database_page_count = (get_total_size_for_block_file(&(mte->database_block_file)) - get_block_size_for_block_file(&(mte->database_block_file))) / mte->stats.page_size;

		// remove the mini transaction engine from recovery mode
		pthread_mutex_lock(&(mte->recovery_mode_lock));
		mte->is_in_recovery_mode = 0;
		pthread_mutex_unlock(&(mte->recovery_mode_lock));
	}

	// allocate enough mini transaction structures to survive safely
	for(uint32_t i = 0; i < mte->bufferpool_frame_count; i++)
	{
		mini_transaction* mt = get_new_mini_transaction();
		insert_head_in_linkedlist(&(mte->free_mini_transactions_list), mt);
	}

	// dirty page table entries may not be created upfront

	// start checkpointer thread
	initialize_job(&(mte->checkpointer_job), checkpointer, mte, NULL, NULL);
	pthread_t thread_id;
	execute_job_async(&(mte->checkpointer_job), &thread_id);

	return 1;
}

void intermediate_wal_flush_for_mini_transaction_engine(mini_transaction_engine* mte)
{
	pthread_mutex_lock(&(mte->global_lock));
	shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));
}

void intermediate_bufferpool_flush_for_mini_transaction_engine(mini_transaction_engine* mte)
{
	pthread_mutex_lock(&(mte->global_lock));
	shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

	flush_all_possible_dirty_pages(&(mte->bufferpool_handle));

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));
}

void debug_print_wal_logs_for_mini_transaction_engine(mini_transaction_engine* mte)
{
	pthread_mutex_lock(&(mte->global_lock));
	shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

	uint256 t = ((wal_accessor*)get_front_of_arraylist(&(mte->wa_list)))->wale_LSNs_from;

	while(!are_equal_uint256(t, INVALID_LOG_SEQUENCE_NUMBER))
	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, t, &lr))
			break;

		printf("LSN : "); print_uint256(t); printf("\n");
		print_log_record(&lr, &(mte->stats));printf("\n");

		destroy_and_free_parsed_log_record(&lr);

		t = get_next_LSN_for_LSN_UNSAFE(mte, t);
	}

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));
}

void deinitialize_mini_transaction_engine(mini_transaction_engine* mte)
{
	pthread_mutex_lock(&(mte->global_lock));

	// below lock is WRITE_PREFERRING because we want the deinitialization to wait if a checkpointer is waiting for an exclusive lock on the manager_lock
	shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

	mte->shutdown_called = 1;

	// come out when both are empty
	while(!(is_empty_hashmap(&(mte->writer_mini_transactions)) && is_empty_linkedlist(&(mte->reader_mini_transactions))))
		pthread_cond_wait(&(mte->conditional_to_wait_for_execution_slot), &(mte->global_lock));

	// wake up checkpointer and wait for it to stop
	pthread_cond_signal(&(mte->wait_for_checkpointer_period));
	while(mte->is_checkpointer_running)
		pthread_cond_wait(&(mte->wait_for_checkpointer_to_stop), &(mte->global_lock));

	// flush wale
	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);

	// flush bufferpool
	flush_all_possible_dirty_pages(&(mte->bufferpool_handle));

	close_all_in_wal_list(&(mte->wa_list));

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	deinitialize_bufferpool(&(mte->bufferpool_handle));
	close_block_file(&(mte->database_block_file));

	pthread_cond_destroy(&(mte->conditional_to_wait_for_execution_slot));
	deinitialize_rwlock(&(mte->manager_lock));
	pthread_mutex_destroy(&(mte->global_lock));

	remove_all_from_linkedlist(&(mte->free_mini_transactions_list), AND_DELETE_MINI_TRANSACTIONS_NOTIFIER);
	remove_all_from_linkedlist(&(mte->reader_mini_transactions), AND_DELETE_MINI_TRANSACTIONS_NOTIFIER);

	remove_all_from_linkedlist(&(mte->free_dirty_page_entries_list), AND_DELETE_DIRTY_PAGE_TABLE_ENTRIES_NOTIFIER);

	remove_all_from_hashmap(&(mte->writer_mini_transactions), AND_DELETE_MINI_TRANSACTIONS_NOTIFIER);
	deinitialize_hashmap(&(mte->writer_mini_transactions));

	remove_all_from_hashmap(&(mte->dirty_page_table), AND_DELETE_DIRTY_PAGE_TABLE_ENTRIES_NOTIFIER);
	deinitialize_hashmap(&(mte->dirty_page_table));

	deinitialize_log_record_tuple_defs(&(mte->lrtd));

	pthread_mutex_destroy(&(mte->database_expansion_lock));

	pthread_mutex_destroy(&(mte->recovery_mode_lock));
}