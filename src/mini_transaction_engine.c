#include<mini_transaction_engine.h>

#include<callbacks_bufferpool.h>
#include<callbacks_wale.h>
#include<wal_list_utils.h>

#define GLOBAL_PERIODIC_FLUSH_JOB_STATUS (periodic_flush_job_status){.frames_to_flush = 100, .period_in_milliseconds = 10}

int initialize_mini_transaction_engine(mini_transaction_engine* mte, const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint32_t bufferpool_frame_count, uint32_t wale_append_only_buffer_block_count, uint64_t checkpointing_period_in_miliseconds)
{
	if(mte->bufferpool_frame_count == 0 || mte->wale_append_only_buffer_block_count == 0)
		return 0;

	// initialize everything that does not need resource allocation first
	mte->database_file_name = database_file_name;
	pthread_mutex_init(&(mte->global_lock), NULL);
	mte->bufferpool_frame_count = bufferpool_frame_count;
	mte->bufferpool_frame_count_changed = 0;
	mte->wale_append_only_buffer_block_count = wale_append_only_buffer_block_count;
	mte->wale_append_only_buffer_block_count_changed = 0;
	mte->flushedLSN = INVALID_LOG_SEQUENCE_NUMBER;
	initialize_rwlock(&(mte->manager_lock), &(mte->global_lock));
	pthread_cond_init(&(mte->conditional_to_wait_for_execution_slot), NULL);
	mte->checkpointing_period_in_miliseconds = checkpointing_period_in_miliseconds;

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

		// initialize bufferpool
		if(!initialize_bufferpool(&(mte->bufferpool_handle), mte->bufferpool_frame_count, &(mte->global_lock), get_page_io_ops_for_bufferpool(&(mte->database_block_file), mte->stats.page_size, mte->stats.page_size), can_be_flushed_to_disk_for_bufferpool, was_flushed_to_disk_for_bufferpool, mte, GLOBAL_PERIODIC_FLUSH_JOB_STATUS))
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

		// initialize bufferpool
		if(!initialize_bufferpool(&(mte->bufferpool_handle), mte->bufferpool_frame_count, &(mte->global_lock), get_page_io_ops_for_bufferpool(&(mte->database_block_file), mte->stats.page_size, mte->stats.page_size), can_be_flushed_to_disk_for_bufferpool, was_flushed_to_disk_for_bufferpool, mte, GLOBAL_PERIODIC_FLUSH_JOB_STATUS))
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
	}
	else
		return 0;
/*
	// below three are the parts of mini_transaction table
	hashmap writer_mini_transactions; // mini_transaction_id != 0, state = IN_PROGRESS or UNDOING_FOR_ABORT else if state = ABORTED or COMMITTED then waiters_count > 0
	linkedlist reader_mini_transactions; // mini_transaction_id == 0, state = IN_PROGRESS

	linkedlist free_mini_transactions_list; // list of free mini transactions, new mini transactions are assigned from this list, here waiters_count must be 0

	// below two are the parts of dirty page table
	hashmap dirty_page_table;

	linkedlist free_dirty_page_entries_list; // list of free dirty page entries, new dirty page entrues are assigned from this lists or are allocated
*/
	initialize_log_record_tuple_defs(&(mte->lrtd), &(mte->stats));
	mte->user_stats = get_mini_transaction_engine_user_stats(&(mte->stats), get_block_size_for_block_file(&(mte->database_block_file)));

	return 1;
}