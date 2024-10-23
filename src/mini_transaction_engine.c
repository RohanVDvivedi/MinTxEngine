#include<mini_transaction_engine.h>

#include<callbacks_bufferpool.h>
#include<callbacks_wale.h>
#include<wal_list_utils.h>

#define GLOBAL_PERIODIC_FLUSH_JOB_STATUS (periodic_flush_job_status){.frames_to_flush = 100, .period_in_milliseconds = 10}

int initialize_mini_transaction_engine(mini_transaction_engine* mte, const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint32_t bufferpool_frame_count, uint32_t wale_append_only_buffer_block_count, uint64_t write_lock_wait_timeout_in_microseconds, uint64_t checkpointing_period_in_miliseconds)
{
	if(mte->bufferpool_frame_count == 0 || mte->wale_append_only_buffer_block_count == 0)
		return 0;

	// initialize everything that does not need resource allocation first
	mte->database_file_name = database_file_name;
	pthread_mutex_init(&(mte->global_lock), NULL);
	mte->bufferpool_frame_count = bufferpool_frame_count;
	mte->wale_append_only_buffer_block_count = wale_append_only_buffer_block_count;
	mte->flushedLSN = INVALID_LOG_SEQUENCE_NUMBER;
	mte->checkpointLSN = INVALID_LOG_SEQUENCE_NUMBER;
	initialize_rwlock(&(mte->manager_lock), &(mte->global_lock));
	pthread_cond_init(&(mte->conditional_to_wait_for_execution_slot), NULL);
	mte->write_lock_wait_timeout_in_microseconds = write_lock_wait_timeout_in_microseconds;
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

	if(!initialize_hashmap(&(mte->writer_mini_transactions), ELEMENTS_AS_LINKEDLIST_INSERT_AT_TAIL, mte->bufferpool_frame_count, &simple_hasher(hash_mini_transaction), &simple_comparator(compare_mini_transactions), offsetof(mini_transaction, enode)))
		exit(-1);
	initialize_linkedlist(&(mte->reader_mini_transactions), offsetof(mini_transaction, enode));
	initialize_linkedlist(&(mte->free_mini_transactions_list), offsetof(mini_transaction, enode));
	for(uint32_t i = 0; i < mte->bufferpool_frame_count; i++)
	{
		mini_transaction* mt = get_new_mini_transaction();
		insert_head_in_linkedlist(&(mte->free_mini_transactions_list), mt);
	}

	if(!initialize_hashmap(&(mte->dirty_page_table), ELEMENTS_AS_LINKEDLIST_INSERT_AT_TAIL, mte->bufferpool_frame_count, &simple_hasher(hash_dirty_page_table_entry), &simple_comparator(compare_dirty_page_table_entries), offsetof(dirty_page_table_entry, enode)))
		exit(-1);
	initialize_linkedlist(&(mte->free_dirty_page_entries_list), offsetof(dirty_page_table_entry, enode));
	for(uint32_t i = 0; i < mte->bufferpool_frame_count; i++)
	{
		dirty_page_table_entry* dpte = get_new_dirty_page_table_entry();
		insert_head_in_linkedlist(&(mte->free_dirty_page_entries_list), dpte);
	}

	initialize_log_record_tuple_defs(&(mte->lrtd), &(mte->stats));
	mte->user_stats = get_mini_transaction_engine_user_stats(&(mte->stats), get_block_size_for_block_file(&(mte->database_block_file)));

	// TODO call recovery functions here

	return 1;
}