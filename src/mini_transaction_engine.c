#include<mini_transaction_engine.h>

int initialize_mini_transaction_engine(mini_transaction_engine* mte, const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint32_t bufferpool_frame_count, uint32_t wale_append_only_buffer_block_count, uint64_t checkpointing_period_in_miliseconds)
{
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

	// bufferpool blockfile (located as database_file_name attribute above) and its handle object
	/*block_file bufferpool_block_file;
	bufferpool bufferpool_handle;

	// list of wal_accessor
	// the file name of each wale_block_file is database_file_name + "_logs/" + wale_LSNs_from
	arraylist wa_list;

	// tuple definitions for the log records handled by this engine
	log_record_tuple_defs lrtd;

	// below three are the parts of mini_transaction table
	hashmap writer_mini_transactions; // mini_transaction_id != 0, state = IN_PROGRESS or UNDOING_FOR_ABORT else if state = ABORTED or COMMITTED then waiters_count > 0
	linkedlist reader_mini_transactions; // mini_transaction_id == 0, state = IN_PROGRESS

	linkedlist free_mini_transactions_list; // list of free mini transactions, new mini transactions are assigned from this list, here waiters_count must be 0

	// below two are the parts of dirty page table
	hashmap dirty_page_table;

	linkedlist free_dirty_page_entries_list; // list of free dirty page entries, new dirty page entrues are assigned from this lists or are allocated

	// new threads attempting to start a new mini transaction execution wait here until a slot is available
	// a signal will be called everytime an insert is performed on free_mini_transactions_list
	pthread_cond_t conditional_to_wait_for_execution_slot;

	// stats for internal use
	mini_transaction_engine_stats stats;

	// stats to be used by user
	mini_transaction_engine_user_stats user_stats;*/
}