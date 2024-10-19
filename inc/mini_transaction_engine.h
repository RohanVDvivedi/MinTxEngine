#ifndef MINI_TRANSACTION_ENGINE_H
#define MINI_TRANSACTION_ENGINE_H

#include<block_io.h>

#include<hashmap.h>
#include<linkedlist.h>
#include<arraylist.h>

#include<rwlock.h>

#include<wale.h>
#include<bufferpool.h>

#include<mini_transaction_engine_stats.h>
#include<log_record.h>

#include<mini_transaction.h>
#include<dirty_page_table_entry.h>

/*
	A single minitransaction must comrpise of a single operation using a single thread
	You may never use multiple threads for a minitransaction
	Each thread must get its own mini transaction slot and must operate only on atmost 1 logical disk resident data structure using a single thread to avoid deadlocks
	Yet, you can get concurrency by using multiple concurrent mini_transactions.
*/

// both the accessor structs below must be created over malloc-ed memory and 

typedef struct wal_accessor wal_accessor;
struct wal_accessor
{
	uint256 wale_LSNs_from; // first LSN that can be found in this file, this is also the name of the wal file in decimal
	block_file wale_block_file;
	wale wale_handle;
};

typedef struct mini_transaction_engine mini_transaction_engine;
struct mini_transaction_engine
{
	// this string is never allocated it is preserved here as it was passed as parameter in the initialization function
	const char* database_file_name;
	// database_block_file (located as database_file_name attribute above)
	block_file database_block_file;

	// global lock for the bufferpool_p and wales
	// also for mini_transaction table
	pthread_mutex_t global_lock;

	// bufferpool handle object
	bufferpool bufferpool_handle;

	// internal caching parameter for bufferpool
	uint32_t bufferpool_frame_count; int bufferpool_frame_count_changed; // flag set if bufferpool frame counter needs to be updated

	// list of wal_accessor
	// the file name of each wale_block_file is database_file_name + "_logs/" + wale_LSNs_from
	arraylist wa_list;

	// internal caching parameter for wale
	uint32_t wale_append_only_buffer_block_count; int wale_append_only_buffer_block_count_changed; // flag set if wale's append only buffer counter needs to be updated

	// this variable is to be updated as per the rules defined here
	// flushedLSN = max(flushedLSN, flush_all_log_records(wa_list.last().wale_handle));
	// this is because we do not have a globally consistent view of the flushedLSN because global lock ges released while retrieveing it, and call to flushing log records will not ensure the return in any correct order, as global lock is released also while flushing the log records
	// we can also not rely on the flushed LSN of the most recent wale_handle because if there were no log records writeen then it might even be 0, i.e. invalid
	// if you are starting a new database it must be set to INVALID_LOG_SEQUENCE_NUMBER i.e. 0
	uint256 flushedLSN;

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

	// manger_lock is to be held in shared mode for all the accesses by the user
	// it must be held in exclusive mode for truncating WALe and Bufferpool files, check pointing etc
	rwlock manager_lock;

	// as the name suggests check pointing is done every this many milliseconds
	uint64_t checkpointing_period_in_miliseconds;

	// stats for internal use
	mini_transaction_engine_stats stats;

	// stats to be used by user
	mini_transaction_engine_user_stats user_stats;
};

// page_size, page_id_width and log_sequence_number_width parameter is only used if passed as non-zero
// else they are either used for a new database OR are ensured to be correct for an existing database if non-zero
int initialize_mini_transaction_engine(mini_transaction_engine* mte, const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint32_t bufferpool_frame_count, uint32_t wale_append_only_buffer_block_count, uint64_t checkpointing_period_in_miliseconds);

#endif