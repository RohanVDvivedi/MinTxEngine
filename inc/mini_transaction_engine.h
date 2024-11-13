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

#include<abort_errors_list.h>

/*
	A single minitransaction must comrpise of a single operation using a single thread
	You may never use multiple threads for a minitransaction
	Each thread must get its own mini transaction slot and must operate only on atmost 1 logical disk resident data structure using a single thread to avoid deadlocks
	Yet, you can get concurrency by using multiple concurrent mini_transactions.
*/

// both the accessor structs below must be created over malloc-ed memory and 

// first LSN of any database is 7
#define FIRST_LOG_SEQUENCE_NUMBER get_uint256(7)

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
	uint32_t bufferpool_frame_count;

	// list of wal_accessor
	// the file name of each wale_block_file is database_file_name + "_logs/" + wale_LSNs_from
	arraylist wa_list;

	// internal caching parameter for wale
	uint32_t wale_append_only_buffer_block_count;

	// these variable is to be updated as per the rules defined here
	// flushedLSN = max(flushedLSN, flush_all_log_records(wa_list.last().wale_handle));
	// this is because we do not have a globally consistent view of the flushedLSN and checkpoint*LSN because global lock gets released while retrieveing it, and call to flushing log records will not ensure the return in any correct order, as global lock is released also while flushing the log records
	// we can also not rely on the flushed LSN of the most recent wale_handle because if there were no log records writeen then it might even be 0, i.e. invalid
	uint256 flushedLSN;
	uint256 checkpointLSN; // end of the latest checkpoint

	// this is the number of pages in database that are in use
	// this must be lesser than or equal to user_stats.max_page_count
	uint64_t database_page_count;
	// database_page_count, refers to in-memory copy of the page count in the database_block_file
	// as you guessed we need a exclusive lock on the manager_lock to extend/truncate the file

	// tuple definitions for the log records handled by this engine
	log_record_tuple_defs lrtd;

	// below three are the parts of mini_transaction table
	hashmap writer_mini_transactions; // mini_transaction_id != 0, state = IN_PROGRESS or UNDOING_FOR_ABORT else if state = ABORTED or COMMITTED then waiters_count > 0
	linkedlist reader_mini_transactions; // mini_transaction_id == 0, state = IN_PROGRESS

	linkedlist free_mini_transactions_list; // list of free mini transactions, new mini transactions are assigned from this list, here waiters_count must be 0

	// below two are the parts of dirty page table
	hashmap dirty_page_table;

	linkedlist free_dirty_page_entries_list; // list of free dirty page entries, new dirty page entrues are assigned from this lists or are allocated

	// wait for these many microseconds for acquiring a latch
	uint64_t latch_wait_timeout_in_microseconds;

	// timeout to wait for completion of a mini transaction
	// if you hit the timeout, you must abort the waiting transaction as there could be a deadlock
	uint64_t write_lock_wait_timeout_in_microseconds;

	// new threads attempting to start a new mini transaction execution wait here until a slot is available
	// a signal will be called everytime an insert is performed on free_mini_transactions_list
	pthread_cond_t conditional_to_wait_for_execution_slot;

	// manger_lock is to be held in shared mode for all the accesses by the user
	// it must be held in exclusive mode for truncating WALe and Bufferpool files, check pointing etc
	rwlock manager_lock;

	// job for checkpointer
	job checkpointer_job;

	// as the name suggests check pointing is done every this many microseconds
	uint64_t checkpointing_period_in_microseconds;

	// checkpoint will occur only if there are these many bytes after the last checkpoint, this must be in some MBs
	uint64_t checkpointing_LSN_diff_in_bytes;

	// size of wal file after which a new one is created, this is done only on successfull checkpoints
	// i.e. after checkpointing_period_in_microseconds and that there are atleast checkpointing_LSN_diff_in_bytes after the last checkpoint
	// so in reality the minimum wal filse size is governed by checkpointing_LSN_diff_in_bytes
	uint64_t max_wal_file_size_in_bytes;

	// the checkpointer thread waits here for checkpointing_period_in_microseconds
	// you can signal here, if shutdown is called OR if checkpointing_period_in_microseconds change
	pthread_cond_t wait_for_checkpointer_period;

	// after calling shutdown, you can wait here for the checkpointer to stop
	int is_checkpointer_running;
	pthread_cond_t wait_for_checkpointer_to_stop;

	// stats for internal use
	mini_transaction_engine_stats stats;

	// stats to be used by user
	mini_transaction_engine_user_stats user_stats;

	// this flag will be set if a shutdown was called
	int shutdown_called;

	// used by get_new_page_with_write_latch_for_mini_tx, only while creating a brand new database page
	// this lock only ensures that there will always be a single thread creating a new page in the engine
	pthread_mutex_t database_expansion_lock;

	// special flag that is checked by the bufferpool to ensure that the mini transaction engine is in recovery mode or not
	// and a mutex to protect it
	int is_in_recovery_mode;
	pthread_mutex_t recovery_mode_lock;
};

// page_size, page_id_width and log_sequence_number_width parameter is only used if passed as non-zero
// else they are either used for a new database OR are ensured to be correct for an existing database if non-zero
int initialize_mini_transaction_engine(mini_transaction_engine* mte, const char* database_file_name, uint32_t page_size, uint32_t page_id_width, uint32_t log_sequence_number_width, uint32_t bufferpool_frame_count, uint32_t wale_append_only_buffer_block_count, uint64_t latch_wait_timeout_in_microseconds, uint64_t write_lock_wait_timeout_in_microseconds, uint64_t checkpointing_period_in_microseconds, uint64_t checkpointing_LSN_diff_in_bytes, uint64_t max_wal_file_size_in_bytes);

#include<mini_transaction_engine_wale_only_functions.h>

#include<mini_transaction_engine_bufferpool_only_functions.h>

#include<mini_transaction_engine_page_alloc.h>

#include<mini_transaction_engine_allotment.h>

// appends a user generated log info, this can be used to log begin, abort and commit log records for the higher level transactions of the user
// returns INVALID_LOG_SEQUENCE_NUMBER if this could not be done
// an ideal thing to be done when you couldn't append your user_info log record is to exit(-1)
uint256 append_user_info_log_record_for_mini_transaction_engine(mini_transaction_engine* mte, int flush_after_append, const void* info, uint32_t info_size);

#include<log_record.h>

// returns a valud log record only if return value is 1
int get_log_record_at_LSN_for_mini_transaction_engine(mini_transaction_engine* mte, uint256 LSN, log_record* lr);

// if you pass in LSN == INVALID_LOG_SEQUENCE_NUMBER, then you get the oldest LSN available in the system
uint256 get_next_LSN_of_LSN_for_mini_transaction_engine(mini_transaction_engine* mte, uint256 LSN);

// if your mini transaction is huge perform this intermediately to allow more changes to be left in bufferpool
void intermediate_wal_flush_for_mini_transaction_engine(mini_transaction_engine* mte);

// if your mini transaction is huge perform this intermediately to allow all possibly flushable bufferpool changes to be flushed to disk
void intermediate_bufferpool_flush_for_mini_transaction_engine(mini_transaction_engine* mte);

void debug_print_wal_logs_for_mini_transaction_engine(mini_transaction_engine* mte);

void deinitialize_mini_transaction_engine(mini_transaction_engine* mte);

#endif