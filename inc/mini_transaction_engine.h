#ifndef MINI_TRANSACTION_ENGINE_H
#define MINI_TRANSACTION_ENGINE_H

#include<hashmap.h>
#include<linkedlist.h>
#include<arraylist.h>

#include<rwlock.h>

#include<mini_transaction_engine_stats.h>
#include<log_record.h>

#include<mini_transaction.h>
#include<dirty_page_table_entry.h>

// both the accessor structs below must be created over malloc-ed memory and 

typedef struct wal_accessor wal_accessor;
struct wal_accessor
{
	char* wale_file_name;
	block_file wale_block_file;
	wale wale_handle;
};

typedef struct bufferpool_accessor bufferpool_accessor;
struct bufferpool_accessor
{
	char* bufferpool_file_name; // new wale files are created by appending .log.<first_log_sequence_number> to this string
	block_file bufferpool_block_file;
	bufferpool bufferpool_handle;
};

typedef struct mini_transaction_engine mini_transaction_engine;
struct mini_transaction_engine
{
	// global lock for the bufferpool_p and wales
	// also for mini_transaction table
	pthread_mutex_t global_lock;

	// pointer to the bufferpool accessor
	bufferpool_accessor* bfa;

	// list of wal_accessor
	arraylist wa_list;

	// this variable is to be updated as per the rules defined here
	// flushedLSN = max(flushedLSN, flush_all_log_records(wa_list.last().wale_handle));
	// this is because we do not have a globally consistent view of the flushedLSN because global lock ges released while retrieveing it, and call to flushing log records will not ensure the return in any correct order, as global lock is released also while flushing the log records
	// we can also not rely on the flushed LSN of the most recent wale_handle because if there were no log records writeen then it might even be 0, i.e. invalid
	uint256 flushedLSN;

	// tuple definitions for the log records handled by this engine
	log_record_tuple_defs lrtd;

	// below three are the parts of mini_transaction table
	hashmap writer_mini_transactions; // mini_transaction_id != 0, state = IN_PROGRESS or UNDOING_FOR_ABORT else if state = ABORTED or COMMITTED then waiters_count > 0
	linked_list reader_mini_transactions; // mini_transaction_id == 0, state = IN_PROGRESS

	linked_list free_mini_transactions_list; // list of free mini transactions, new mini transactions are assigned from this list, here waiters_count must be 0

	// below two are the parts of dirty page table
	hashmap dirty_page_table;

	linked_list free_dirty_page_entries_list; // list of free dirty page entries, new dirty page entrues are assigned from this lists or are allocated

	// new threads attempting to start a new mini transaction execution wait here until a slot is available
	// a signal will be called everytime an insert is performed on free_mini_transactions_list
	pthread_mutex_t conditional_to_wait_for_execution_slot;

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

// on malloc failures we do an exit(-1), no exceptions unless we could handle it

#endif