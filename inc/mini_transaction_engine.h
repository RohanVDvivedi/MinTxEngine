#ifndef MINI_TRANSACTION_ENGINE_H
#define MINI_TRANSACTION_ENGINE_H

#include<hashmap.h>
#include<linkedlist.h>
#include<arraylist.h>

#include<rwlock.h>

#include<mini_transaction_engine_stats.h>
#include<mini_transaction.h>
#include<dirty_page_table_entry.h>

typedef struct mini_transaction_engine mini_transaction_engine;
struct mini_transaction_engine
{
	// global lock for the bufferpool_p and wales
	// also for mini_transaction table
	pthread_mutex_t global_lock;

	// below are the 6 main objects that this mini transaction interactes with bufferpool_p and wale_p and their related block files
	block_file* bufferpool_block_file;
	bufferpool* bufferpool_p;

	// the writable wale
	block_file* wale_block_file;
	wale* wale_p;

	// we need more wale objects we only add log record to the latest one (above), all prior wales are read only
	// the manager/checkpointer discards wales and truncates the old wale block files not longer in use
	arraylist wale_block_files;
	arraylist wales;

	// below three are the parts of mini_transaction table
	hashmap writer_mini_transactions; // mini_transaction_id != 0, state = IN_PROGRESS or UNDOING_FOR_ABORT else if state = ABORTED or COMMITTED then waiters_count > 0
	linked_list reader_mini_transactions; // mini_transaction_id == 0, state = IN_PROGRESS

	linked_list free_mini_transactions_list; // list of free mini transactions, new mini transactions are assigned from this list, here waiters_count must be 0

	// below two are the parts of dirty page table
	hashmap dirty_page_table;

	linked_list free_dirty_pages_list; // list of free dirty pages, new dirty pages are assigned from this lists

	// new threads attempting to start a new mini transaction execution wait here until a slot is available
	// a signal will be called everytime an insert is performed on free_mini_transactions_list
	pthread_mutex_t conditional_to_wait_for_execution_slot;

	// manger_lock is to be held in shared mode for all the accesses by the user
	// it must be held in exclusive mode for truncating WALe and Bufferpool files, check pointing etc
	rwlock manager_lock;

	// as the name suggests check pointing is done every this many milliseconds
	uint64_t checkpointing_period_in_miliseconds;

	mini_transaction_engine_stats stats;
};

// on malloc failures we do an exit(-1), no exceptions unless we could handle it

#endif