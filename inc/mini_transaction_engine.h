#ifndef MINI_TRANSACTION_ENGINE_H
#define MINI_TRANSACTION_ENGINE_H

#include<hashmap.h>
#include<linkedlist.h>
#include<arraylist.h>

#include<rwlock.h>

typedef struct dirty_page_table_entry dirty_page_table_entry;
struct dirty_page_table_entry
{
	uint64_t page_id; // page_id of the page that is dirty
	uint256 recLSN; // the oldest LSN that made this page dirty, also called recoveryLSN -> you need to start redoing from this LSN to reach latest state of this page
};

typedef struct mini_transaction_engine mini_transaction_engine;
struct mini_transaction_engine
{
	// global lock for the bufferpool_p and wales
	// also for mini_transaction table
	pthread_mutex_t global_lock;

	// below are the two main objects that this mini transaction interactes with
	bufferpool* bufferpool_p;

	// we need more wale objects we only add log record to the latest one
	arraylist wales;

	// below three are the parts of mini_transaction table
	hashmap writer_mini_transactions; // mini_transaction_id != 0, state = IN_PROGRESS or UNDOING_FOR_ABORT else if state = ABORTED or COMMITTED then waiters_count > 0
	linked_list reader_mini_transactions; // mini_transaction_id == 0, state = IN_PROGRESS

	linked_list free_mini_transactions_list; // list of free mini transactions, new mini transactions are assigned from this list, here waiters_count must be 0

	// below two are the parts of dirty page table
	hashmap dirty_page_table;

	linked_list free_dirty_pages_list; // list of free dirty pages, new dirty pages are assigned from this lists

	// manger_lock is to be held in shared mode for all the accesses by the user
	// it must be held in exclusive mode for truncating WALe and Bufferpool files, check pointing etc
	rwlock manager_lock;

	// as the name suggests check pointing is done every this many milliseconds
	uint64_t checkpointing_period_in_miliseconds;
};

#endif