#ifndef MINI_TRANSACTION_ENGINE_H
#define MINI_TRANSACTION_ENGINE_H

#include<hashmap.h>
#include<linkedlist.h>

#include<rwlock.h>

typedef struct mini_transaction_engine mini_transaction_engine;
struct mini_transaction_engine
{
	// global lock for the bufferpool_p and wale_p
	// also for mini_transaction table
	pthread_mutex_t global_lock;

	// below are the two main objects that this mini transaction interactes with
	bufferpool* bufferpool_p;
	wale* wale_p;

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