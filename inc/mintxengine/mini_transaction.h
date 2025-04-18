#ifndef MINI_TRANSACTION_H
#define MINI_TRANSACTION_H

#include<pthread.h>

#include<stdint.h>
#include<serint/large_uints.h>

#include<cutlery/linkedlist.h>

typedef enum mini_transaction_state mini_transaction_state;
enum mini_transaction_state
{
	MIN_TX_IN_PROGRESS,			// normal flow of operation of the transaction, reading and making changes
	MIN_TX_ABORTED,				// aborted, abort_error set, but the ABORT_MIN_TX log not yet written
	MIN_TX_UNDOING_FOR_ABORT,	// abort log written, and the changes of this transaction are being undone
	MIN_TX_COMPLETED,			// COMPLETE_MINI_TX log written, nothing needs to be done now
};

/*
	An instance of mini transaction can not be distributed to several threads to do more concurrent operations
	Yet you may have multiple active mini transactions to do more work in parallel OR concurrently
	But note that a mini transaction is a culmination of multiple sequential transactional operations
*/

typedef struct mini_transaction mini_transaction;
struct mini_transaction
{
	uint256 mini_transaction_id; // key for the mini transaction table, this is also the LSN of the first modification that this mini transaction made
	// it is 0 for a mini_transaction that only has read data until the current point in time

	uint256 lastLSN; // LSN of the last log record that this mini_transaction made
	// this is used to chain new log records in the reverse order, necessary for undoing upon aborts

	// the above 2 attributes are local to the mini_transaction and since user must have only a single reference to the mini transaction, you do not need need global lock to access them
	// the mini_transaction_id is constant after it is assigned, for the complete duration of the mini transaction, so it is accessible without global lock
	// all the rest of the below attributes are monitored by other mini transsctions also, hence you need global lock to access them

	mini_transaction_state state;
	/*
		State transition
		MIN_TX_IN_PROGRESS -----------------------------------------------> MIN_TX_COMPLETED
		OR
		MIN_TX_IN_PROGRESS -> MIN_TX_ABORTED -> MIN_TX_UNDOING_FOR_ABORT -> MIN_TX_COMPLETED
	*/

	int abort_error; // reason for abort if state = MIN_TX_ABORTED, MIN_TX_UNDOING_FOR_ABORT OR MIN_TX_COMPLETED, else set to 0

	// below attribute maintains the counter of the page latched that this mini transaction holds
	// a mini transaction can not be completed, (i.e. you can not call mte_complete_mini_tx() function on it), until it releases all the latches that it holds
	uint64_t page_latches_held_counter;

	pthread_cond_t write_lock_wait; // any mini_transaction who wants to waits for the writer lock on the page, write locked by this mini_transaction waits here
	// this wait completes soon after this transaction moves to COMPLETED state

	uint64_t reference_counter; // the number of transactions waiting on write_lock_wait + the users of the transaction

	// a mini transaction is moved to free list only after it is in MIN_TX_COMPLETED state and the reference_counter == 0

	// -----------------
	// nodes for intrusive structures that this mini transaction resides in, are below
	llnode enode;
};

// only mini_transaction_id is the key for the following two functions
int compare_mini_transactions(const void* mt1, const void* mt2);
cy_uint hash_mini_transaction(const void* mt);

// exits on failure to allocate memory, won't return NULL
mini_transaction* get_new_mini_transaction();
void delete_mini_transaction(mini_transaction* mt);

#include<cutlery/hashmap.h>

// returns minimum mini_transaction_id for the hashtable of mini_transactions
// returns INVALID_LOG_SEQUENCE_NUMBER if no mini_transaction-s are present
uint256 get_minimum_mini_transaction_id_for_mini_transaction_table(const hashmap* mini_transaction_table);

#define initialize_mini_transaction_table(mini_transaction_table, bucket_count) initialize_hashmap(mini_transaction_table, ELEMENTS_AS_LINKEDLIST_INSERT_AT_TAIL, bucket_count, &simple_hasher(hash_mini_transaction), &simple_comparator(compare_mini_transactions), offsetof(mini_transaction, enode))

void delete_mini_transaction_notify(void* resource_p, const void* data_p);
#define AND_DELETE_MINI_TRANSACTIONS_NOTIFIER &(notifier_interface){.resource_p = NULL, .notify = delete_mini_transaction_notify}

void transfer_to_mini_transaction_table_notify(void* resource_p, const void* data_p);
#define AND_TRANSFER_TO_MINI_TRANSACTION_TABLE_NOTIFIER(mini_transaction_table) &(notifier_interface){.resource_p = mini_transaction_table, .notify = transfer_to_mini_transaction_table_notify}

#endif