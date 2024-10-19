#ifndef MINI_TRANSACTION_H
#define MINI_TRANSACTION_H

#include<pthread.h>

#include<stdint.h>
#include<large_uints.h>

#include<linkedlist.h>

typedef enum mini_transaction_state mini_transaction_state;
enum mini_transaction_state
{
	IN_PROGRESS,
	UNDOING_FOR_ABORT,
	ABORTED,
	COMMITTED,
};

typedef struct mini_transaction mini_transaction;
struct mini_transaction
{
	uint256 mini_transaction_id; // key for the mini transaction table, this is also the LSN of the first modification that this mini transaction made
	// it is 0 for a mini_transaction that only has read data until the current point in time

	mini_transaction_state state;
	/*
		ABORTED <- UNDOING_FOR_ABORT <- IN_PROGRESS -> COMMITTED
	*/

	int abort_error; // reason for abort if state = UNDOING_FOR_ABORT or ABORTED, else set to 0

	pthread_cond_t write_lock_wait; // any mini_transaction who wants to waits for the writer lock on the page, write locked by this mini_transaction waits here
	// this wait completes soon after this transaction moves to COMMITTED or ABORTED state

	uint64_t waiters_count; // the number of transactions waiting on write_lock_wait

	uint256 lastLSN; // LSN of the last log record that this mini_transaction made
	// this is used to chain new log records in the reverse order, necessary for undoing upon aborts

	// a mini transaction is moved to free list only after it is in ABORTED/COMMITTED state and the waiters_count == 0

	// -----------------
	// nodes for intrusive structures that this mini transaction resides in, are below
	llnode enode;
};

// only mini_transaction_id is the key for the following two functions
int compare_mini_transactions(const void* mt1, const void* mt2);
cy_uint hash_mini_transaction(const void* mt);

#endif