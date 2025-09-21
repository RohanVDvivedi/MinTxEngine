#ifndef MINI_TRANSACTION_ENGINE_ALLOTMENT_H
#define MINI_TRANSACTION_ENGINE_ALLOTMENT_H

#include<mintxengine/mini_transaction_engine.h>

/*
	For both the below function page_latches_to_be_borrowed is the number of latches that are to be borrowed from completing mini transaction to another one that gets alloted
	You can only borrow latches from a non-aborted completed mini transaction to the new one
	For completing an aborted mini transaction you must release all latches prior to calling mte_complete_mini_tx()

	This feature will allow you to hold latches with TupleIndexer's iterators on successfull mini transactions, even after completing them,
	so that this iterator will not need to re-traverse the data structure again to figure out the insert/update/delete positions for the next mini transaction, as you will already be holding latches with write locks released after the completion
*/

// allots a new mini transaction for you to use
// wait_timeout_in_microseconds can not be BLOCKING or NON_BLOCKING
mini_transaction* mte_allot_mini_tx(mini_transaction_engine* mte, uint64_t page_latches_to_be_borrowed);

// completes a mini transaction for you
// if it is a reader it is directly put into MINI_TX_COMPLETED state
// if it is a writer that is still in MINI_TX_IN_PROGRESS state, then a COMPLETE_MINI_TX log record is appended, then put into MINI_TX_COMPLETED state
// if it is a writer in MINI_TX_ABORTED state, then a ABORT_MINI_TX is appended, and then placed in MINI_TX_UNDOING_FOR_ABORT
//    then if it is in MINI_TX_UNDOING_FOR_ABORT state then COMPESATION_LOG records are generated from its last log record until we reach the beginning
//    then a COMPLETE_MINI_TX log record is appended, then put into MINI_TX_COMPLETED state
// finally the mini transaction reference counter is decremented and returned to be used by someone else
// this function must be called only on a non-complete mini transaction, and must be called only once, any reuse of the pointer *mt after calling this function is an undefined behaviour
// you must never use the pointer mt again after calling this function
uint256 mte_complete_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, int flush_on_completion, const void* complete_info, uint32_t complete_info_size, uint64_t* page_latches_to_be_borrowed);

// this function only gives a user level api to mark a transaction aborted, you will still need to call mte_complete_mini_tx
// abort_error can not be non-negative
int mark_aborted_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, int abort_error);

// returns negative abort_error if the mini transaction is aborted
// you must not call this function on any mini_transaction* mt, that has already, mte_complete_mini_tx called for
int is_aborted_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt);
#define get_abort_error_for_mini_tx is_aborted_for_mini_tx

// returns true if the mini transaction in context has ever done anything to make changes to the database
// you must not call this function on any mini_transaction* mt, that has already, mte_complete_mini_tx called for
int is_writer_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt);

#endif