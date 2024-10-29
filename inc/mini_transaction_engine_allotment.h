#ifndef MINI_TRANSACTION_ENGINE_ALLOTMENT_H
#define MINI_TRANSACTION_ENGINE_ALLOTMENT_H

#include<mini_transaction_engine.h>

// allots a new mini transaction for you to use
mini_transaction* mte_allot_mini_tx(mini_transaction_engine* mte, uint64_t wait_timeout_in_microseconds);

// completes a mini transaction for you
// if it is a reader it is directly put into MINI_TX_COMPLETED state
// if it is a writer that is still in MINI_TX_IN_PROGRESS state, then a COMPLETE_MINI_TX log record is appended, then put into MINI_TX_COMPLETED state
// if it is a writer in MINI_TX_ABORTED state, then a ABORT_MINI_TX is appended, and then placed in MINI_TX_UNDOING_FOR_ABORT
//    then if it is in MINI_TX_UNDOING_FOR_ABORT state then COMPESATION_LOG records are generated from its last log record until we reach the beginning
//    then a COMPLETE_MINI_TX log record is appended, then put into MINI_TX_COMPLETED state
// finally the mini transaction reference counter is decremented and returned to be used by someone else
// this function must be called only on a non-complete mini transaction, and must be called only once, any reuse of the pointer *mt after calling this function is an undefined behaviour
void mte_complete_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, const void* complete_info, uint32_t complete_info_size);

#endif