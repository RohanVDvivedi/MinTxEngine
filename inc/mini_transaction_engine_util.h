#ifndef MINI_TRANSACTION_ENGINE_UTIL_H
#define MINI_TRANSACTION_ENGINE_UTIL_H

#include<mini_transaction_engine.h>

// All _UNSAFE functions, must be called with global lock held

// it marks the page as dirty in the dirty page table and bufferpool, both as dirty
// it is expected that page contains the pageLSN that made it dirty, so the newly inserted dirty page table entry (if any) has its recLSN as pageLSN of the page
// if the page is already in dirty_page_table, then its recLSN is not updated
// it is expected that the page is already write latched
void mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mini_transaction_engine* mte, void* page, uint64_t page_id);

// the below dunction is only to be called on data pages, and never on free space mapper pages
// this function reads the writerLSN on the page and tries to figure out if there is an active transactions that has this page persistently write locked
// is so a pointer to this transaction is returned
// this function call does not affect the reference_counter of that mini transaction, hence the return values must not be used post release of the global lock
// a standard correct procedure would be to get the locker mini transaction, then relese latch on the page and then wait for the this mini transaction to complete (using the function wait_for_mini_transaction_completion_UNSAFE() below)
mini_transaction* get_mini_transaction_that_last_persistent_write_locked_this_page_UNSAFE(mini_transaction_engine* mte, void* page);

// decrement the reference_counter of the mini transaction
// if it reaches 0, move it from reader_mini_transactions/writer_mini_transactions to free_mini_transactions_list, and also wake up any one thread waiting for conditional_to_wait_for_execution_slot
void decrement_mini_transaction_reference_counter_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt);

// never wait on your self for competion
// if you are waiting here in order to subsequently acquire lock/latch on a page, then you must first release latch on that page (with force_flush = 0) before you go ahead with waiting here
// if you hold latch on the page and go to wait then the other mini transaction will never have a chance to complete, (as it may need the locked page in future to make other changes or to undo changes if it aborts)
int wait_for_mini_transaction_completion_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt);

#endif