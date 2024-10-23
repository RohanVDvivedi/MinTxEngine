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
// a standard correct procedure would be to get the locker mini transaction, then relese latch on the page and then wait for the this mini transaction to complete (using the function below)
mini_transaction* get_mini_transaction_that_last_persistent_write_locked_this_page_UNSAFE(mini_transaction_engine* mte, void* page);

#endif