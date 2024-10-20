#ifndef MINI_TRANSACTION_ENGINE_UTIL_H
#define MINI_TRANSACTION_ENGINE_UTIL_H

#include<mini_transaction_engine.h>

// it is an unsafe function, must be held with global lock held
// it marks the page as dirty in the dirty page table and bufferpool both as dirty
// it is expected that page contains the pageLSN that made it dirty
// if the page is already in dirty_page_table, then its recLSN is not updated
// it is expected that the page is already write latched
void mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mini_transaction_engine* mte, void* page, uint64_t page_id);

// the below function does the following
/*
	1. logs log record to latest wale, gets the LSN for this record
	2. if mt->mini_transaction_id == INVALID, then assigns it and moves the mt to writer_mini_transactions and assigns its mini_transaction_id to LSN
	3. mt->lastLSN = LSN
	4. page->pageLSN = LSN
	5. if the page_id is not that of free space mapper page, then page->writerLSN = mt->mini_transaction_id
	6. returns LSN of the log record we just logged

	// if you were logging an update to the PAGE_ALLOCATION and PAGE_DEALLOCATION then you must also write the writerLSN of the alocatee page with the mt->mini_transaction_id
*/
uint256 log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mini_transaction_engine* mte, const void* log_record, mini_transaction* mt, void* page, uint64_t page_id);

#endif