#include<mini_transaction_engine_wale_only_functions.h>

// the below function does the following
/*
	1. logs log record to latest wale, gets the LSN for this record
	2. if mt->mini_transaction_id == INVALID, then assigns it and moves the mt to writer_mini_transactions and assigns its mini_transaction_id to LSN
	3. mt->lastLSN = LSN
	4. page->pageLSN = LSN
	5. if the page_id is not that of free space mapper page, then page->writerLSN = mt->mini_transaction_id
	6. returns LSN of the log record we just logged
*/
uint256 log_the_already_applied_log_record_for_mini_transaction_and_manage_state_UNSAFE(mini_transaction_engine* mte, const void* log_record, mini_transaction* mt, void* page, uint64_t page_id);