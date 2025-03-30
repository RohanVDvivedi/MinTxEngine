#ifndef MINI_TRANSACTION_ENGINE_BUFFERPOOL_ONLY_FUNCTIONS_H
#define MINI_TRANSACTION_ENGINE_BUFFERPOOL_ONLY_FUNCTIONS_H

#include<mintxengine/mini_transaction_engine.h>

/*
	You should never attempt to acquire a latch on a page that you do know of being allocated or not
	Attempting to lock free pages and uses them may succeed but will create dead locks
	We while granting locks to you do not ensure that the page is not free
*/

/*
	For acquire_page_with_*_latch_for_mini_tx function, we do check that the page_id is within bounds of max_page_count and database_page_count, and that it is not a free space mapper page
	But we do not check if the page is free or not (ideal logic should be to abort if a user is trying to latch a free page)
	This is not done because
	To ensure no deadlocks happen, while allocating a free page, the order is first lock the free space mapper page and then the actual page
	while for freeing a page, the order is to first lock the actual data page and then the corresponding free space mapper page

	if we are unsure and want to check if a page is free or not, we are thwarted by the logic as we do not know the correct order to lock these both pages, and a wring decission may result in a deadlock
	so we do not do this check here

	This very same thing happens also when you release latch on the page.

	SO I ADVISE YOU TO ONLY ACCESS/LATCH PAGES THAT YOU KNOW ARE ALLOCATED, AND FREE/RELEASE LATCHES TO PAGES YOU GET POINTERS TO FOR ENGINE, PERIOD.
*/

// for the below function you can pass mt = NULL, if you are sure that you will never modify the page, such a page can also be released by release_reader_latch_on_page_for_mini_tx(), again with mt = NULL
// Note: this function even with mt = NULL, may still block for a writer latch or a writer lock held by some another mini_transaction
void* acquire_page_with_reader_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id);

void* acquire_page_with_writer_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id);

int downgrade_writer_latch_to_reader_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents);
int upgrade_reader_latch_to_writer_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents);

// for the below two function you can pass mt = NULL, if the free_page is also 0
// this allows you to release latches on pages even after its completion

int release_reader_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page);
int release_writer_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page);

#endif