#include<mini_transaction_engine_bufferpool_only_functions.h>

void* acquire_page_with_reader_lock_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	// TODO
}

void* acquire_page_with_writer_lock_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	// TODO
}

int downgrade_writer_lock_to_reader_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents)
{
	// TODO
}

int upgrade_reader_lock_to_writer_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents)
{
	// TODO
}

int release_reader_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page)
{
	// TODO
}

int release_writer_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page)
{
	// TODO
}