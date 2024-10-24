#ifndef MINI_TRANSACTION_ENGINE_BUFFERPOOL_ONLY_FUNCTIONS_H
#define MINI_TRANSACTION_ENGINE_BUFFERPOOL_ONLY_FUNCTIONS_H

#include<mini_transaction_engine.h>

void* acquire_page_with_reader_lock_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id);
void* acquire_page_with_writer_lock_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id);

int downgrade_writer_lock_to_reader_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents);
int upgrade_reader_lock_to_writer_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents);

int release_reader_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page);
int release_writer_lock_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page);

#endif