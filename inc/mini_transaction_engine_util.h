#ifndef MINI_TRANSACTION_ENGINE_UTIL_H
#define MINI_TRANSACTION_ENGINE_UTIL_H

#include<mini_transaction_engine.h>

// it is an unsafe function, must be held with global lock held
// it marks the page as dirty in the dirty page table and bufferpool both as dirty
// it is expected that page contains the pageLSN that made it dirty
// if the page is already in dirty_page_table, then its recLSN is not updated
// it is expected that the page is already write latched
void mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mini_transaction_engine* mte, void* page, uint64_t page_id);

#endif