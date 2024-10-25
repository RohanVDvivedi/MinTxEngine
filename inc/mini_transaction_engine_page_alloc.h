#ifndef MINI_TRANSACTION_ENGINE_PAGE_ALLOC_H
#define MINI_TRANSACTION_ENGINE_PAGE_ALLOC_H

#include<mini_transaction_engine.h>

int free_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id);

#endif