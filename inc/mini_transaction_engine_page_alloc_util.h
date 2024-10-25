#ifndef MINI_TRANSACTION_ENGINE_PAGE_ALLOC_UTIL_H
#define MINI_TRANSACTION_ENGINE_PAGE_ALLOC_UTIL_H

#include<mini_transaction_engine.h>

// All _UNSAFE functions, must be called with with global lock held
// All _INTERNAL functions, must be called with with global lock not held

// performes a free page routine to free a page write latched by the given mini transaction
// this function must be called with manager lock, but without global lock held, it will take global lock as and when necessary
// page may not be persistently write locked by the provided mini transaction, and is assumed to be either not write locked by anyone or write locked by self
// so you need to make sure that this page is either not write locked by any one or write locked by self
// we also do not check whether the page is free or not, you must ensure that the page is allocated prior to this call
// aborts, only if we couldn't latch the free_space_mapper_page for the provided page_id
int free_write_latched_page_INTERNAL_UNSAFE(mini_transaction_engine* mte, mini_transaction* mt, void* page, uint64_t page_id);

#endif