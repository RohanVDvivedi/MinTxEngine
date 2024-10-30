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
// it will release latch on page provided, if it is successfully freed
int free_write_latched_page_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, void* page, uint64_t page_id);

// below two are _INTERNAL functions to be called without global_lock held
// they are to be used to be used for allocating a new page for the provided mini transaction
// below function allocates an existing database page (without increasing database_page_count), that is free and not actively write_locked by any other mini transaction
// below function should be called with atleast a shared lock held on manager_lock
void* allocate_page_without_database_expansion_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, uint64_t* page_id);
// below function allocates a new database page (and a page for its free page if required), expanding the datbase by atmost 2 pages
// below function should be called with atleast a shared lock held on manager_lock
void* allocate_page_with_database_expansion_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, uint64_t* page_id);

#endif