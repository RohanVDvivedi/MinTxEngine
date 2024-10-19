#ifndef DIRTY_PAGE_TABLE_ENTRY_H
#define DIRTY_PAGE_TABLE_ENTRY_H

#include<stdint.h>
#include<large_uints.h>

#include<linkedlist.h>

typedef struct dirty_page_table_entry dirty_page_table_entry;
struct dirty_page_table_entry
{
	uint64_t page_id; // page_id of the page that is dirty
	uint256 recLSN; // the oldest LSN that made this page dirty, also called recoveryLSN -> you need to start redoing from this LSN to reach latest state of this page

	// embedded node to manage the entry
	// this entry resides in either dirty_page_table or in free_dirty_page_entries_list
	llnode enode;
};

// only page_id is the key for the following two functions
int compare_dirty_page_table_entries(const void* dpte1, const void* dpte2);
cy_uint hash_dirty_page_table_entry(const void* dpte);

#endif