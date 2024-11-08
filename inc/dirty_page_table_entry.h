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

// exits on failure to allocate memory, won't return NULL
dirty_page_table_entry* get_new_dirty_page_table_entry();
void delete_dirty_page_table_entry(dirty_page_table_entry* dpte);

#include<hashmap.h>

// returns minimum recLSN for the dirty_page_table
// returns INVALID_LOG_SEQUENCE_NUMBER if dirty_page_table is empty
uint256 get_minimum_recLSN_for_dirty_page_table(const hashmap* dirty_page_table);

#define initialize_dirty_page_table(dirty_page_table, bucket_count) initialize_hashmap(dirty_page_table, ELEMENTS_AS_LINKEDLIST_INSERT_AT_TAIL, bucket_count, &simple_hasher(hash_dirty_page_table_entry), &simple_comparator(compare_dirty_page_table_entries), offsetof(dirty_page_table_entry, enode))

void delete_dirty_page_table_notify(void* resource_p, const void* data_p);
#define AND_DELETE_DIRTY_PAGE_TABLE_ENTRIES_NOTIFIER &(notifier_interface){.resource_p = NULL, .notify = delete_dirty_page_table_notify}

#endif