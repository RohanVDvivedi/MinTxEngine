#include<mini_transaction_engine_util.h>

#include<system_page_header_util.h>

void mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mini_transaction_engine* mte, void* page, uint64_t page_id)
{
	notify_modification_for_write_locked_page(&(mte->bufferpool_handle), page);

	dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(mte->dirty_page_table), &((dirty_page_table_entry){.page_id = page_id}));
	
	// if it is already present in dirty page table then nothing needs to be done
	if(dpte != NULL)
		return ;

	// else create or get one from free list
	dpte = (dirty_page_table_entry*)get_head_of_linkedlist(&(mte->free_dirty_page_entries_list));
	if(dpte != NULL)
		remove_head_from_linkedlist(&(mte->free_dirty_page_entries_list));
	else
		dpte = get_new_dirty_page_table_entry();

	// set appropriate parameters and insert it to dirty page table
	dpte->page_id = page_id;
	dpte->recLSN = get_pageLSN_for_page(page, &(mte->stats));
	insert_in_hashmap(&(mte->dirty_page_table), dpte);
}