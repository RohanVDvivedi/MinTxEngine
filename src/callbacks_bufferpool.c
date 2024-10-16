#include<callbacks_bufferpool.h>

#include<block_io.h>

// page_id * page_size, will only overflow if the 64 bit off_t offset you are trying to read/write overflows, hence no problem here
static off_t get_first_block_id_for_page_id(uint64_t page_id, uint32_t page_size, size_t block_size)
{
	return ((page_id * page_size) / block_size) + 1; // this +1 ensures that we do not read/write the first read-only header block
}

int read_page_for_bufferpool(const void* page_io_ops_handle, void* frame_dest, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = get_first_block_id_for_page_id(page_id, page_size, block_size);
	size_t block_count = page_size / block_size;
	return read_blocks_from_block_file(((block_file*)(page_io_ops_handle)), frame_dest, block_id, block_count);
}

int write_page_for_bufferpool(const void* page_io_ops_handle, const void* frame_src, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = get_first_block_id_for_page_id(page_id, page_size, block_size);
	size_t block_count = page_size / block_size;
	return write_blocks_to_block_file(((block_file*)(page_io_ops_handle)), frame_src, block_id, block_count);
}

int flush_all_pages_for_bufferpool(const void* page_io_ops_handle)
{
	return flush_all_writes_to_block_file(((block_file*)(page_io_ops_handle)));
}

#include<system_page_header_util.h>
#include<mini_transaction_engine.h>

int can_be_flushed_to_disk_for_bufferpool(void* flush_callback_handle, uint64_t page_id, const void* frame)
{
	mini_transaction_engine* mte = flush_callback_handle;

	uint256 pageLSN = get_pageLSN_for_page(frame, &(mte->stats));

	// if the pageLSN on the page is invalid, then there is a possibility that the page is not being tracked by ARIES algorithm (because it just came into use, and was dirtied for some reason) so it can be flushed to disk
	if(are_equal_uint256(pageLSN, INVALID_LOG_SEQUENCE_NUMBER))
		return 1;

	// if the flushedLSN is 0, that means no logs were flushed yet, so you can not flush this page to disk
	if(are_equal_uint256(mte->flushedLSN, INVALID_LOG_SEQUENCE_NUMBER))
		return 0;

	return compare_uint256(mte->flushedLSN, pageLSN) >= 0; // ARIES suggests that flushedLSN should be greater than or equal to pageLSN to allow the page to reach to the disk
}

void was_flushed_to_disk_for_bufferpool(void* flush_callback_handle, uint64_t page_id, const void* frame)
{
	mini_transaction_engine* mte = flush_callback_handle;

	// the page that was flushed to disk, may not be dirty, so first find the corresponding entry from the dirty_page_table that matches the page_id
	dirty_page_table_entry* entry = (dirty_page_table_entry*) find_equals_in_hashmap(&(mte->dirty_page_table), &(dirty_page_table_entry){.page_id = page_id});

	// if no such entry is found in out dirty page table quit
	if(entry == NULL)
		return;

	// else remove it from the dirty_page_table and insert it to the free_dirty_page_entries_list
	// be mindful and reset the attributes of the entry
	remove_from_hashmap(&(mte->dirty_page_table), entry);
	entry->page_id = mte->user_stats.NULL_PAGE_ID;
	entry->recLSN = INVALID_LOG_SEQUENCE_NUMBER;
	insert_tail_in_linkedlist(&(mte->free_dirty_page_entries_list), entry);
}