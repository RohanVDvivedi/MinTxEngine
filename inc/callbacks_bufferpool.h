#ifndef CALLBACKS_BUFFERPOOL_H
#define CALLBACKS_BUFFERPOOL_H

#include<stdint.h>

// implementing all callbacks to be implemented by the MinTxEngine for its bufferpool structure

// implementation of functions for page_io_ops handle

// remember the first block is a read only block consisting of only magic_data and mini_transaction_engine_stats
// and you must ensure that the page_size is multiple of block_size of the file

int read_page_for_bufferpool(const void* page_io_ops_handle, void* frame_dest, uint64_t page_id, uint32_t page_size);
int write_page_for_bufferpool(const void* page_io_ops_handle, const void* frame_src, uint64_t page_id, uint32_t page_size);
int flush_all_pages_for_bufferpool(const void* page_io_ops_handle);

#define get_page_io_ops_for_bufferpool(bf, page_size, page_frame_alignment) (page_io_ops){ \
					.page_io_ops_handle = bf, \
					.page_size = page_size, \
					.page_frame_alignment = page_frame_alignment, \
					.read_page = read_page_for_bufferpool, \
					.write_page = write_page_for_bufferpool, \
					.flush_all_writes = flush_all_pages_for_bufferpool, \
				};

// any page id you pass on to the bufferpool must be lesser than MAX_PAGE_COUNT, else abort the mini_transaction for illegal access
#define MAX_PAGE_COUNT(PAGE_SIZE, BLOCK_SIZE) ((MAX_BLOCK_COUNT(BLOCK_SIZE) - 1) / (PAGE_SIZE / BLOCK_SIZE))

// flush_callback_handle is same as mini_transaction_engine for the below 2 functions

// return true if flushedLSN for the WAL is greater than or equal to the pageLSN of the frame passed to the callback
int can_be_flushed_to_disk_for_bufferpool(void* flush_callback_handle, uint64_t page_id, const void* frame);

// remove the dirty page entry for the corresponding page_id from the mini_transaction_engine and move it to the free list
void was_flushed_to_disk_for_bufferpool(void* flush_callback_handle, uint64_t page_id, const void* frame);

#endif