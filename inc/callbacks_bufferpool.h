#ifndef CALLBACKS_BUFFERPOOL_H
#define CALLBACKS_BUFFERPOOL_H

#include<stdint.h>

// implementing all callbacks to be implemented by the MinTxEngine for its bufferpool structure

// implementation of functions for page_io_ops handle

// remember the first block is a read only block consisting of only magic_data and mini_transaction_engine_stats

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

#endif