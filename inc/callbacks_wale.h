#ifndef CALLBACKS_WALE_H
#define CALLBACKS_WALE_H

#include<stdint.h>

// implementing all callbacks to be implemented by the MinTxEngine for its wale structure

// implementation of functions for block_io_ops handle

int read_blocks_for_wale(const void* block_io_ops_handle, void* dest, uint64_t block_id, uint64_t block_count);
int write_blocks_for_wale(const void* block_io_ops_handle, const void* src, uint64_t block_id, uint64_t block_count);
int flush_all_writes_for_wale(const void* block_io_ops_handle);

#define get_block_io_ops(bf) (block_io_ops){ \
		.block_io_ops_handle = bf, \
		.block_size = get_block_size_for_block_file(bf), \
		.block_buffer_alignment = get_block_size_for_block_file(bf), \
		.read_blocks = read_blocks_for_wale, \
		.write_blocks = write_blocks_for_wale, \
		.flush_all_writes = flush_all_writes_for_wale, \
	}

#endif