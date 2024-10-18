#include<callbacks_wale.h>

#include<block_io.h>

int read_blocks_for_wale(const void* block_io_ops_handle, void* dest, uint64_t block_id, uint64_t block_count)
{
	return read_blocks_from_block_file(((block_file*)block_io_ops_handle), dest, block_id, block_count);
}

int write_blocks_for_wale(const void* block_io_ops_handle, const void* src, uint64_t block_id, uint64_t block_count)
{
	return write_blocks_to_block_file(((block_file*)block_io_ops_handle), src, block_id, block_count);
}

int flush_all_writes_for_wale(const void* block_io_ops_handle)
{
	return flush_all_writes_to_block_file(((block_file*)block_io_ops_handle));
}