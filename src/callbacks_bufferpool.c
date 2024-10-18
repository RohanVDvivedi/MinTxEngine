#include<callbacks_bufferpool.h>

#include<block_io.h>

int read_page_for_bufferpool(const void* page_io_ops_handle, void* frame_dest, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = ((page_id * page_size) / block_size) + 1; // this +1 ensures that we do not write the first read-only header block
	size_t block_count = page_size / block_size;
	return read_blocks_from_block_file(((block_file*)(page_io_ops_handle)), frame_dest, block_id, block_count);
}

int write_page_for_bufferpool(const void* page_io_ops_handle, const void* frame_src, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = ((page_id * page_size) / block_size) + 1;// this +1 ensures that we do not read the first read-only header block
	size_t block_count = page_size / block_size;
	return write_blocks_to_block_file(((block_file*)(page_io_ops_handle)), frame_src, block_id, block_count);
}

int flush_all_pages_for_bufferpool(const void* page_io_ops_handle)
{
	return flush_all_writes_to_block_file(((block_file*)(page_io_ops_handle)));
}