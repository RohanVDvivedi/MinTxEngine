#include<callbacks_wale.h>

#include<block_io.h>

int read_blocks_for_wale(const void* block_io_ops_handle, void* dest, uint64_t block_id, uint64_t block_count)
{
	int res = read_blocks_from_block_file(((block_file*)block_io_ops_handle), dest, block_id, block_count);

	if(!res)
	{
		printf("ISSUE :: read io error on wale\n");
		exit(-1);
	}

	return res;
}

int write_blocks_for_wale(const void* block_io_ops_handle, const void* src, uint64_t block_id, uint64_t block_count)
{
	int res = write_blocks_to_block_file(((block_file*)block_io_ops_handle), src, block_id, block_count);

	if(!res)
	{
		printf("ISSUE :: write io error on wale\n");
		exit(-1);
	}

	return res;
}

int flush_all_writes_for_wale(const void* block_io_ops_handle)
{
	int res = flush_all_writes_to_block_file(((block_file*)block_io_ops_handle));

	if(!res)
	{
		printf("ISSUE :: flush io error on wale\n");
		exit(-1);
	}

	return res;
}