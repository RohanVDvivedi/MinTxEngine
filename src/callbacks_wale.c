#include<blockio/block_io.h>
#include<mintxengine/callbacks_wale.h>

#include<stdlib.h>
#include<stdio.h>

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

#include<mintxengine/mini_transaction_engine.h>

void were_logs_flushed_to_disk_for_wale(void* flush_callback_handle, uint256 flushedLSN)
{
	mini_transaction_engine* mte = flush_callback_handle;

	mte->flushedLSN = flushedLSN;

	// there is one lesser thread performing wal flush
	mte->thread_counter_performing_wal_flush--;

	if(mte->thread_counter_waiting_for_wal_flush > 0)
		pthread_cond_broadcast(&(mte->wait_for_flushedLSN_advancement));
}