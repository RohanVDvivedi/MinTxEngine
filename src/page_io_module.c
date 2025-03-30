#include<blockio/block_io.h>
#include<mintxengine/page_io_module.h>

#include<mintxengine/system_page_header_util.h>

#include<stdlib.h>
#include<stdio.h>

// page_id * page_size, will only overflow if the 64 bit off_t offset you are trying to read/write overflows, hence no problem here
static off_t get_first_block_id_for_page_id(uint64_t page_id, uint32_t page_size, size_t block_size)
{
	return ((page_id * page_size) / block_size) + 1; // this +1 ensures that we do not read/write the first read-only header block
}

int read_page_from_database_file(mini_transaction_engine* mte, void* frame_dest, uint64_t page_id)
{
	size_t block_size = get_block_size_for_block_file(&(mte->database_block_file));
	off_t block_id = get_first_block_id_for_page_id(page_id, mte->stats.page_size, block_size);
	size_t block_count = mte->stats.page_size / block_size;
	int res = read_blocks_from_block_file(&(mte->database_block_file), frame_dest, block_id, block_count);

	if(!res)
	{
		printf("ISSUE :: read io error on read_page_from_database_file\n");
		exit(-1);
	}

	pthread_mutex_lock(&(mte->recovery_mode_lock));
	int is_in_recovery_mode = mte->is_in_recovery_mode;
	pthread_mutex_unlock(&(mte->recovery_mode_lock));

	// if we are not in recovery mode and the checksum fails to match then fail with an exit signalling corruption
	if(!is_in_recovery_mode && !validate_page_checksum(frame_dest, &(mte->stats)))
	{
		printf("ISSUE :: page checksum validation failed on ead_page_from_database_file, database file corrupted\n");
		exit(-1);
	}

	return res;
}

int write_page_to_database_file(mini_transaction_engine* mte, const void* frame_src, uint64_t page_id)
{
	// while writing the page to disk always update its checksum
	// on-disk copy of the page must always have valid checksum
	recalculate_page_checksum((void*)frame_src, &(mte->stats));

	size_t block_size = get_block_size_for_block_file(&(mte->database_block_file));
	off_t block_id = get_first_block_id_for_page_id(page_id, mte->stats.page_size, block_size);
	size_t block_count = mte->stats.page_size / block_size;
	int res = write_blocks_to_block_file(&(mte->database_block_file), frame_src, block_id, block_count);

	if(!res)
	{
		printf("ISSUE :: write io error on write_page_to_database_file\n");
		exit(-1);
	}

	return res;
}