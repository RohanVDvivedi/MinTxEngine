#include<mini_transaction_engine_stats.h>

#include<serial_int.h>

#include<stdlib.h>
#include<string.h>

const char* magic_data = "0xda1aba5ef11e";

int write_to_first_block(block_file* bf, const mini_transaction_engine_stats* stats)
{
	const uint32_t offset_to_magic_data = 0;
	const uint32_t offset_to_page_size = strlen(magic_data);
	const uint32_t offset_to_page_id_width = offset_to_page_size + sizeof(uint32_t);
	const uint32_t offset_to_log_sequence_number_width = offset_to_page_id_width + sizeof(uint32_t);
	const uint32_t offset_to_end_of_first_block_data = offset_to_log_sequence_number_width + sizeof(uint32_t);

	if(offset_to_end_of_first_block_data > get_block_size_for_block_file(bf))
		return 0;

	char* first_block = aligned_alloc(get_block_size_for_block_file(bf), get_block_size_for_block_file(bf));
	if(first_block == NULL)
		return 0;

	// reset all bytes of first block to all zeros
	memory_set(first_block, 0, get_block_size_for_block_file(bf));

	memcpy(first_block + offset_to_magic_data, magic_data, strlen(magic_data));
	serialize_uint32(first_block + offset_to_page_size, sizeof(uint32_t), stats->page_size);
	serialize_uint32(first_block + offset_to_page_id_width, sizeof(uint32_t), stats->page_id_width);
	serialize_uint32(first_block + offset_to_log_sequence_number_width, sizeof(uint32_t), stats->log_sequence_number_width);

	if(!write_blocks_to_block_file(bf, first_block, 0, 1)
		|| !flush_all_writes_to_block_file(bf))
		goto ERROR;

	free(first_block);
	return 1;

	ERROR :;
	free(first_block);
	return 0;
}

int read_from_first_block(block_file* bf, mini_transaction_engine_stats* stats)
{
	const uint32_t offset_to_magic_data = 0;
	const uint32_t offset_to_page_size = strlen(magic_data);
	const uint32_t offset_to_page_id_width = offset_to_page_size + sizeof(uint32_t);
	const uint32_t offset_to_log_sequence_number_width = offset_to_page_id_width + sizeof(uint32_t);
	const uint32_t offset_to_end_of_first_block_data = offset_to_log_sequence_number_width + sizeof(uint32_t);

	if(offset_to_end_of_first_block_data > get_block_size_for_block_file(bf))
		return 0;

	// if the file is empty, do not read it
	if(get_total_size_for_block_file(bf) == 0)
		return 0;

	char* first_block = aligned_alloc(get_block_size_for_block_file(bf), get_block_size_for_block_file(bf));
	if(first_block == NULL)
		return 0;

	if(!read_blocks_from_block_file(bf, first_block, 0, 1))
		goto ERROR;

	// ensure that magic data matches
	if(memcmp(first_block + offset_to_magic_data, magic_data, strlen(magic_data)))
		goto ERROR;

	stats->page_size = deserialize_uint32(first_block + offset_to_page_size, sizeof(uint32_t));
	stats->page_id_width = deserialize_uint32(first_block + offset_to_page_id_width, sizeof(uint32_t));
	stats->log_sequence_number_width = deserialize_uint32(first_block + offset_to_log_sequence_number_width, sizeof(uint32_t));

	free(first_block);
	return 1;

	ERROR :;
	free(first_block);
	return 0;
}

#include<system_page_header_util.h>
#include<callbacks_bufferpool.h>

mini_transaction_engine_user_stats get_mini_transaction_engine_user_stats(const mini_transaction_engine_stats* stats, uint32_t database_file_block_size)
{
	// max_page_count is min(MAX_PAGE_COUNT_possible, 1 << (8 * page_id_width))
	// it is either dictated by the overflow of off_t or the page_id_width
	uint64_t max_page_count = MAX_PAGE_COUNT(stats->page_size, database_file_block_size);
	if(stats->page_id_width < 8)
		max_page_count = min(max_page_count, UINT64_C(1) << (CHAR_BIT * stats->page_id_width));

	return (mini_transaction_engine_user_stats){
		.page_size = get_page_content_size_for_data_pages(stats),
		.page_id_width = stats->page_id_width,
		.log_sequence_number_width = stats->log_sequence_number_width,
		.NULL_PAGE_ID = 0,
		.max_page_count = max_page_count,
	};
}