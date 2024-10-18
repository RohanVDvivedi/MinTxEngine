#include<mini_transaction_engine_stats.h>

#include<serial_int.h>

#include<stdlib.h>
#include<string.h>

const char* magic_data = "0xda1aba5ef11e";

int write_to_first_block(block_file* bf, const mini_transaction_engine_stats* stats)
{
	const uint32_t offset_to_magic_data = 0;
	const uint32_t offset_to_page_size = strlen(magic_data);
	const uint32_t offset_to_page_id_width = offset_to_page_size + 4;
	const uint32_t offset_to_log_sequence_number_width = offset_to_page_id_width + 4;
	const uint32_t offset_to_end_of_first_block_data = offset_to_log_sequence_number_width + 4;

	if(offset_to_end_of_first_block_data > get_block_size_for_block_file(bf))
		return 0;

	char* first_block = aligned_alloc(get_block_size_for_block_file(bf), get_block_size_for_block_file(bf));
	if(first_block == NULL)
		return 0;

	memcpy(first_block + offset_to_magic_data, magic_data, strlen(magic_data));
	serialize_uint32(first_block + offset_to_page_size, 4, stats->page_size);
	serialize_uint32(first_block + offset_to_page_id_width, 4, stats->page_id_width);
	serialize_uint32(first_block + offset_to_log_sequence_number_width, 4, stats->log_sequence_number_width);

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
	const uint32_t offset_to_page_id_width = offset_to_page_size + 4;
	const uint32_t offset_to_log_sequence_number_width = offset_to_page_id_width + 4;
	const uint32_t offset_to_end_of_first_block_data = offset_to_log_sequence_number_width + 4;

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

	stats->page_size = deserialize_uint32(first_block + offset_to_page_size, 4);
	stats->page_id_width = deserialize_uint32(first_block + offset_to_page_id_width, 4);
	stats->log_sequence_number_width = deserialize_uint32(first_block + offset_to_log_sequence_number_width, 4);

	free(first_block);
	return 1;

	ERROR :;
	free(first_block);
	return 0;
}

#include<system_page_header_util.h>

mini_transaction_engine_user_stats get_mini_transaction_engine_user_stats(const mini_transaction_engine_stats* stats)
{
	return (mini_transaction_engine_user_stats){
		.page_size = get_page_content_size_for_data_pages(stats),
		.page_id_width = stats->page_id_width,
		.log_sequence_number_width = stats->log_sequence_number_width,
		.NULL_PAGE_ID = 0,
	};
}