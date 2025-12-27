#include<mintxengine/system_page_header_util.h>

#include<zlib.h>

static uint32_t calculate_checksum(const void* data, uint32_t data_size)
{
	uint32_t crc32_result = crc32(0UL, NULL, 0U);
	crc32_result = crc32(crc32_result, data, data_size);
	return crc32_result;
}

uint32_t recalculate_page_checksum(void* page, const mini_transaction_engine_stats* stats)
{
	uint32_t checksum = calculate_checksum(page + sizeof(uint32_t), stats->page_size - sizeof(uint32_t));
	serialize_uint32(page, sizeof(uint32_t), checksum);
	return checksum;
}

int validate_page_checksum(const void* page, const mini_transaction_engine_stats* stats)
{
	uint32_t checksum = calculate_checksum(page + sizeof(uint32_t), stats->page_size - sizeof(uint32_t));
	return checksum == deserialize_uint32(page, sizeof(uint32_t));
}

uint256 get_pageLSN_for_page(const void* page, const mini_transaction_engine_stats* stats)
{
	return deserialize_uint256(page + sizeof(uint32_t), stats->log_sequence_number_width);
}

int set_pageLSN_for_page(void* page, uint256 pageLSN, const mini_transaction_engine_stats* stats)
{
	serialize_uint256(page + sizeof(uint32_t), stats->log_sequence_number_width, pageLSN);
	return 1;
}

uint64_t is_valid_bits_count_on_free_space_mapper_page(const mini_transaction_engine_stats* stats)
{
	return ((uint64_t)(stats->page_size - sizeof(uint32_t) - stats->log_sequence_number_width)) * CHAR_BIT; // there are CHAR_BIT bits in each byte on the page
}

#define PAGE_POS_MULTIPLIER(stats) (is_valid_bits_count_on_free_space_mapper_page(stats) + UINT64_C(1))

int is_free_space_mapper_page(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return (page_id % PAGE_POS_MULTIPLIER(stats)) == 0;
}

int get_next_free_space_mapper_page_id(uint64_t curr_free_space_mapper_page_id, uint64_t* next_free_space_mapper_page_id, const mini_transaction_engine_stats* stats)
{
	// curr_free_space_mapper_page_id must be a free space mapper page, else we fail
	if(!is_free_space_mapper_page(curr_free_space_mapper_page_id, stats))
		return 0;

	// if the next_free_space_mapper_page_id, could overflow, fail
	if(will_unsigned_sum_overflow(uint64_t, curr_free_space_mapper_page_id, PAGE_POS_MULTIPLIER(stats)))
		return 0;

	// just add the PAGE_POS_MULTIPLIER(stats), and we are done
	(*next_free_space_mapper_page_id) = curr_free_space_mapper_page_id + PAGE_POS_MULTIPLIER(stats);
	return 1;
}

uint64_t get_is_valid_bit_page_id_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return (page_id / PAGE_POS_MULTIPLIER(stats)) * PAGE_POS_MULTIPLIER(stats);
}

uint64_t get_is_valid_bit_position_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return (page_id % PAGE_POS_MULTIPLIER(stats)) - UINT64_C(1);
}

int has_writerLSN_on_page(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return !is_free_space_mapper_page(page_id, stats);
}

uint256 get_writerLSN_for_page(const void* page, const mini_transaction_engine_stats* stats)
{
	return deserialize_uint256(page + sizeof(uint32_t) + stats->log_sequence_number_width, stats->log_sequence_number_width);
}

int set_writerLSN_for_page(void* page, uint256 writerLSN, const mini_transaction_engine_stats* stats)
{
	serialize_uint256(page + sizeof(uint32_t) + stats->log_sequence_number_width, stats->log_sequence_number_width, writerLSN);
	return 1;
}

uint32_t get_system_header_size_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	if(is_free_space_mapper_page(page_id, stats))
		return sizeof(uint32_t) + stats->log_sequence_number_width;
	else
		return sizeof(uint32_t) + (2 * stats->log_sequence_number_width);
}

uint32_t get_page_content_size_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return stats->page_size - get_system_header_size_for_page(page_id, stats);
}

uint32_t get_system_header_size_for_data_pages(const mini_transaction_engine_stats* stats)
{
	return (sizeof(uint32_t) + (2 * stats->log_sequence_number_width));
}

uint32_t get_page_content_size_for_data_pages(const mini_transaction_engine_stats* stats)
{
	return stats->page_size - get_system_header_size_for_data_pages(stats);
}

uint32_t get_page_content_size_for_free_space_mapper_pages(const mini_transaction_engine_stats* stats)
{
	return stats->page_size - (sizeof(uint32_t) + (1 * stats->log_sequence_number_width));
}

void* get_page_contents_for_page(void* page, uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return page + get_system_header_size_for_page(page_id, stats);
}

void* get_page_for_page_contents(void* page_contents, uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return page_contents - get_system_header_size_for_page(page_id, stats);
}

uint64_t get_extent_id_for_page_id(uint64_t page_id, const mini_transaction_engine_stats* stats)
{
	return page_id / PAGE_POS_MULTIPLIER(stats);
}

int is_full_free_space_mapper_page(void* page, const mini_transaction_engine_stats* stats)
{
	// get system header size assuming that it is a free_space_mapper_page, it only has a checksum and a pageLSN on it
	uint32_t system_header_size = sizeof(uint32_t) + stats->log_sequence_number_width;

	// get it's page contents
	char* page_contents = page + system_header_size;

	// and make sure that there is atleast 1 zero-byte, if so return 0 i.e. not full
	for(uint64_t i = 0; i < (stats->page_size - system_header_size); i++)
		if(page_contents[i] != 0xff) // even if a single bit is 0, return 0
			return 0;

	// if all the bits are 1, then this extent is full, so return 1
	return 1;
}
