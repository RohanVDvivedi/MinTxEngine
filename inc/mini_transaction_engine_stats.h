#ifndef MINI_TRANSACTION_ENGINE_STATS_H
#define MINI_TRANSACTION_ENGINE_STATS_H

#include<stdint.h>

typedef struct mini_transaction_engine_stats mini_transaction_engine_stats;
struct mini_transaction_engine_stats
{
	uint32_t page_size; // size of page in bytes
	uint32_t page_id_width; // bytes required to store page_id
	uint32_t log_sequence_number_width; // required to store log_sequence_number
};

#include<block_io.h>

// 1 means flushed successfully
int write_to_first_block(block_file* bf, const mini_transaction_engine_stats* stats);

// 1 means success
// 0 implies not structured well for being a valid file
int read_from_first_block(block_file* bf, mini_transaction_engine_stats* stats);

typedef struct mini_transaction_engine_user_stats mini_transaction_engine_user_stats;
struct mini_transaction_engine_user_stats
{
	uint32_t page_size; // size of page in bytes available to the user, effectively page_content_size for non free space mapper pages
	uint32_t page_id_width; // bytes required to store page_id, same as as mini_transaction_engne_stats.page_id_width
	uint32_t log_sequence_number_width; // required to store log_sequence_number, same as as mini_transaction_engne_stats.log_sequence_number_width

	uint64_t NULL_PAGE_ID; // zero value, never access this page, ideally never access any page not allocated by the mini transaction engine, it will result in abort

	uint64_t max_page_count; // user is not allowed to access more than this number of pages in the database
};

// generate mini transaction engine user stats from stats
mini_transaction_engine_user_stats get_mini_transaction_engine_user_stats(const mini_transaction_engine_stats* stats, uint32_t database_file_block_size);

#endif