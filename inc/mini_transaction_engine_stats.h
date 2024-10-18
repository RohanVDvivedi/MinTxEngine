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

#endif