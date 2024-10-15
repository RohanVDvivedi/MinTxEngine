#ifndef MINI_TRANSACTION_ENGINE_STATS_H
#define MINI_TRANSACTION_ENGINE_STATS_H

typedef struct mini_transaction_engine_stats mini_transaction_engine_stats;
struct mini_transaction_engine_stats
{
	uint32_t log_sequence_number_width; // required to store log_sequence_number

	uint32_t page_id_width; // bytes required to store page_id

	uint32_t tuple_count_width; // bytes_required to store of tuple_count and tuple_index-es

	uint32_t page_size; // size of page in bytes
};

#endif