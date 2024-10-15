#ifndef LOG_RECORD_TUPLE_SWAP_H
#define LOG_RECORD_TUPLE_SWAP_H

// log record struct for TUPLE_SWAP
// -> undo by the same swap operation
typedef struct tuple_swap_log_record tuple_swap_log_record;
struct tuple_swap_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	uint32_t swap_index1;
	uint32_t swap_index2;
};

#endif