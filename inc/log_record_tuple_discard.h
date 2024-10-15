#ifndef LOG_RECORD_TUPLE_DISCARD_H
#define LOG_RECORD_TUPLE_DISCARD_H

// log record struct for TUPLE_DISCARD
// -> undo by inserting the tuple at discard index
typedef struct tuple_discard_log_record tuple_discard_log_record;
struct tuple_discard_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* old_tuple;
	uint32_t discard_index;
};

#endif