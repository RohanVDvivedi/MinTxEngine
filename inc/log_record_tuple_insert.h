#ifndef LOG_RECORD_TUPLE_INSERT_H
#define LOG_RECORD_TUPLE_INSERT_H

// log record struct for TUPLE_INSERT
// -> undo by discarding the indexed tuple
typedef struct tuple_insert_log_record tuple_insert_log_record;
struct tuple_insert_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* new_tuple;
	uint32_t insert_index;
};

#endif