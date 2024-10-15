#ifndef LOG_RECORD_TUPLE_APPEND_H
#define LOG_RECORD_TUPLE_APPEND_H

// log record struct for TUPLE_APPEND
// -> undo by discarding the last tuple
typedef struct tuple_append_log_record tuple_append_log_record;
struct tuple_append_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* new_tuple;
};

#endif