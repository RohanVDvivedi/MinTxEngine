#ifndef LOG_RECORD_TUPLE_UPDATE_H
#define LOG_RECORD_TUPLE_UPDATE_H

// log record struct for TUPLE_UPDATE
// -> undo by reversing the update call, preferably after a compaction if necessary
typedef struct tuple_update_log_record tuple_update_log_record;
struct tuple_update_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	uint32_t update_index;

	const void* old_tuple;

	const void* new_tuple;
};

#endif