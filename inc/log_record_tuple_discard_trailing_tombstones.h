#ifndef LOG_RECORD_TUPLE_DISCARD_TRAILING_TOMBSTONES_H
#define LOG_RECORD_TUPLE_DISCARD_TRAILING_TOMBSTONES_H

// log record struct for TUPLE_DISCARD_TRAILING_TOMBSTONES
// -> undo appending NULLs as many as we discarded
typedef struct tuple_discard_trailing_tombstones_log_record tuple_discard_trailing_tombstones_log_record;
struct tuple_discard_trailing_tombstones_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	// number of tombstones discarded
	uint32_t discarded_trailing_tomb_stones_count;
};

#endif