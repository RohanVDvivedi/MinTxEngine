#ifndef LOG_RECORD_TUPLE_DISCARD_ALL_H
#define LOG_RECORD_TUPLE_DISCARD_ALL_H

// log record struct for TUPLE_DISCARD_ALL
// -> undo by copy pasting the old_page_contents
typedef struct tuple_discard_all_log_record tuple_discard_all_log_record;
struct tuple_discard_all_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	// prior_page_contents as is
	const void* old_page_contents;
};

#endif