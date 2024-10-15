#ifndef LOG_RECORD_FULL_PAGE_WRITE_H
#define LOG_RECORD_FULL_PAGE_WRITE_H

// log record struct for FULL_PAGE_WRITE
// -> undo is a NO-OP, in best case you can put back the page_contents back to the page
typedef struct full_page_write_log_record full_page_write_log_record;
struct full_page_write_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* page_contents;
};

#endif