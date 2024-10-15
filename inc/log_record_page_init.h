#ifndef LOG_RECORD_PAGE_INIT_H
#define LOG_RECORD_PAGE_INIT_H

// log record struct for PAGE_INIT
typedef struct page_init_log_record page_init_log_record;
struct page_init_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;

	// prior_page_contents as is
	const void* old_page_contents;

	// input params for page init
	uint32_t new_page_header_size;
	tuple_size_def new_size_def;
};

#endif