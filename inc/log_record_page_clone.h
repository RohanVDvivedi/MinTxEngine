#ifndef LOG_RECORD_PAGE_CLONE_H
#define LOG_RECORD_PAGE_CLONE_H

// log record struct for PAGE_CLONE
typedef struct page_clone_log_record page_clone_log_record;
struct page_clone_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;

	// prior_page_contents as is
	const void* old_page_contents;

	// contents of the page to be cloned from
	const void* new_page_contents;
};

#endif