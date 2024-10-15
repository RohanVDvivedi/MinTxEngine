#ifndef LOG_RECORD_PAGE_ALLOCATION_H
#define LOG_RECORD_PAGE_ALLOCATION_H

// log record struct for PAGE_ALLOCATION and PAGE_DEALLOCATION
// -> undo by deallocation and allocation respectively
typedef struct page_allocation_log_record page_allocation_log_record;
struct page_allocation_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
};

#endif