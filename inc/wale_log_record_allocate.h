#ifndef WALE_ALLOCATION_LOG_RECORD_H
#define WALE_ALLOCATION_LOG_RECORD_H

typedef struct allocation_log_record allocation_log_record;
struct allocation_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
};

// this log record is used for both allocation and deallocation of a page

#endif