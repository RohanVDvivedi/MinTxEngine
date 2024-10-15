#ifndef LOG_RECORD_COMPENSATION_LOG_RECORD_H
#define LOG_RECORD_COMPENSATION_LOG_RECORD_H

typedef struct compensation_log_record compensation_log_record;
struct compensation_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transactionss

	uint256 compensation_of; // this log record is undo log record of

	uint256 next_log_record_to_undo; // this is the prev_log_record value of the log record at compensation_of
};

#endif