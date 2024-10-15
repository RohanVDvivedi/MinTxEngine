#ifndef LOG_RECORD_TYPES_H
#define LOG_RECORD_TYPES_H

typedef enum log_record_type log_record_type;
enum log_record_type
{
	PAGE_ALLOCATION = 1,
	PAGE_DEALLOCATION = 2,

	PAGE_INIT = 3,

	TUPLE_APPEND = 4,
	TUPLE_INSERT = 5,
	TUPLE_UPDATE = 6,
	TUPLE_DISCARD = 7,

	TUPLE_DISCARD_ALL = 8,
	TUPLE_DISCARD_TRAILING_TOMB_STONES = 9,

	TUPLE_SWAP = 10,

	TUPLE_UPDATE_ELEMENT_IN_PLACE = 11,

	PAGE_CLONE = 12,

	FULL_PAGE_WRITE = 13,
	// this log record is first written for any page type, the first time it becomes dirty after a checkpoint

	COMPENSATION_LOG = 14,
	// this is the log record type to be used it points to any of the above log types and performs their undo on tha page
};

#endif