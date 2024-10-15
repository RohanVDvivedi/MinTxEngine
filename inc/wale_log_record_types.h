#ifndef WALE_LOG_RECORD_TYPES_H
#define WALE_LOG_RECORD_TYPES_H

typedef enum wale_log_record_type wale_log_record_type;
enum wale_log_record_type
{
	PAGE_ALLOCATION,
	PAGE_DEALLOCATION,

	PAGE_INIT,

	TUPLE_APPEND,
	TUPLE_INSERT,
	TUPLE_UPDATE,
	TUPLE_DISCARD,

	TUPLE_DISCARD_ALL,
	TUPLE_DISCARD_TRAILING_TOMB_STONES,

	TUPLE_SWAP,

	TUPLE_UPDATE_ELEMENT_IN_PLACE,

	PAGE_CLONE,
};

#endif