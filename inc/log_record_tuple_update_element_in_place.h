#ifndef LOG_RECORD_TUPLE_UPDATE_ELEMENT_IN_PLACE_H
#define LOG_RECORD_TUPLE_UPDATE_ELEMENT_IN_PLACE_H

// log record struct for TUPLE_UPDATE_ELEMENT_IN_PLACE
// -> undo by reversing the update call, preferably after a compaction if necessary
typedef struct tuple_update_element_in_place_log_record tuple_update_element_in_place_log_record;
struct tuple_update_element_in_place_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_def tpl_def;

	uint32_t tuple_index;
	positional_accessor element_index;

	const void* old_element;

	const void* new_element;
};

#endif