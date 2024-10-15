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

	ABORT_MINI_TX = 15,
	// informational suggesting abort of the mini transaction

	COMPLETE_MINI_TX = 16,
	// informational suggesting no more log records will be or should be generated for this mini transaction
};

// log record struct for PAGE_ALLOCATION and PAGE_DEALLOCATION
// -> undo by deallocation and allocation respectively
typedef struct page_allocation_log_record page_allocation_log_record;
struct page_allocation_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
};

// log record struct for PAGE_INIT
// -> undo by copy pasting the old_page_contents
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

// log record struct for TUPLE_APPEND
// -> undo by discarding the last tuple
typedef struct tuple_append_log_record tuple_append_log_record;
struct tuple_append_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* new_tuple;
};

// log record struct for TUPLE_INSERT
// -> undo by discarding the indexed tuple
typedef struct tuple_insert_log_record tuple_insert_log_record;
struct tuple_insert_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* new_tuple;
	uint32_t insert_index;
};

// log record struct for TUPLE_UPDATE
// -> undo by reversing the update call, preferably after a compaction if necessary
typedef struct tuple_update_log_record tuple_update_log_record;
struct tuple_update_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	uint32_t update_index;

	const void* old_tuple;

	const void* new_tuple;
};

// log record struct for TUPLE_DISCARD
// -> undo by inserting the tuple at discard index
typedef struct tuple_discard_log_record tuple_discard_log_record;
struct tuple_discard_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* old_tuple;
	uint32_t discard_index;
};

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

// log record struct for TUPLE_DISCARD_TRAILING_TOMBSTONES
// -> undo appending NULLs as many as we discarded
typedef struct tuple_discard_trailing_tombstones_log_record tuple_discard_trailing_tombstones_log_record;
struct tuple_discard_trailing_tombstones_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	// number of tombstones discarded
	uint32_t discarded_trailing_tomb_stones_count;
};

// log record struct for TUPLE_SWAP
// -> undo by the same swap operation
typedef struct tuple_swap_log_record tuple_swap_log_record;
struct tuple_swap_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	uint32_t swap_index1;
	uint32_t swap_index2;
};

// log record struct for TUPLE_UPDATE_ELEMENT_IN_PLACE
// -> undo by reversing the update call, preferably after a compaction if necessary
typedef struct tuple_update_element_in_place_log_record tuple_update_element_in_place_log_record;
struct tuple_update_element_in_place_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_def tpl_def; // to be destroyed if parsed

	uint32_t tuple_index;
	positional_accessor element_index;

	const void* old_element;

	const void* new_element;
};

// log record struct for PAGE_CLONE
typedef struct page_clone_log_record page_clone_log_record;
struct page_clone_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	// prior_page_contents as is
	const void* old_page_contents;

	// contents of the page to be cloned from
	const void* new_page_contents;
};

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

// this type of log record can be redone but never undone
// the redo infomation is acquired from the log record at undo_of
typedef struct compensation_log_record compensation_log_record;
struct compensation_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transactionss

	uint256 undo_of; // this log record is undo log record of

	uint256 next_log_record_to_undo; // this is the prev_log_record value of the log record at compensation_of
};

// informational log record
typedef struct abort_mini_tx_log_record abort_mini_tx_log_record;
struct abort_mini_tx_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transactionss
};

// informational log record
typedef struct complete_mini_tx_log_record complete_mini_tx_log_record;
struct complete_mini_tx_log_record
{
	uint256 prev_log_record; // LSN of the previous log record in the WALe for this very same mini transactionss

	const void* info; // must not be more than 1 page in size
	uint32_t info_size;
};

typedef struct log_record log_record;
struct log_record
{
	log_record_type type;

	union
	{
		page_allocation_log_record palr;
		page_init_log_record pilr;
		tuple_append_log_record talr;
		tuple_insert_log_record tilr;
		tuple_update_log_record tulr;
		tuple_discard_log_record tdlr;
		tuple_discard_all_log_record tdalr;
		tuple_discard_trailing_tombstones_log_record tdttlr;
		tuple_swap_log_record tslr;
		tuple_update_element_in_place_log_record tueiplr;
		page_clone_log_record pclr;
		full_page_write_log_record fpwlr;
		compensation_log_record clr;
		abort_mini_tx_log_record amtlr;
		complete_mini_tx_log_record cmtlr;
	};

	const void* serialized_log_record;
	// this memory if not null should be freed when this object is no longer in use
};

typedef struct log_record_tuple_defs log_record_tuple_defs;
struct log_record_tuple_defs
{
	uint32_t max_log_record_size;

	data_type_info page_id_type; // type for page_id
	data_type_info LSN_type; // type for log sequence number
	data_type_info data_in_bytes_type; // BLOB type atmost as big as max_size = page_size, for tuples and elements
	data_type_info size_info_in_bytes_type; // BLOB type atmost as big as 13 bytes -> dictated by tuplestore
	data_type_info type_info_in_bytes_type; // for data_type_info of type_info for tuple types atmost page size bytes

	// first byte of the log record decides its type

	tuple_def palr_def;
	tuple_def pilr_def;
	tuple_def talr_def;
	tuple_def tilr_def;
	tuple_def tulr_def;
	tuple_def tdlr_def;
	tuple_def tdalr_def;
	tuple_def tdttlr_def;
	tuple_def tslr_def;
	tuple_def tueiplr_def;
	tuple_def pclr_def;
	tuple_def fpwlr_def;
	tuple_def clr_def;
	tuple_def amtlr_def;
	tuple_def cmtlr_def;
};

// this function is crucial insucceeding the creation of mini_transaction_engine
// it won't fail, it any malloc calls fail, we do an exit(-1)
// we do not have a means to destroy what it initialized so a ctrl+c is what we need
// i.e. it leaks one time minimal memory, only a fool would call this function in a loop
log_record_tuple_defs initialize_log_record_tuple_defs(const mini_transaction_engine_stats* stats);

#endif