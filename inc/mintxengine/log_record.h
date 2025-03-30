#ifndef LOG_RECORD_TYPES_H
#define LOG_RECORD_TYPES_H

#include<stdint.h>

#include<mintxengine/mini_transaction_engine_stats.h>

#include<tuplestore/tuple.h>

typedef enum log_record_type log_record_type;
enum log_record_type
{
	UNIDENTIFIED = 0, // this log record can not be parsed or serialized with functions in this header and source file

// below are log records that can exist in any mini transaction

	PAGE_ALLOCATION = 1,
	PAGE_DEALLOCATION = 2,

	PAGE_INIT = 3,

	PAGE_SET_HEADER = 4,

	TUPLE_APPEND = 5,
	TUPLE_INSERT = 6,
	TUPLE_UPDATE = 7,
	TUPLE_DISCARD = 8,

	TUPLE_DISCARD_ALL = 9,
	TUPLE_DISCARD_TRAILING_TOMB_STONES = 10,

	TUPLE_SWAP = 11,

	TUPLE_UPDATE_ELEMENT_IN_PLACE = 12,

	PAGE_CLONE = 13,

	PAGE_COMPACTION = 14,

	FULL_PAGE_WRITE = 15,
	// this log record is first written for any page type, the first time it becomes dirty after a checkpoint

	COMPENSATION_LOG = 16,
	// this is the log record type to be used it points to any of the above log types and performs their undo on tha page

	ABORT_MINI_TX = 17,
	// informational suggesting abort of the mini transaction

	COMPLETE_MINI_TX = 18,
	// informational suggesting no more log records will be or should be generated for this mini transaction

// below are log records that can exist only in checkpoints one ofter another in consecutive fashion

	CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY = 19,
	CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY = 20,

	CHECKPOINT_END = 21,

// below are log records that are used for the user to log begin, abort and end log records of the higher level transactions

	USER_INFO = 22,
};

/*
	Every log record modifies the pageLSN to their own value
	All log records from 3-14 persistently write lock the page and have a corresponding undo, and get a corresponding COMPENSATION_LOG_RECORD
	  -> while redoing these log records writerLSN becomes the mt->min_transaction_id, while undoing them their CLR records never change the writerLSN
	FULL_PAGE_WRITE log records are always redo-ne, and their undo is a NO-OP
	  -> while redoing them they get writerLSN as in the log record, if their page_id suggests that they have one (if theyr are not a free space mapper page)
*/

extern const char log_record_type_strings[23][64];

/*
	NOTE :: for the first log record for any mini transaction
	mini_transaction_id and prev_log_record_LSN must be 0 i.e. INVALID_LOG_SEQUENCE_NUMBER
*/

// log record struct for PAGE_ALLOCATION and PAGE_DEALLOCATION
// -> undo by deallocation and allocation respectively
typedef struct page_allocation_log_record page_allocation_log_record;
struct page_allocation_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
};

// log record struct for PAGE_INIT
// -> undo by copy pasting the old_page_contents
typedef struct page_init_log_record page_init_log_record;
struct page_init_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;

	// prior_page_contents as is
	const void* old_page_contents;

	// input params for page init
	uint32_t new_page_header_size;
	tuple_size_def new_size_def;
};

// log record struct for PAGE_SET_HEADER
// -> undo by copy pasting the old_page_header
typedef struct page_set_header_log_record page_set_header_log_record;
struct page_set_header_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;

	// prior_page_header as is
	const void* old_page_header_contents;

	// new_page_header as is
	const void* new_page_header_contents;

	// this is not stored in the log record, it is derieved from the blob_size of old_page_header_contents
	uint32_t page_header_size;
};

// log record struct for TUPLE_APPEND
// -> undo by discarding the last tuple
typedef struct tuple_append_log_record tuple_append_log_record;
struct tuple_append_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	const void* new_tuple;
};

// log record struct for TUPLE_INSERT
// -> undo by discarding the indexed tuple
typedef struct tuple_insert_log_record tuple_insert_log_record;
struct tuple_insert_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	uint32_t insert_index;
	const void* new_tuple;
};

// log record struct for TUPLE_UPDATE
// -> undo by reversing the update call, preferably after a compaction if necessary
typedef struct tuple_update_log_record tuple_update_log_record;
struct tuple_update_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
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
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	uint32_t discard_index;
	const void* old_tuple;
};

// log record struct for TUPLE_DISCARD_ALL
// -> undo by copy pasting the old_page_contents
typedef struct tuple_discard_all_log_record tuple_discard_all_log_record;
struct tuple_discard_all_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
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
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	// number of tombstones discarded
	uint32_t discarded_trailing_tomb_stones_count; // ideally ths value is available only after the operation is performed, but you may call get_trailing_tomb_stones_count_on_page() to get this valu prior to applying this transformation
};

// log record struct for TUPLE_SWAP
// -> undo by the same swap operation
typedef struct tuple_swap_log_record tuple_swap_log_record;
struct tuple_swap_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
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
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_def tpl_def; // to be destroyed if parsed, i.e. if serialized_log_record != NULL

	uint32_t tuple_index;
	positional_accessor element_index; // to be destroyed if parsed, i.e. if serialized_log_record != NULL

	user_value old_element;
	user_value new_element;
};

// log record struct for PAGE_CLONE
typedef struct page_clone_log_record page_clone_log_record;
struct page_clone_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def;

	// prior_page_contents as is
	const void* old_page_contents;

	// contents of the page to be cloned from
	const void* new_page_contents;
};

// log record struct for PAGE_COMPACTION
// -> undo is a NO-OP
typedef struct page_compaction_log_record page_compaction_log_record;
struct page_compaction_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;
	tuple_size_def size_def; // this is required for redo
};

// log record struct for FULL_PAGE_WRITE
// -> undo is a NO-OP, in best case you can put back the page_contents back to the page
// for REDO copy page_contents to the page and reset it's pageLSN to LSN of this log record, if it is not a free space mapper page, then also copy the writerLSN
typedef struct full_page_write_log_record full_page_write_log_record;
struct full_page_write_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction
	uint64_t page_id;

	uint256 writerLSN; // writerLSN of the page (if it is not a free space mapper page) at the time of writing this log record
	const void* page_contents; // there is no size def here, because a just allocated page may not have a valid size_def
};

// this type of log record can be redone but never undone
// the redo infomation is acquired from the log record at undo_of
typedef struct compensation_log_record compensation_log_record;
struct compensation_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction

	uint256 undo_of_LSN; // this log record is undo log record of
};

// informational log record
typedef struct abort_mini_tx_log_record abort_mini_tx_log_record;
struct abort_mini_tx_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction

	int abort_error; // reason for abort, this is not needed, but it is provided so that someone reading logs could debug
};

// informational log record
typedef struct complete_mini_tx_log_record complete_mini_tx_log_record;
struct complete_mini_tx_log_record
{
	uint256 mini_transaction_id; // mini_transaction that this log record belongs to
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same mini transaction

	int is_aborted:1; // this bit is set if the mini transaction was aborted

	const void* info; // must not be more than 1 page in size
	uint32_t info_size;
};

#include<mintxengine/mini_transaction.h>
#include<mintxengine/dirty_page_table_entry.h>

typedef struct checkpoint_mini_transaction_table_entry_log_record checkpoint_mini_transaction_table_entry_log_record;
struct checkpoint_mini_transaction_table_entry_log_record
{
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same checkpoint

	mini_transaction mt;
};

typedef struct checkpoint_dirty_page_table_entry_log_record checkpoint_dirty_page_table_entry_log_record;
struct checkpoint_dirty_page_table_entry_log_record
{
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same checkpoint

	dirty_page_table_entry dpte;
};

typedef struct checkpoint_end_log_record checkpoint_end_log_record;
struct checkpoint_end_log_record
{
	uint256 prev_log_record_LSN; // LSN of the previous log record in the WALe for this very same checkpoint

	uint256 begin_LSN; // LSN of the first log record in the WALe for this very same checkpoint
};

typedef struct user_info_log_record user_info_log_record;
struct user_info_log_record
{
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
		page_set_header_log_record pshlr;
		tuple_append_log_record talr;
		tuple_insert_log_record tilr;
		tuple_update_log_record tulr;
		tuple_discard_log_record tdlr;
		tuple_discard_all_log_record tdalr;
		tuple_discard_trailing_tombstones_log_record tdttlr;
		tuple_swap_log_record tslr;
		tuple_update_element_in_place_log_record tueiplr;
		page_clone_log_record pclr;
		page_compaction_log_record pcptlr;
		full_page_write_log_record fpwlr;
		compensation_log_record clr;
		abort_mini_tx_log_record amtlr;
		complete_mini_tx_log_record cmtlr;

		checkpoint_mini_transaction_table_entry_log_record ckptmttelr;
		checkpoint_dirty_page_table_entry_log_record ckptdptelr;
		checkpoint_end_log_record ckptelr;

		user_info_log_record uilr;
	};

	const void* parsed_from;
	uint32_t parsed_from_size;
	// above union may possibly points to data in the parsed_from attribute
	// destroyed and freed once it is no longer in use
};

typedef struct log_record_tuple_defs log_record_tuple_defs;
struct log_record_tuple_defs
{
	uint32_t max_log_record_size;

	data_type_info page_id_type; // type for page_id
	data_type_info LSN_type; // type for log sequence number
	data_type_info page_index_type; // type used for tuple and element_indices
	data_type_info tuple_positional_accessor_type; // to store positional_accessor, a variable sized array of page_index_type
	data_type_info data_in_bytes_type; // BLOB type atmost as big as max_size = page_size, for tuples and elements
	data_type_info size_def_in_bytes_type; // BLOB type atmost as big as 13 bytes -> dictated by tuplestore
	data_type_info type_info_in_bytes_type; // for data_type_info of type_info for tuple types atmost page size bytes
	data_type_info info_in_bytes_type; // BLOB type atmost as big as max_size = 6 * page_size, for user info that gets posted in cmtlr and uilr

	data_type_info* mini_transaction_type; // tuple type that consists of mini_transaction_id, lastLSN and state
	data_type_info* dirty_page_table_entry_type; // tuple type that consists of page_id and recLSN

	// first byte of the log record decides its type

	tuple_def palr_def;
	tuple_def pilr_def;
	tuple_def pshlr_def;
	tuple_def talr_def;
	tuple_def tilr_def;
	tuple_def tulr_def;
	tuple_def tdlr_def;
	tuple_def tdalr_def;
	tuple_def tdttlr_def;
	tuple_def tslr_def;
	tuple_def tueiplr_def;
	tuple_def pclr_def;
	tuple_def pcptlr_def;
	tuple_def fpwlr_def;
	tuple_def clr_def;
	tuple_def amtlr_def;
	tuple_def cmtlr_def;

	tuple_def ckptmttelr_def;
	tuple_def ckptdptelr_def;
	tuple_def ckptelr_def;

	tuple_def uilr_def;
};

// this function is crucial in succeeding the creation of mini_transaction_engine
// it won't fail, it any malloc calls fail, we do an exit(-1)
void initialize_log_record_tuple_defs(log_record_tuple_defs* lrtd, const mini_transaction_engine_stats* stats);

// destroys all memeory allocated by the above function
void deinitialize_log_record_tuple_defs(log_record_tuple_defs* lrtd);

log_record uncompress_and_parse_log_record(const log_record_tuple_defs* lrtd_p, const void* serialized_log_record, uint32_t serialized_log_record_size);

// to be called only on parsed log record, it will also free the memory of the parsed log record
void destroy_and_free_parsed_log_record(log_record* lr);

const void* serialize_and_compress_log_record(const log_record_tuple_defs* lrtd_p, const mini_transaction_engine_stats* stats, const log_record* lr, uint32_t* result_size);

void print_log_record(const log_record* lr, const mini_transaction_engine_stats* stats);

// common getter setter calls

uint256 get_mini_transaction_id_for_log_record(const log_record* lr);
int set_mini_transaction_id_for_log_record(log_record* lr, uint256 mini_transaction_id);

uint256 get_prev_log_record_LSN_for_log_record(const log_record* lr);
int set_prev_log_record_LSN_for_log_record(log_record* lr, uint256 prev_log_record_LSN);

uint64_t get_page_id_for_log_record(const log_record* lr);

#endif