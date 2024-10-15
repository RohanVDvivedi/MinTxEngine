#include<log_record.h>

log_record_tuple_defs initialize_log_record_tuple_defs(const mini_transaction_engine_stats* stats)
{
	log_record_tuple_defs lrtd = {};

	// first initialize the dtis required
	lrtd.page_id_type = define_uint_non_nullable_type("page_id", stats->page_id_width);
	lrtd.LSN_type = define_large_uint_non_nullable_type("LSN", stats->log_sequence_number_width);
	lrtd.data_in_bytes_type = get_variable_length_blob_type("data", stats->page_size);
	lrtd.size_info_in_bytes_type = get_variable_length_blob_type("size_info", 13);
	lrtd.type_info_in_bytes_type = get_variable_length_blob_type("type_info", stats->page_size);

	return lrtd;
}