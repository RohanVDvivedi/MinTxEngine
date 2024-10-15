#include<log_record.h>

#include<stdlib.h>
#include<string.h>

log_record_tuple_defs initialize_log_record_tuple_defs(const mini_transaction_engine_stats* stats)
{
	log_record_tuple_defs lrtd = {};

	lrtd.max_log_record_size = stats->page_size * 5;
	//lsn_width * 2 + page_id_width + page_size + 4 + 13;

	// first initialize the dtis required
	lrtd.page_id_type = define_uint_non_nullable_type("page_id", stats->page_id_width);
	lrtd.LSN_type = define_large_uint_non_nullable_type("LSN", stats->log_sequence_number_width);
	lrtd.data_in_bytes_type = get_variable_length_blob_type("data", stats->page_size);
	lrtd.size_def_in_bytes_type = get_variable_length_blob_type("size_def", 13);
	lrtd.type_info_in_bytes_type = get_variable_length_blob_type("type_info", stats->page_size);

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		initialize_tuple_data_type_info(dti, "palr_def", 0, lrtd.max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.palr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		initialize_tuple_data_type_info(dti, "pilr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "old_page_contents");
		dti->containees[3].type_info = &(lrtd.data_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_page_header_size");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[5].field_name, "new_size_def");
		dti->containees[5].type_info = &(lrtd.size_def_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.pilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		initialize_tuple_data_type_info(dti, "talr_def", 0, lrtd.max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_tuple");
		dti->containees[4].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.talr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		initialize_tuple_data_type_info(dti, "tilr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "insert_index");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[5].field_name, "new_tuple");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(7));
		initialize_tuple_data_type_info(dti, "tulr_def", 0, lrtd.max_log_record_size, 7);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "update_index");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		strcpy(dti->containees[6].field_name, "new_tuple");
		dti->containees[6].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tulr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		initialize_tuple_data_type_info(dti, "tdlr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "discard_index");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tdlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		initialize_tuple_data_type_info(dti, "tdalr_def", 0, lrtd.max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tdalr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		initialize_tuple_data_type_info(dti, "tdttlr_def", 0, lrtd.max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "discarded_trailing_tombstones_count");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		// this shall never fail
		initialize_tuple_def(&(lrtd.tdttlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		initialize_tuple_data_type_info(dti, "tslr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "swap_index1");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[5].field_name, "swap_index2");
		dti->containees[5].type_info = UINT_NON_NULLABLE[4];

		// this shall never fail
		initialize_tuple_def(&(lrtd.tslr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(8));
		initialize_tuple_data_type_info(dti, "tueiplr_def", 0, lrtd.max_log_record_size, 8);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "tpl_def");
		dti->containees[3].type_info = &(lrtd.type_info_in_bytes_type);

		strcpy(dti->containees[4].field_name, "tuple_index");
		dti->containees[4].type_info = UINT_NON_NULLABLE[4];

		strcpy(dti->containees[5].field_name, "element_index");
		dti->containees[5].type_info = NULL; // TODO : add positional accessor as type

		strcpy(dti->containees[6].field_name, "old_element");
		dti->containees[6].type_info = &(lrtd.data_in_bytes_type);

		strcpy(dti->containees[7].field_name, "new_element");
		dti->containees[7].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tueiplr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		initialize_tuple_data_type_info(dti, "pclr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "tpl_def");
		dti->containees[3].type_info = &(lrtd.type_info_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].type_info = &(lrtd.data_in_bytes_type);

		strcpy(dti->containees[5].field_name, "new_page_contents");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.pclr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		initialize_tuple_data_type_info(dti, "fpwlr_def", 0, lrtd.max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "tpl_def");
		dti->containees[3].type_info = &(lrtd.type_info_in_bytes_type);

		strcpy(dti->containees[4].field_name, "page_contents");
		dti->containees[4].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.fpwlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		initialize_tuple_data_type_info(dti, "clr_def", 0, lrtd.max_log_record_size, 4);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "undo_of");
		dti->containees[2].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[3].field_name, "next_log_record_to_undo");
		dti->containees[3].type_info = &(lrtd.LSN_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.clr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(2));
		initialize_tuple_data_type_info(dti, "amtlr_def", 0, lrtd.max_log_record_size, 2);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.amtlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		initialize_tuple_data_type_info(dti, "cmtlr_def", 0, lrtd.max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "info");
		dti->containees[2].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.cmtlr_def), dti);
	}

	return lrtd;
}