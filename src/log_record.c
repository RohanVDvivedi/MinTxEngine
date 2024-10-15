#include<log_record.h>

#include<system_page_header_util.h>

#include<stdlib.h>
#include<string.h>

static uint32_t bytes_for_page_index(uint32_t page_size)
{
	if(page_size < (UINT32_C(1) < 8))
		return 1;
	else if(page_size < (UINT32_C(1) < 16))
		return 2;
	else if(page_size < (UINT32_C(1) < 24))
		return 3;
	else
		return 4;
}

log_record_tuple_defs initialize_log_record_tuple_defs(const mini_transaction_engine_stats* stats)
{
	log_record_tuple_defs lrtd = {};

	lrtd.max_log_record_size = stats->page_size * 6; // TODO :: to be configured

	// first initialize the dtis required
	lrtd.page_id_type = define_uint_non_nullable_type("page_id", stats->page_id_width);
	lrtd.LSN_type = define_large_uint_non_nullable_type("LSN", stats->log_sequence_number_width);
	lrtd.page_index_type = define_uint_non_nullable_type("page_index", bytes_for_page_index(stats->page_size));
	lrtd.tuple_positional_accessor_type = get_variable_element_count_array_type("tuple_positional_accessor_type", stats->page_size, &(lrtd.page_index_type));
	lrtd.page_content_in_bytes_type = get_fixed_length_blob_type("page_contents", get_page_content_size_for_data_pages(stats), 0);
	lrtd.data_in_bytes_type = get_variable_length_blob_type("data", stats->page_size);
	lrtd.size_def_in_bytes_type = get_variable_length_blob_type("size_def", 13);
	lrtd.type_info_in_bytes_type = get_variable_length_blob_type("type_info", stats->page_size);

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		if(dti == NULL)
			exit(-1);
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
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "pilr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "old_page_contents");
		dti->containees[3].type_info = &(lrtd.page_content_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_page_header_size");
		dti->containees[4].type_info = &(lrtd.page_index_type);

		strcpy(dti->containees[5].field_name, "new_size_def");
		dti->containees[5].type_info = &(lrtd.size_def_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.pilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
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
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_index_type);

		strcpy(dti->containees[5].field_name, "new_tuple");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(7));
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_index_type);

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		strcpy(dti->containees[6].field_name, "new_tuple");
		dti->containees[6].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tulr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_index_type);

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tdlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_content_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tdalr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_index_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tdttlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_index_type);

		strcpy(dti->containees[5].field_name, "swap_index2");
		dti->containees[5].type_info = &(lrtd.page_index_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tslr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(8));
		if(dti == NULL)
			exit(-1);
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
		dti->containees[4].type_info = &(lrtd.page_index_type);

		strcpy(dti->containees[5].field_name, "element_index");
		dti->containees[5].type_info = &(lrtd.tuple_positional_accessor_type);

		strcpy(dti->containees[6].field_name, "old_element");
		dti->containees[6].type_info = &(lrtd.data_in_bytes_type);

		strcpy(dti->containees[7].field_name, "new_element");
		dti->containees[7].type_info = &(lrtd.data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.tueiplr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "pclr_def", 0, lrtd.max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].type_info = &(lrtd.page_content_in_bytes_type);

		strcpy(dti->containees[5].field_name, "new_page_contents");
		dti->containees[5].type_info = &(lrtd.page_content_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.pclr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "fpwlr_def", 0, lrtd.max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record");
		dti->containees[1].type_info = &(lrtd.LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd.page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd.size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "page_contents");
		dti->containees[4].type_info = &(lrtd.page_content_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd.fpwlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
			exit(-1);
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
		if(dti == NULL)
			exit(-1);
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
		if(dti == NULL)
			exit(-1);
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

log_record parse_log_record(const log_record_tuple_defs* lrtd_p, const void* serialized_log_record, uint32_t serialized_log_record_size)
{
	if(serialized_log_record_size <= 1 || serialized_log_record_size > lrtd_p->max_log_record_size)
		return (log_record){};

	unsigned char log_record_type = ((const unsigned char*)serialized_log_record)[0];
	const void* log_record_contents = serialized_log_record + 1;

	switch(log_record_type)
	{
		default : return (log_record){};
		case PAGE_ALLOCATION :
		{
			log_record lr;
			lr.type = PAGE_ALLOCATION;

			lr.palr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.palr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.palr.page_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			return lr;
		}
		case PAGE_DEALLOCATION :
		{
			log_record lr;
			lr.type = PAGE_DEALLOCATION;

			lr.palr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.palr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.palr.page_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			return lr;
		}
		case PAGE_INIT :
		{
			log_record lr;
			lr.type = PAGE_INIT;

			lr.pilr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pilr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pilr.page_id = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(2), log_record_contents).uint_value;
			lr.pilr.old_page_contents = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(3), log_record_contents).blob_value;
			lr.pilr.new_page_header_size = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value new_size_def = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(5), log_record_contents);
			deserialize_tuple_size_def(&(lr.pilr.new_size_def), new_size_def.blob_value, new_size_def.blob_size);

			return lr;
		}
		case TUPLE_APPEND :
		{
			log_record lr;
			lr.type = TUPLE_APPEND;

			lr.talr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.talr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.talr.page_id = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.talr.size_def), size_def.blob_value, size_def.blob_size);

			user_value new_tuple = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(4), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.talr.new_tuple = NULL;
			else
				lr.talr.new_tuple = new_tuple.blob_value;

			return lr;
		}
		case TUPLE_INSERT :
		{
			log_record lr;
			lr.type = TUPLE_INSERT;

			lr.tilr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tilr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tilr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tilr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tilr.insert_index = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value new_tuple = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.tilr.new_tuple = NULL;
			else
				lr.tilr.new_tuple = new_tuple.blob_value;

			return lr;
		}
		case TUPLE_UPDATE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE;

			lr.tulr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tulr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tulr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tulr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tulr.update_index = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value old_tuple = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&old_tuple))
				lr.tulr.old_tuple = NULL;
			else
				lr.tulr.old_tuple = old_tuple.blob_value;

			user_value new_tuple = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(6), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.tulr.new_tuple = NULL;
			else
				lr.tulr.new_tuple = new_tuple.blob_value;

			return lr;
		}
		case TUPLE_DISCARD :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD;

			lr.tdlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdlr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdlr.discard_index = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value old_tuple = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&old_tuple))
				lr.tdlr.old_tuple = NULL;
			else
				lr.tdlr.old_tuple = old_tuple.blob_value;

			return lr;
		}
		case TUPLE_DISCARD_ALL :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_ALL;

			lr.tdalr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdalr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdalr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdalr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdalr.old_page_contents = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			return lr;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_TRAILING_TOMB_STONES;

			lr.tdttlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdttlr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdttlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdttlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdttlr.discarded_trailing_tomb_stones_count = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			return lr;
		}
		case TUPLE_SWAP :
		{
			log_record lr;
			lr.type = TUPLE_SWAP;

			lr.tslr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tslr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tslr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tslr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tslr.swap_index1 = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(4), log_record_contents).uint_value;
			lr.tslr.swap_index2 = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(5), log_record_contents).uint_value;

			return lr;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE_ELEMENT_IN_PLACE;

			lr.tueiplr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tueiplr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tueiplr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value tpl_def = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(3), log_record_contents);
			int allocation_error = 0;
			data_type_info* dti = deserialize_type_info(tpl_def.blob_value, tpl_def.blob_size, &allocation_error);
			if(dti == NULL)
				exit(-1);
			if(!initialize_tuple_def(&(lr.tueiplr.tpl_def), dti))
				exit(-1);

			lr.tueiplr.tuple_index = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			lr.tueiplr.element_index.positions_length = get_element_count_for_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(5), log_record_contents);
			lr.tueiplr.element_index.positions = malloc(sizeof(uint32_t) * lr.tueiplr.element_index.positions_length);
			for(uint32_t i = 0; i < lr.tueiplr.element_index.positions_length; i++)
				lr.tueiplr.element_index.positions[i] = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(5, i), log_record_contents).uint_value;

			const data_type_info* ele_def = get_type_info_for_element_from_tuple_def(&(lr.tueiplr.tpl_def), lr.tueiplr.element_index);

			user_value old_element = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(6), log_record_contents);
			if(is_user_value_NULL(&old_element))
				lr.tueiplr.old_element = (*NULL_USER_VALUE);
			else if(ele_def->type == BIT_FIELD)
			{
				lr.tueiplr.old_element = get_user_value_for_type_info(UINT_NULLABLE[8], old_element.blob_value);
				lr.tueiplr.old_element.bit_field_value = lr.tueiplr.old_element.uint_value;
			}
			else
				lr.tueiplr.old_element = get_user_value_for_type_info(ele_def, old_element.blob_value);

			user_value new_element = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(7), log_record_contents);
			if(is_user_value_NULL(&new_element))
				lr.tueiplr.new_element = (*NULL_USER_VALUE);
			else if(ele_def->type == BIT_FIELD)
			{
				lr.tueiplr.new_element = get_user_value_for_type_info(UINT_NULLABLE[8], new_element.blob_value);
				lr.tueiplr.new_element.bit_field_value = lr.tueiplr.new_element.uint_value;
			}
			else
				lr.tueiplr.new_element = get_user_value_for_type_info(ele_def, new_element.blob_value);

			return lr;
		}
		case PAGE_CLONE :
		{
			log_record lr;
			lr.type = PAGE_CLONE;

			lr.pclr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pclr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pclr.page_id = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.pclr.size_def), size_def.blob_value, size_def.blob_size);

			lr.pclr.old_page_contents = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(4), log_record_contents).blob_value;
			lr.pclr.new_page_contents = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(5), log_record_contents).blob_value;

			return lr;
		}
		case FULL_PAGE_WRITE :
		{
			log_record lr;
			lr.type = FULL_PAGE_WRITE;

			lr.fpwlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.fpwlr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.fpwlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.fpwlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.fpwlr.page_contents = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			return lr;
		}
		case COMPENSATION_LOG :
		{
			log_record lr;
			lr.type = COMPENSATION_LOG;

			lr.clr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.clr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.clr.undo_of = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(2), log_record_contents).large_uint_value;
			lr.clr.next_log_record_to_undo = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(3), log_record_contents).large_uint_value;

			return lr;
		}
		case ABORT_MINI_TX :
		{
			log_record lr;
			lr.type = ABORT_MINI_TX;

			lr.amtlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.amtlr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;

			return lr;
		}
		case COMPLETE_MINI_TX :
		{
			log_record lr;
			lr.type = COMPLETE_MINI_TX;

			lr.cmtlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.cmtlr.prev_log_record = get_value_from_element_from_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;

			user_value info = get_value_from_element_from_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(2), log_record_contents);
			if(is_user_value_NULL(&info))
				lr.cmtlr.info = NULL;
			else
			{
				lr.cmtlr.info = info.blob_value;
				lr.cmtlr.info_size = info.blob_size;
			}

			return lr;
		}
	}
}

const void* serialized_log_record(const log_record_tuple_defs* lrtd_p, const log_record* lr);

