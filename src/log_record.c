#include<log_record.h>

#include<system_page_header_util.h>

#include<stdlib.h>
#include<string.h>

const char log_record_type_strings[19][64] = {
	"UNIDENTIFIED",
	"PAGE_ALLOCATION",
	"PAGE_DEALLOCATION",
	"PAGE_INIT",
	"PAGE_SET_HEADER",
	"TUPLE_APPEND",
	"TUPLE_INSERT",
	"TUPLE_UPDATE",
	"TUPLE_DISCARD",
	"TUPLE_DISCARD_ALL",
	"TUPLE_DISCARD_TRAILING_TOMB_STONES",
	"TUPLE_SWAP",
	"TUPLE_UPDATE_ELEMENT_IN_PLACE",
	"PAGE_CLONE",
	"PAGE_COMPACTION",
	"FULL_PAGE_WRITE",
	"COMPENSATION_LOG",
	"ABORT_MINI_TX",
	"COMPLETE_MINI_TX"
};

static uint32_t bytes_for_page_index(uint32_t page_size)
{
	if(page_size < (UINT32_C(1) << 8))
		return 1;
	else if(page_size < (UINT32_C(1) << 16))
		return 2;
	else if(page_size < (UINT32_C(1) << 24))
		return 3;
	else
		return 4;
}

void initialize_log_record_tuple_defs(log_record_tuple_defs* lrtd, const mini_transaction_engine_stats* stats)
{
	lrtd->max_log_record_size = stats->page_size * 6; // TODO :: to be configured

	// first initialize the dtis required
	lrtd->page_id_type = define_uint_non_nullable_type("page_id", stats->page_id_width);
	lrtd->LSN_type = define_large_uint_non_nullable_type("LSN", stats->log_sequence_number_width);
	lrtd->page_index_type = define_uint_non_nullable_type("page_index", bytes_for_page_index(stats->page_size));
	lrtd->tuple_positional_accessor_type = get_variable_element_count_array_type("tuple_positional_accessor_type", stats->page_size, &(lrtd->page_index_type));
	// in the below 4 data types 4 is added to include the element count in sizes for the corresponding types
	lrtd->data_in_bytes_type = get_variable_length_blob_type("data", stats->page_size + 4);
	lrtd->size_def_in_bytes_type = get_variable_length_blob_type("size_def", 13 + 4);
	lrtd->type_info_in_bytes_type = get_variable_length_blob_type("type_info", stats->page_size + 4);

	// mark all the above initilaized data types to static
	lrtd->page_id_type.is_static = 1;
	lrtd->LSN_type.is_static = 1;
	lrtd->page_index_type.is_static = 1;
	lrtd->tuple_positional_accessor_type.is_static = 1;
	lrtd->data_in_bytes_type.is_static = 1;
	lrtd->size_def_in_bytes_type.is_static = 1;
	lrtd->type_info_in_bytes_type.is_static = 1;

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "palr_def", 0, lrtd->max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->palr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "pilr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "old_page_contents");
		dti->containees[3].type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_page_header_size");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "new_size_def");
		dti->containees[5].type_info = &(lrtd->size_def_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "pshlr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "old_page_header_contents");
		dti->containees[3].type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_page_header_contents");
		dti->containees[4].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pshlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "talr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_tuple");
		dti->containees[4].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->talr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tilr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "insert_index");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "new_tuple");
		dti->containees[5].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(7));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tulr_def", 0, lrtd->max_log_record_size, 7);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "update_index");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[6].field_name, "new_tuple");
		dti->containees[6].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tulr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tdlr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "discard_index");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tdlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tdalr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tdalr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tdttlr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "discarded_trailing_tombstones_count");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tdttlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tslr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "swap_index1");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "swap_index2");
		dti->containees[5].type_info = &(lrtd->page_index_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tslr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(8));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "tueiplr_def", 0, lrtd->max_log_record_size, 8);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "tpl_def");
		dti->containees[3].type_info = &(lrtd->type_info_in_bytes_type);

		strcpy(dti->containees[4].field_name, "tuple_index");
		dti->containees[4].type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "element_index");
		dti->containees[5].type_info = &(lrtd->tuple_positional_accessor_type);

		strcpy(dti->containees[6].field_name, "old_element");
		dti->containees[6].type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[7].field_name, "new_element");
		dti->containees[7].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tueiplr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "pclr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[5].field_name, "new_page_contents");
		dti->containees[5].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pclr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "pcptlr_def", 0, lrtd->max_log_record_size, 4);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].type_info = &(lrtd->size_def_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pcptlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "fpwlr_def", 0, lrtd->max_log_record_size, 4);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "page_contents");
		dti->containees[3].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->fpwlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "clr_def", 0, lrtd->max_log_record_size, 4);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "undo_of");
		dti->containees[2].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[3].field_name, "next_log_record_to_undo");
		dti->containees[3].type_info = &(lrtd->LSN_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->clr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(2));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "amtlr_def", 0, lrtd->max_log_record_size, 2);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->amtlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "cmtlr_def", 0, lrtd->max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "info");
		dti->containees[2].type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->cmtlr_def), dti);
	}
}

void deinitialize_log_record_tuple_defs(log_record_tuple_defs* lrtd)
{
	free(lrtd->palr_def.type_info);
	free(lrtd->pilr_def.type_info);
	free(lrtd->pshlr_def.type_info);
	free(lrtd->talr_def.type_info);
	free(lrtd->tilr_def.type_info);
	free(lrtd->tulr_def.type_info);
	free(lrtd->tdlr_def.type_info);
	free(lrtd->tdalr_def.type_info);
	free(lrtd->tdttlr_def.type_info);
	free(lrtd->tslr_def.type_info);
	free(lrtd->tueiplr_def.type_info);
	free(lrtd->pclr_def.type_info);
	free(lrtd->pcptlr_def.type_info);
	free(lrtd->fpwlr_def.type_info);
	free(lrtd->clr_def.type_info);
	free(lrtd->amtlr_def.type_info);
	free(lrtd->cmtlr_def.type_info);

	(*lrtd) = (log_record_tuple_defs){};
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
			lr.palr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.palr.page_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case PAGE_DEALLOCATION :
		{
			log_record lr;
			lr.type = PAGE_DEALLOCATION;

			lr.palr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.palr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.palr.page_id = get_value_from_element_from_tuple(&(lrtd_p->palr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case PAGE_INIT :
		{
			log_record lr;
			lr.type = PAGE_INIT;

			lr.pilr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pilr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pilr.page_id = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(2), log_record_contents).uint_value;
			lr.pilr.old_page_contents = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(3), log_record_contents).blob_value;
			lr.pilr.new_page_header_size = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value new_size_def = get_value_from_element_from_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(5), log_record_contents);
			deserialize_tuple_size_def(&(lr.pilr.new_size_def), new_size_def.blob_value, new_size_def.blob_size);

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case PAGE_SET_HEADER :
		{
			log_record lr;
			lr.type = PAGE_SET_HEADER;

			lr.pshlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pshlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pshlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(2), log_record_contents).uint_value;
			lr.pshlr.old_page_header_contents = get_value_from_element_from_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(3), log_record_contents).blob_value;
			lr.pshlr.new_page_header_contents = get_value_from_element_from_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			lr.pshlr.page_header_size = get_value_from_element_from_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(3), log_record_contents).blob_size;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_APPEND :
		{
			log_record lr;
			lr.type = TUPLE_APPEND;

			lr.talr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.talr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.talr.page_id = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.talr.size_def), size_def.blob_value, size_def.blob_size);

			user_value new_tuple = get_value_from_element_from_tuple(&(lrtd_p->talr_def), STATIC_POSITION(4), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.talr.new_tuple = NULL;
			else
				lr.talr.new_tuple = new_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_INSERT :
		{
			log_record lr;
			lr.type = TUPLE_INSERT;

			lr.tilr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tilr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tilr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tilr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tilr.insert_index = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value new_tuple = get_value_from_element_from_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.tilr.new_tuple = NULL;
			else
				lr.tilr.new_tuple = new_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_UPDATE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE;

			lr.tulr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tulr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
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

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_DISCARD :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD;

			lr.tdlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdlr.discard_index = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value old_tuple = get_value_from_element_from_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&old_tuple))
				lr.tdlr.old_tuple = NULL;
			else
				lr.tdlr.old_tuple = old_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_DISCARD_ALL :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_ALL;

			lr.tdalr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdalr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdalr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdalr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdalr.old_page_contents = get_value_from_element_from_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_TRAILING_TOMB_STONES;

			lr.tdttlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdttlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdttlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdttlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdttlr.discarded_trailing_tomb_stones_count = get_value_from_element_from_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_SWAP :
		{
			log_record lr;
			lr.type = TUPLE_SWAP;

			lr.tslr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tslr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tslr.page_id = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tslr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tslr.swap_index1 = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(4), log_record_contents).uint_value;
			lr.tslr.swap_index2 = get_value_from_element_from_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(5), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE_ELEMENT_IN_PLACE;

			lr.tueiplr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tueiplr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
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

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case PAGE_CLONE :
		{
			log_record lr;
			lr.type = PAGE_CLONE;

			lr.pclr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pclr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pclr.page_id = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.pclr.size_def), size_def.blob_value, size_def.blob_size);

			lr.pclr.old_page_contents = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(4), log_record_contents).blob_value;
			lr.pclr.new_page_contents = get_value_from_element_from_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(5), log_record_contents).blob_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case PAGE_COMPACTION :
		{
			log_record lr;
			lr.type = PAGE_COMPACTION;

			lr.pcptlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pcptlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pcptlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = get_value_from_element_from_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.pcptlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case FULL_PAGE_WRITE :
		{
			log_record lr;
			lr.type = FULL_PAGE_WRITE;

			lr.fpwlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.fpwlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.fpwlr.page_id = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			lr.fpwlr.page_contents = get_value_from_element_from_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(3), log_record_contents).blob_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case COMPENSATION_LOG :
		{
			log_record lr;
			lr.type = COMPENSATION_LOG;

			lr.clr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.clr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.clr.undo_of = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(2), log_record_contents).large_uint_value;
			lr.clr.next_log_record_to_undo = get_value_from_element_from_tuple(&(lrtd_p->clr_def), STATIC_POSITION(3), log_record_contents).large_uint_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case ABORT_MINI_TX :
		{
			log_record lr;
			lr.type = ABORT_MINI_TX;

			lr.amtlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.amtlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;

			lr.parsed_from = serialized_log_record;
			return lr;
		}
		case COMPLETE_MINI_TX :
		{
			log_record lr;
			lr.type = COMPLETE_MINI_TX;

			lr.cmtlr.mini_transaction_id = get_value_from_element_from_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.cmtlr.prev_log_record_LSN = get_value_from_element_from_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;

			user_value info = get_value_from_element_from_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(2), log_record_contents);
			if(is_user_value_NULL(&info))
				lr.cmtlr.info = NULL;
			else
			{
				lr.cmtlr.info = info.blob_value;
				lr.cmtlr.info_size = info.blob_size;
			}

			lr.parsed_from = serialized_log_record;
			return lr;
		}
	}
}

void destroy_and_free_parsed_log_record(log_record* lr)
{
	switch(lr->type)
	{
		default :
			break;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			destroy_non_static_type_info_recursively(lr->tueiplr.tpl_def.type_info);
			free(lr->tueiplr.element_index.positions);
			break;
		}
	}

	free((void*)(lr->parsed_from));

	(*lr) = (log_record){};
}

const void* serialize_log_record(const log_record_tuple_defs* lrtd_p, const mini_transaction_engine_stats* stats, const log_record* lr, uint32_t* result_size)
{
	void* result = NULL;
	(*result_size) = 0;

	switch(lr->type)
	{
		default :
		{
			(*result_size) = 0;
			return NULL;
		}
		case PAGE_ALLOCATION :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->palr_def));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = PAGE_ALLOCATION;

			init_tuple(&(lrtd_p->palr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->palr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->palr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->palr.page_id}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->palr_def), result + 1) + 1;
			return result;
		}
		case PAGE_DEALLOCATION :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->palr_def));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = PAGE_DEALLOCATION;

			init_tuple(&(lrtd_p->palr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->palr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->palr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->palr.page_id}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->palr_def), result + 1) + 1;
			return result;
		}
		case PAGE_INIT :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pilr_def)) + (4 + get_page_content_size_for_page(lr->pilr.page_id, stats)) + 16;

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = PAGE_INIT;

			init_tuple(&(lrtd_p->pilr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->pilr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->pilr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->pilr.page_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(3), result + 1, &(user_value){.blob_value = lr->pilr.old_page_contents, .blob_size = get_page_content_size_for_page(lr->pilr.page_id, stats)}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->pilr.new_page_header_size}, UINT32_MAX))
				goto ERROR;

			user_value new_size_def = {.blob_value = (uint8_t [13]){}};
			new_size_def.blob_size = serialize_tuple_size_def(&(lr->pilr.new_size_def), (void*)(new_size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(5), result + 1, &new_size_def, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->pilr_def), result + 1) + 1;
			return result;
		}
		case PAGE_SET_HEADER :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pshlr_def)) + 2 * (4 + lr->pshlr.page_header_size);

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = PAGE_SET_HEADER;

			init_tuple(&(lrtd_p->pshlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->pshlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->pshlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->pshlr.page_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(3), result + 1, &(user_value){.blob_value = lr->pshlr.old_page_header_contents, .blob_size = lr->pshlr.page_header_size}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(4), result + 1, &(user_value){.blob_value = lr->pshlr.new_page_header_contents, .blob_size = lr->pshlr.page_header_size}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->pshlr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_APPEND :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->talr_def)) + 16;
			if(lr->talr.new_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_APPEND;

			init_tuple(&(lrtd_p->talr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->talr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->talr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->talr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->talr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(lr->talr.new_tuple == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(4), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(4), result + 1, &(user_value){.blob_value = lr->talr.new_tuple, .blob_size = get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple)}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->talr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_INSERT :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tilr_def)) + 16;
			if(lr->tilr.new_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_INSERT;

			init_tuple(&(lrtd_p->tilr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tilr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tilr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tilr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->tilr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->tilr.insert_index}, UINT32_MAX))
				goto ERROR;

			if(lr->tilr.new_tuple == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(5), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(5), result + 1, &(user_value){.blob_value = lr->tilr.new_tuple, .blob_size = get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple)}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->tilr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_UPDATE :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tulr_def)) + 16;
			if(lr->tulr.old_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple));
			if(lr->tulr.new_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_UPDATE;

			init_tuple(&(lrtd_p->tulr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tulr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tulr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tulr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->tulr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->tulr.update_index}, UINT32_MAX))
				goto ERROR;

			if(lr->tulr.old_tuple == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(5), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(5), result + 1, &(user_value){.blob_value = lr->tulr.old_tuple, .blob_size = get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple)}, UINT32_MAX))
					goto ERROR;
			}

			if(lr->tulr.new_tuple == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(6), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(6), result + 1, &(user_value){.blob_value = lr->tulr.new_tuple, .blob_size = get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple)}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->tulr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_DISCARD :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tdlr_def)) + 16;
			if(lr->tdlr.old_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_DISCARD;

			init_tuple(&(lrtd_p->tdlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tdlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tdlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tdlr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->tdlr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->tdlr.discard_index}, UINT32_MAX))
				goto ERROR;

			if(lr->tdlr.old_tuple == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(5), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(5), result + 1, &(user_value){.blob_value = lr->tdlr.old_tuple, .blob_size = get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple)}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->tdlr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_DISCARD_ALL :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tdalr_def)) + 16 + (4 + get_page_content_size_for_page(lr->tdalr.page_id, stats));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_DISCARD_ALL;

			init_tuple(&(lrtd_p->tdalr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tdalr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tdalr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tdalr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->tdalr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(4), result + 1, &(user_value){.blob_value = lr->tdalr.old_page_contents, .blob_size = get_page_content_size_for_page(lr->tdalr.page_id, stats)}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->tdalr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tdttlr_def)) + 16;

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_DISCARD_TRAILING_TOMB_STONES;

			init_tuple(&(lrtd_p->tdttlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tdttlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tdttlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tdttlr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->tdttlr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->tdttlr.discarded_trailing_tomb_stones_count}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->tdttlr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_SWAP :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tslr_def)) + 16;

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_SWAP;

			init_tuple(&(lrtd_p->tslr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tslr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tslr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tslr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->tslr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->tslr.swap_index1}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(5), result + 1, &(user_value){.uint_value = lr->tslr.swap_index2}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->tslr_def), result + 1) + 1;
			return result;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			const data_type_info* ele_def = get_type_info_for_element_from_tuple_def(&(lr->tueiplr.tpl_def), lr->tueiplr.element_index);

			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tueiplr_def)) + (4 + get_byte_count_for_serialized_type_info(lr->tueiplr.tpl_def.type_info));
			capacity += (4 + 4 * lr->tueiplr.element_index.positions_length);
			if(!is_user_value_NULL(&(lr->tueiplr.old_element)))
			{
				if(!is_variable_sized_type_info(ele_def))
				{
					if(ele_def->type == BIT_FIELD)
						capacity += (4 + 8);
					else
						capacity += (4 + ele_def->size);
				}
				else
					capacity += (4 + stats->page_size);
			}
			if(!is_user_value_NULL(&(lr->tueiplr.new_element)))
			{
				if(!is_variable_sized_type_info(ele_def))
				{
					if(ele_def->type == BIT_FIELD)
						capacity += (4 + 8);
					else
						capacity += (4 + ele_def->size);
				}
				else
					capacity += (4 + stats->page_size);
			}

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = TUPLE_UPDATE_ELEMENT_IN_PLACE;

			init_tuple(&(lrtd_p->tueiplr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->tueiplr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->tueiplr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->tueiplr.page_id}, UINT32_MAX))
				goto ERROR;

			{
				user_value tpl_def = {.blob_value = malloc(get_byte_count_for_serialized_type_info(lr->tueiplr.tpl_def.type_info))};
				tpl_def.blob_size = serialize_type_info(lr->tueiplr.tpl_def.type_info, (void*)(tpl_def.blob_value));
				if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(3), result + 1, &tpl_def, UINT32_MAX))
				{
					free((void*)(tpl_def.blob_value));
					goto ERROR;
				}
				free((void*)(tpl_def.blob_value));
			}

			if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(4), result + 1, &(user_value){.uint_value = lr->tueiplr.tuple_index}, UINT32_MAX))
				goto ERROR;

			{
				if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(5), result + 1, EMPTY_USER_VALUE, UINT32_MAX))
					goto ERROR;
				if(!expand_element_count_for_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(5), result + 1, 0, lr->tueiplr.element_index.positions_length, UINT32_MAX))
					goto ERROR;
				for(uint32_t i = 0; i < lr->tueiplr.element_index.positions_length; i++)
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(5, i), result + 1, &(user_value){.uint_value = lr->tueiplr.element_index.positions[i]}, UINT32_MAX))
						goto ERROR;
			}

			{
				if(is_user_value_NULL(&(lr->tueiplr.old_element)))
				{
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(6), result + 1, NULL_USER_VALUE, UINT32_MAX))
						goto ERROR;
				}
				else if(ele_def->type == BIT_FIELD)
				{
					user_value e = {.blob_value = (uint8_t [8]){}, .blob_size = 8};
					if(!set_user_value_for_type_info(UINT_NULLABLE[8], (void*)(e.blob_value), 0, UINT32_MAX, &(user_value){.uint_value = lr->tueiplr.old_element.bit_field_value}))
						goto ERROR;
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(6), result + 1, &e, UINT32_MAX))
						goto ERROR;
				}
				else
				{
					user_value e = {.blob_value = malloc(stats->page_size)};
					if(!set_user_value_for_type_info(ele_def, (void*)(e.blob_value), 0, UINT32_MAX, &(lr->tueiplr.old_element)))
					{
						free((void*)(e.blob_value));
						goto ERROR;
					}
					e.blob_size = get_size_for_type_info(ele_def, e.blob_value);
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(6), result + 1, &e, UINT32_MAX))
					{
						free((void*)(e.blob_value));
						goto ERROR;
					}
					free((void*)(e.blob_value));
				}
			}

			{
				if(is_user_value_NULL(&(lr->tueiplr.new_element)))
				{
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(7), result + 1, NULL_USER_VALUE, UINT32_MAX))
						goto ERROR;
				}
				else if(ele_def->type == BIT_FIELD)
				{
					user_value e = {.blob_value = (uint8_t [8]){}, .blob_size = 8};
					if(!set_user_value_for_type_info(UINT_NULLABLE[8], (void*)(e.blob_value), 0, UINT32_MAX, &(user_value){.uint_value = lr->tueiplr.new_element.bit_field_value}))
						goto ERROR;
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(7), result + 1, &e, UINT32_MAX))
						goto ERROR;
				}
				else
				{
					user_value e = {.blob_value = malloc(stats->page_size)};
					if(!set_user_value_for_type_info(ele_def, (void*)(e.blob_value), 0, UINT32_MAX, &(lr->tueiplr.new_element)))
					{
						free((void*)(e.blob_value));
						goto ERROR;
					}
					e.blob_size = get_size_for_type_info(ele_def, e.blob_value);
					if(!set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(7), result + 1, &e, UINT32_MAX))
					{
						free((void*)(e.blob_value));
						goto ERROR;
					}
					free((void*)(e.blob_value));
				}
			}

			(*result_size) = get_tuple_size(&(lrtd_p->tueiplr_def), result + 1) + 1;
			return result;
		}
		case PAGE_CLONE :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pclr_def)) + 16 + 2 * (4 + get_page_content_size_for_page(lr->pclr.page_id, stats));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = PAGE_CLONE;

			init_tuple(&(lrtd_p->pclr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->pclr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->pclr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->pclr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->pclr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(4), result + 1, &(user_value){.blob_value = lr->pclr.old_page_contents, .blob_size = get_page_content_size_for_page(lr->pclr.page_id, stats)}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(5), result + 1, &(user_value){.blob_value = lr->pclr.new_page_contents, .blob_size = get_page_content_size_for_page(lr->pclr.page_id, stats)}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->pclr_def), result + 1) + 1;
			return result;
		}
		case PAGE_COMPACTION :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pcptlr_def)) + 16;

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = PAGE_COMPACTION;

			init_tuple(&(lrtd_p->pcptlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->pcptlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->pcptlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->pcptlr.page_id}, UINT32_MAX))
				goto ERROR;

			user_value size_def = {.blob_value = (uint8_t [13]){}};
			size_def.blob_size = serialize_tuple_size_def(&(lr->pcptlr.size_def), (void*)(size_def.blob_value));
			if(!set_element_in_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(3), result + 1, &size_def, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->pcptlr_def), result + 1) + 1;
			return result;
		}
		case FULL_PAGE_WRITE :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->fpwlr_def)) + 16 + (4 + get_page_content_size_for_page(lr->fpwlr.page_id, stats));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = FULL_PAGE_WRITE;

			init_tuple(&(lrtd_p->fpwlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->fpwlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->fpwlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(2), result + 1, &(user_value){.uint_value = lr->fpwlr.page_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(3), result + 1, &(user_value){.blob_value = lr->fpwlr.page_contents, .blob_size = get_page_content_size_for_page(lr->fpwlr.page_id, stats)}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->fpwlr_def), result + 1) + 1;
			return result;
		}
		case COMPENSATION_LOG :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->clr_def));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = COMPENSATION_LOG;

			init_tuple(&(lrtd_p->clr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->clr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->clr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(2), result + 1, &(user_value){.large_uint_value = lr->clr.undo_of}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(3), result + 1, &(user_value){.large_uint_value = lr->clr.next_log_record_to_undo}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->clr_def), result + 1) + 1;
			return result;
		}
		case ABORT_MINI_TX :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->amtlr_def));

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = ABORT_MINI_TX;

			init_tuple(&(lrtd_p->amtlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->amtlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->amtlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->amtlr_def), result + 1) + 1;
			return result;
		}
		case COMPLETE_MINI_TX :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->cmtlr_def));
			if(lr->cmtlr.info != NULL)
				capacity += (4 + lr->cmtlr.info_size);

			void* result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = COMPLETE_MINI_TX;

			init_tuple(&(lrtd_p->cmtlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->cmtlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->cmtlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(lr->cmtlr.info == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(2), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(2), result + 1, &(user_value){.blob_value = lr->cmtlr.info, .blob_size = lr->cmtlr.info_size}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->cmtlr_def), result + 1) + 1;
			return result;
		}
	}

	ERROR :;
	free(result);
	result = NULL;
	(*result_size) = 0;
	return NULL;
}

void update_prev_log_record_LSN_in_serialized_log_record(const log_record_tuple_defs* lrtd_p, void* serialized_log_record, uint256 prev_log_record_LSN)
{
	switch(((const unsigned char*)serialized_log_record)[0])
	{
		default :
		{
			break;
		}
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			set_element_in_tuple(&(lrtd_p->palr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case PAGE_INIT :
		{
			set_element_in_tuple(&(lrtd_p->pilr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case PAGE_SET_HEADER :
		{
			set_element_in_tuple(&(lrtd_p->pshlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_APPEND :
		{
			set_element_in_tuple(&(lrtd_p->talr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_INSERT :
		{
			set_element_in_tuple(&(lrtd_p->tilr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_UPDATE :
		{
			set_element_in_tuple(&(lrtd_p->tulr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_DISCARD :
		{
			set_element_in_tuple(&(lrtd_p->tdlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_DISCARD_ALL :
		{
			set_element_in_tuple(&(lrtd_p->tdalr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			set_element_in_tuple(&(lrtd_p->tdttlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_SWAP :
		{
			set_element_in_tuple(&(lrtd_p->tslr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			set_element_in_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case PAGE_CLONE :
		{
			set_element_in_tuple(&(lrtd_p->pclr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case PAGE_COMPACTION :
		{
			set_element_in_tuple(&(lrtd_p->pcptlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case FULL_PAGE_WRITE :
		{
			set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case COMPENSATION_LOG :
		{
			set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case ABORT_MINI_TX :
		{
			set_element_in_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
		case COMPLETE_MINI_TX :
		{
			set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(1), serialized_log_record + 1, &(user_value){.large_uint_value = prev_log_record_LSN}, UINT32_MAX);
			break;
		}
	}
}

static void print_blob(const void* data, uint32_t data_size)
{
	for(uint32_t i = 0; i < data_size; i++)
		printf("%02x, ", ((unsigned char*)data)[i]);
}

void print_log_record(const log_record* lr, const mini_transaction_engine_stats* stats)
{
	printf("type : %s\n", log_record_type_strings[lr->type]);

	switch(lr->type)
	{
		default :
			return;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			printf("mini_transaction_id : "); print_uint256(lr->palr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->palr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->palr.page_id);
			return;
		}
		case PAGE_INIT :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pilr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->pilr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pilr.page_id);
			printf("old_page_contents : "); print_blob(lr->pilr.old_page_contents, get_page_content_size_for_page(lr->pilr.page_id, stats)); printf("\n");
			printf("new_page_header_size : %"PRIu32"\n", lr->pilr.new_page_header_size);
			printf("new_size_def : \n"); print_tuple_size_def(&(lr->pilr.new_size_def)); printf("\n");
			return;
		}
		case PAGE_SET_HEADER :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pshlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->pshlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pshlr.page_id);
			printf("old_page_header_contents : "); print_blob(lr->pshlr.old_page_header_contents, lr->pshlr.page_header_size); printf("\n");
			printf("new_page_header_contents : "); print_blob(lr->pshlr.new_page_header_contents, lr->pshlr.page_header_size); printf("\n");
			return;
		}
		case TUPLE_APPEND :
		{
			printf("mini_transaction_id : "); print_uint256(lr->talr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->talr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->talr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->talr.size_def)); printf("\n");
			printf("new_tuple : "); print_blob(lr->talr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple)); printf("\n");
			return;
		}
		case TUPLE_INSERT :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tilr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tilr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tilr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tilr.size_def)); printf("\n");
			printf("insert_index : %"PRIu32"\n", lr->tilr.insert_index);
			printf("new_tuple : "); print_blob(lr->tilr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple)); printf("\n");
			return;
		}
		case TUPLE_UPDATE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tulr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tulr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tulr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tulr.size_def)); printf("\n");
			printf("update_index : %"PRIu32"\n", lr->tulr.update_index);
			printf("old_tuple : "); print_blob(lr->tulr.old_tuple, get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple)); printf("\n");
			printf("new_tuple : "); print_blob(lr->tulr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple)); printf("\n");
			return;
		}
		case TUPLE_DISCARD :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tdlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdlr.size_def)); printf("\n");
			printf("discard_index : %"PRIu32"\n", lr->tdlr.discard_index);
			printf("old_tuple : "); print_blob(lr->tdlr.old_tuple, get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple)); printf("\n");
			return;
		}
		case TUPLE_DISCARD_ALL :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdalr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tdalr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdalr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdalr.size_def)); printf("\n");
			printf("old_page_contents : "); print_blob(lr->tdalr.old_page_contents, get_page_content_size_for_page(lr->tdalr.page_id, stats)); printf("\n");
			return;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdttlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tdttlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdttlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdttlr.size_def)); printf("\n");
			printf("discarded_trailing_tomb_stones_count : %"PRIu32"\n", lr->tdttlr.discarded_trailing_tomb_stones_count);
			return;
		}
		case TUPLE_SWAP :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tslr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tslr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tslr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tslr.size_def)); printf("\n");
			printf("swap_index1 : %"PRIu32"\n", lr->tslr.swap_index1);
			printf("swap_index2 : %"PRIu32"\n", lr->tslr.swap_index2);
			return;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tueiplr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->tueiplr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tueiplr.page_id);
			printf("tpl_def : "); print_tuple_def(&(lr->tueiplr.tpl_def)); printf("\n");
			printf("tuple_index : %"PRIu32"\n", lr->tueiplr.tuple_index);
			printf("element_index : {"); for(uint32_t i = 0; i < lr->tueiplr.element_index.positions_length; i++) printf("%"PRIu32", ", lr->tueiplr.element_index.positions[i]); printf("}\n");
			const data_type_info* ele_def = get_type_info_for_element_from_tuple_def(&(lr->tueiplr.tpl_def), lr->tueiplr.element_index);
			printf("old_element : "); print_user_value(&(lr->tueiplr.old_element), ele_def); printf("\n");
			printf("new_element : "); print_user_value(&(lr->tueiplr.new_element), ele_def); printf("\n");
			return;
		}
		case PAGE_CLONE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pclr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->pclr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pclr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->pclr.size_def)); printf("\n");
			printf("old_page_contents : "); print_blob(lr->pclr.old_page_contents, get_page_content_size_for_page(lr->pclr.page_id, stats)); printf("\n");
			printf("new_page_contents : "); print_blob(lr->pclr.new_page_contents, get_page_content_size_for_page(lr->pclr.page_id, stats)); printf("\n");
			return;
		}
		case PAGE_COMPACTION :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pcptlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->pcptlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pcptlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->pcptlr.size_def)); printf("\n");
			return;
		}
		case FULL_PAGE_WRITE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->fpwlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->fpwlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->fpwlr.page_id);
			printf("page_contents : "); print_blob(lr->fpwlr.page_contents, get_page_content_size_for_page(lr->fpwlr.page_id, stats)); printf("\n");
			return;
		}
		case COMPENSATION_LOG :
		{
			printf("mini_transaction_id : "); print_uint256(lr->clr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->clr.prev_log_record_LSN); printf("\n");
			printf("undo_of : "); print_uint256(lr->clr.undo_of); printf("\n");
			printf("next_log_record_to_undo : "); print_uint256(lr->clr.next_log_record_to_undo); printf("\n");
			return;
		}
		case ABORT_MINI_TX :
		{
			printf("mini_transaction_id : "); print_uint256(lr->amtlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->amtlr.prev_log_record_LSN); printf("\n");
			return;
		}
		case COMPLETE_MINI_TX :
		{
			printf("mini_transaction_id : "); print_uint256(lr->cmtlr.mini_transaction_id); printf("\n");
			printf("prev_log_record : "); print_uint256(lr->cmtlr.prev_log_record_LSN); printf("\n");
			printf("info : "); print_blob(lr->cmtlr.info, lr->cmtlr.info_size); printf("\n");
			return;
		}
	}
}

