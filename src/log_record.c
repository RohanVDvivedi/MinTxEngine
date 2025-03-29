#include<log_record.h>

#include<system_page_header_util.h>

#include<zlib.h>

#include<stdlib.h>
#include<string.h>

const char log_record_type_strings[23][64] = {
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
	"COMPLETE_MINI_TX",
	"CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY",
	"CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY",
	"CHECKPOINT_END",
	"USER_INFO",
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
	if(will_unsigned_mul_overflow(uint32_t, stats->page_size, 20))
	{
		printf("ISSUE :: page_size to big for log_record_tuple_defs, (page_size * 20) must for 32 bit unsigned integer\n");
		exit(-1);
	}
	lrtd->max_log_record_size = stats->page_size * 20;

	// first initialize the dtis required
	lrtd->page_id_type = define_uint_non_nullable_type("page_id", stats->page_id_width);
	lrtd->LSN_type = define_large_uint_non_nullable_type("LSN", stats->log_sequence_number_width);
	lrtd->page_index_type = define_uint_non_nullable_type("page_index", bytes_for_page_index(stats->page_size));
	lrtd->tuple_positional_accessor_type = get_variable_element_count_array_type("tuple_positional_accessor_type", stats->page_size, &(lrtd->page_index_type));
	// in the below 4 data types 4 is added to include the element count in sizes for the corresponding types
	lrtd->data_in_bytes_type = get_variable_length_blob_type("data", stats->page_size + 4);
	lrtd->size_def_in_bytes_type = get_variable_length_blob_type("size_def", 13 + 4);
	lrtd->type_info_in_bytes_type = get_variable_length_blob_type("type_info", 4 * stats->page_size + 4);
	lrtd->info_in_bytes_type = get_variable_length_blob_type("info_data_type", 6 * stats->page_size + 4);

	// mark all the above initilaized data types to static
	lrtd->page_id_type.is_static = 1;
	lrtd->LSN_type.is_static = 1;
	lrtd->page_index_type.is_static = 1;
	lrtd->tuple_positional_accessor_type.is_static = 1;
	lrtd->data_in_bytes_type.is_static = 1;
	lrtd->size_def_in_bytes_type.is_static = 1;
	lrtd->type_info_in_bytes_type.is_static = 1;

	{
		lrtd->mini_transaction_type = malloc(sizeof_tuple_data_type_info(4));
		if(lrtd->mini_transaction_type == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(lrtd->mini_transaction_type, "mini_transaction", 0, lrtd->max_log_record_size, 4);

		strcpy(lrtd->mini_transaction_type->containees[0].field_name, "mini_transaction_id");
		lrtd->mini_transaction_type->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(lrtd->mini_transaction_type->containees[1].field_name, "lastLSN");
		lrtd->mini_transaction_type->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(lrtd->mini_transaction_type->containees[2].field_name, "state");
		lrtd->mini_transaction_type->containees[2].al.type_info = UINT_NON_NULLABLE[1];

		strcpy(lrtd->mini_transaction_type->containees[3].field_name, "abort_error");
		lrtd->mini_transaction_type->containees[3].al.type_info = INT_NON_NULLABLE[sizeof(int)];
	}

	{
		lrtd->dirty_page_table_entry_type = malloc(sizeof_tuple_data_type_info(2));
		if(lrtd->dirty_page_table_entry_type == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(lrtd->dirty_page_table_entry_type, "dirty_page_table_entry", 0, lrtd->max_log_record_size, 2);

		strcpy(lrtd->dirty_page_table_entry_type->containees[0].field_name, "page_id");
		lrtd->dirty_page_table_entry_type->containees[0].al.type_info = &(lrtd->page_id_type);

		strcpy(lrtd->dirty_page_table_entry_type->containees[1].field_name, "recLSN");
		lrtd->dirty_page_table_entry_type->containees[1].al.type_info = &(lrtd->LSN_type);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "palr_def", 0, lrtd->max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->palr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "pilr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "old_page_contents");
		dti->containees[3].al.type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_page_header_size");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "new_size_def");
		dti->containees[5].al.type_info = &(lrtd->size_def_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "pshlr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "old_page_header_contents");
		dti->containees[3].al.type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_page_header_contents");
		dti->containees[4].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pshlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "talr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "new_tuple");
		dti->containees[4].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->talr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tilr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "insert_index");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "new_tuple");
		dti->containees[5].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tilr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(7));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tulr_def", 0, lrtd->max_log_record_size, 7);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "update_index");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].al.type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[6].field_name, "new_tuple");
		dti->containees[6].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tulr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tdlr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "discard_index");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "old_tuple");
		dti->containees[5].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tdlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tdalr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tdalr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tdttlr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "discarded_trailing_tombstones_count");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tdttlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tslr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "swap_index1");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "swap_index2");
		dti->containees[5].al.type_info = &(lrtd->page_index_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tslr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(8));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "tueiplr_def", 0, lrtd->max_log_record_size, 8);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "tpl_def");
		dti->containees[3].al.type_info = &(lrtd->type_info_in_bytes_type);

		strcpy(dti->containees[4].field_name, "tuple_index");
		dti->containees[4].al.type_info = &(lrtd->page_index_type);

		strcpy(dti->containees[5].field_name, "element_index");
		dti->containees[5].al.type_info = &(lrtd->tuple_positional_accessor_type);

		strcpy(dti->containees[6].field_name, "old_element");
		dti->containees[6].al.type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[7].field_name, "new_element");
		dti->containees[7].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->tueiplr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(6));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "pclr_def", 0, lrtd->max_log_record_size, 6);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		strcpy(dti->containees[4].field_name, "old_page_contents");
		dti->containees[4].al.type_info = &(lrtd->data_in_bytes_type);

		strcpy(dti->containees[5].field_name, "new_page_contents");
		dti->containees[5].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pclr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "pcptlr_def", 0, lrtd->max_log_record_size, 4);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "size_def");
		dti->containees[3].al.type_info = &(lrtd->size_def_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->pcptlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(5));
		if(dti == NULL)
			exit(-1);
		initialize_tuple_data_type_info(dti, "fpwlr_def", 0, lrtd->max_log_record_size, 5);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "page_id");
		dti->containees[2].al.type_info = &(lrtd->page_id_type);

		strcpy(dti->containees[3].field_name, "writerLSN");
		dti->containees[3].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[4].field_name, "page_contents");
		dti->containees[4].al.type_info = &(lrtd->data_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->fpwlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "clr_def", 0, lrtd->max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "undo_of_LSN");
		dti->containees[2].al.type_info = &(lrtd->LSN_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->clr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(3));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "amtlr_def", 0, lrtd->max_log_record_size, 3);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "abort_error");
		dti->containees[2].al.type_info = INT_NON_NULLABLE[4];

		// this shall never fail
		initialize_tuple_def(&(lrtd->amtlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(4));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "cmtlr_def", 0, lrtd->max_log_record_size, 4);

		strcpy(dti->containees[0].field_name, "mini_transaction_id");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "prev_log_record_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[2].field_name, "is_aborted");
		dti->containees[2].al.type_info = BIT_FIELD_NON_NULLABLE[1];

		strcpy(dti->containees[3].field_name, "info");
		dti->containees[3].al.type_info = &(lrtd->info_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->cmtlr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(2));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "ckptmttelr_def", 0, lrtd->max_log_record_size, 2);

		strcpy(dti->containees[0].field_name, "prev_log_record_LSN");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "mini_transaction");
		dti->containees[1].al.type_info = lrtd->mini_transaction_type;

		// this shall never fail
		initialize_tuple_def(&(lrtd->ckptmttelr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(2));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "ckptdptelr_def", 0, lrtd->max_log_record_size, 2);

		strcpy(dti->containees[0].field_name, "prev_log_record_LSN");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "dirty_page_table_entry");
		dti->containees[1].al.type_info = lrtd->dirty_page_table_entry_type;

		// this shall never fail
		initialize_tuple_def(&(lrtd->ckptdptelr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(2));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "ckptelr_def", 0, lrtd->max_log_record_size, 2);

		strcpy(dti->containees[0].field_name, "prev_log_record_LSN");
		dti->containees[0].al.type_info = &(lrtd->LSN_type);

		strcpy(dti->containees[1].field_name, "checkpoint_begin_LSN");
		dti->containees[1].al.type_info = &(lrtd->LSN_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->ckptelr_def), dti);
	}

	{
		data_type_info* dti = malloc(sizeof_tuple_data_type_info(1));
		if(dti == NULL)
		{
			printf("ISSUE :: unable to allocate memory for log record tuple definitions\n");
			exit(-1);
		}
		initialize_tuple_data_type_info(dti, "uilr_def", 0, lrtd->max_log_record_size, 1);

		strcpy(dti->containees[0].field_name, "info");
		dti->containees[0].al.type_info = &(lrtd->info_in_bytes_type);

		// this shall never fail
		initialize_tuple_def(&(lrtd->uilr_def), dti);
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

	free(lrtd->ckptmttelr_def.type_info);
	free(lrtd->ckptdptelr_def.type_info);
	free(lrtd->ckptelr_def.type_info);

	free(lrtd->uilr_def.type_info);

	free(lrtd->mini_transaction_type);
	free(lrtd->dirty_page_table_entry_type);

	(*lrtd) = (log_record_tuple_defs){};
}

// input is always consumed and freed
static void* uncompress_serialized_log_record_idempotently(void* input, uint32_t input_size, uint32_t* output_size)
{
	if(input_size == 0)
	{
		printf("ISSUE :: invalid serialized log record of size 0, requested to be uncompressed\n");
		exit(-1);
	}

	// if the log record is already uncompressed , return input as is
	if(!(((char*)input)[0] & (1<<7)))
	{
		(*output_size) = input_size;
		return input;
	}

	{
		// initialize output
		uint32_t output_capacity = 50;
		void* output = malloc(50);
		if(output == NULL)
		{
			printf("ISSUE :: failure to allocate memory for uncompression of log record\n");
			exit(-1);
		}

		// consume first byte
		((char*)output)[0] = ((char*)input)[0] & (~(1<<7));

		z_stream zstrm;
		zstrm.zalloc = Z_NULL;
		zstrm.zfree = Z_NULL;
		zstrm.opaque = Z_NULL;

		zstrm.next_in = input + 1;
		zstrm.avail_in = input_size - 1;

		zstrm.next_out = output + 1;
		zstrm.avail_out = output_capacity - 1;

		if(Z_OK != inflateInit(&zstrm))
		{
			printf("ISSUE :: failure to initialize zlib uncompression stream for uncompressing log record\n");
			exit(-1);
		}

		while(1)
		{
			int res = inflate(&zstrm, Z_FINISH);

			if(res == Z_OK || res == Z_BUF_ERROR)
			{
				uint32_t new_output_capacity = output_capacity * 2;
				output = realloc(output, new_output_capacity);
				if(output == NULL)
				{
					printf("ISSUE :: failure to allocate memory for uncompression of log record\n");
					exit(-1);
				}

				zstrm.next_out = output + output_capacity;
				zstrm.avail_out = new_output_capacity - output_capacity;

				output_capacity = new_output_capacity;
			}
			else if(res == Z_STREAM_END)
				break;
			else
			{
				printf("ISSUE :: %d error encountered while uncompressing log record inside zlib\n", res);
				exit(-1);
			}
		}

		(*output_size) = zstrm.total_out + 1;

		inflateEnd(&zstrm);

		free(input);
		return output;
	}
}

static inline const user_value getter_for_attribute_of_uncompressed_log_record_contents(const tuple_def* def, const positional_accessor pa, const void* log_record_contents)
{
	user_value uval;
	if(!get_value_from_element_from_tuple(&uval, def, pa, log_record_contents))
	{
		printf("ISSUE :: failed to get attribute from log record contents for parsing after uncompressing\n");
		exit(-1);
	}
	return uval;
}

log_record uncompress_and_parse_log_record(const log_record_tuple_defs* lrtd_p, const void* serialized_log_record, uint32_t serialized_log_record_size)
{
	if(serialized_log_record_size <= 1 || serialized_log_record_size > lrtd_p->max_log_record_size)
		return (log_record){};

	// uncompress it before parsing
	serialized_log_record = uncompress_serialized_log_record_idempotently((void*)serialized_log_record, serialized_log_record_size, &serialized_log_record_size);

	unsigned char log_record_type = ((const unsigned char*)serialized_log_record)[0];
	const void* log_record_contents = serialized_log_record + 1;

	switch(log_record_type)
	{
		default : return (log_record){.type = UNIDENTIFIED, .parsed_from = serialized_log_record, .parsed_from_size = serialized_log_record_size};
		case PAGE_ALLOCATION :
		{
			log_record lr;
			lr.type = PAGE_ALLOCATION;

			lr.palr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->palr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.palr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->palr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.palr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->palr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_DEALLOCATION :
		{
			log_record lr;
			lr.type = PAGE_DEALLOCATION;

			lr.palr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->palr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.palr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->palr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.palr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->palr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_INIT :
		{
			log_record lr;
			lr.type = PAGE_INIT;

			lr.pilr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pilr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pilr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pilr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pilr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pilr_def), STATIC_POSITION(2), log_record_contents).uint_value;
			lr.pilr.old_page_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pilr_def), STATIC_POSITION(3), log_record_contents).blob_value;
			lr.pilr.new_page_header_size = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pilr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value new_size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pilr_def), STATIC_POSITION(5), log_record_contents);
			deserialize_tuple_size_def(&(lr.pilr.new_size_def), new_size_def.blob_value, new_size_def.blob_size);

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_SET_HEADER :
		{
			log_record lr;
			lr.type = PAGE_SET_HEADER;

			lr.pshlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pshlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pshlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pshlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pshlr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pshlr_def), STATIC_POSITION(2), log_record_contents).uint_value;
			lr.pshlr.old_page_header_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pshlr_def), STATIC_POSITION(3), log_record_contents).blob_value;
			lr.pshlr.new_page_header_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pshlr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			lr.pshlr.page_header_size = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pshlr_def), STATIC_POSITION(3), log_record_contents).blob_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_APPEND :
		{
			log_record lr;
			lr.type = TUPLE_APPEND;

			lr.talr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->talr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.talr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->talr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.talr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->talr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->talr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.talr.size_def), size_def.blob_value, size_def.blob_size);

			user_value new_tuple = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->talr_def), STATIC_POSITION(4), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.talr.new_tuple = NULL;
			else
				lr.talr.new_tuple = new_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_INSERT :
		{
			log_record lr;
			lr.type = TUPLE_INSERT;

			lr.tilr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tilr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tilr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tilr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tilr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tilr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tilr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tilr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tilr.insert_index = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tilr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value new_tuple = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tilr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.tilr.new_tuple = NULL;
			else
				lr.tilr.new_tuple = new_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_UPDATE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE;

			lr.tulr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tulr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tulr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tulr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tulr.update_index = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value old_tuple = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&old_tuple))
				lr.tulr.old_tuple = NULL;
			else
				lr.tulr.old_tuple = old_tuple.blob_value;

			user_value new_tuple = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tulr_def), STATIC_POSITION(6), log_record_contents);
			if(is_user_value_NULL(&new_tuple))
				lr.tulr.new_tuple = NULL;
			else
				lr.tulr.new_tuple = new_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_DISCARD :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD;

			lr.tdlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdlr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdlr.discard_index = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdlr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			user_value old_tuple = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdlr_def), STATIC_POSITION(5), log_record_contents);
			if(is_user_value_NULL(&old_tuple))
				lr.tdlr.old_tuple = NULL;
			else
				lr.tdlr.old_tuple = old_tuple.blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_DISCARD_ALL :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_ALL;

			lr.tdalr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdalr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdalr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdalr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdalr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdalr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdalr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdalr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdalr.old_page_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdalr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_TRAILING_TOMB_STONES;

			lr.tdttlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdttlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tdttlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdttlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tdttlr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdttlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdttlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tdttlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tdttlr.discarded_trailing_tomb_stones_count = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tdttlr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_SWAP :
		{
			log_record lr;
			lr.type = TUPLE_SWAP;

			lr.tslr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tslr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tslr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tslr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tslr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tslr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tslr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.tslr.size_def), size_def.blob_value, size_def.blob_size);

			lr.tslr.swap_index1 = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tslr_def), STATIC_POSITION(4), log_record_contents).uint_value;
			lr.tslr.swap_index2 = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tslr_def), STATIC_POSITION(5), log_record_contents).uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE_ELEMENT_IN_PLACE;

			lr.tueiplr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.tueiplr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.tueiplr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value tpl_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(3), log_record_contents);
			int allocation_error = 0;
			data_type_info* dti = deserialize_type_info(tpl_def.blob_value, tpl_def.blob_size, &allocation_error);
			if(dti == NULL)
			{
				printf("ISSUE :: failure to deserialize a data type info from the log record\n");
				exit(-1);
			}
			if(!initialize_tuple_def(&(lr.tueiplr.tpl_def), dti))
			{
				printf("ISSUE :: failure to initialize tuple type info from the log record\n");
				exit(-1);
			}

			lr.tueiplr.tuple_index = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(4), log_record_contents).uint_value;

			lr.tueiplr.element_index.positions_length = get_element_count_for_element_from_tuple(&(lrtd_p->tueiplr_def), STATIC_POSITION(5), log_record_contents);
			lr.tueiplr.element_index.positions = malloc(sizeof(uint32_t) * lr.tueiplr.element_index.positions_length);
			for(uint32_t i = 0; i < lr.tueiplr.element_index.positions_length; i++)
				lr.tueiplr.element_index.positions[i] = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(5, i), log_record_contents).uint_value;

			const data_type_info* ele_def = get_type_info_for_element_from_tuple_def(&(lr.tueiplr.tpl_def), lr.tueiplr.element_index);

			user_value old_element = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(6), log_record_contents);
			if(is_user_value_NULL(&old_element))
				lr.tueiplr.old_element = (*NULL_USER_VALUE);
			else if(ele_def->type == BIT_FIELD)
			{
				get_user_value_for_type_info(&(lr.tueiplr.old_element), UINT_NULLABLE[8], old_element.blob_value);
				lr.tueiplr.old_element.bit_field_value = lr.tueiplr.old_element.uint_value;
			}
			else
				get_user_value_for_type_info(&(lr.tueiplr.old_element), ele_def, old_element.blob_value);

			user_value new_element = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->tueiplr_def), STATIC_POSITION(7), log_record_contents);
			if(is_user_value_NULL(&new_element))
				lr.tueiplr.new_element = (*NULL_USER_VALUE);
			else if(ele_def->type == BIT_FIELD)
			{
				get_user_value_for_type_info(&(lr.tueiplr.new_element), UINT_NULLABLE[8], new_element.blob_value);
				lr.tueiplr.new_element.bit_field_value = lr.tueiplr.new_element.uint_value;
			}
			else
				get_user_value_for_type_info(&(lr.tueiplr.new_element), ele_def, new_element.blob_value);

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_CLONE :
		{
			log_record lr;
			lr.type = PAGE_CLONE;

			lr.pclr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pclr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pclr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pclr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pclr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pclr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pclr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.pclr.size_def), size_def.blob_value, size_def.blob_size);

			lr.pclr.old_page_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pclr_def), STATIC_POSITION(4), log_record_contents).blob_value;
			lr.pclr.new_page_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pclr_def), STATIC_POSITION(5), log_record_contents).blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_COMPACTION :
		{
			log_record lr;
			lr.type = PAGE_COMPACTION;

			lr.pcptlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pcptlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.pcptlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pcptlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.pcptlr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pcptlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			user_value size_def = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->pcptlr_def), STATIC_POSITION(3), log_record_contents);
			deserialize_tuple_size_def(&(lr.pcptlr.size_def), size_def.blob_value, size_def.blob_size);

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case FULL_PAGE_WRITE :
		{
			log_record lr;
			lr.type = FULL_PAGE_WRITE;

			lr.fpwlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->fpwlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.fpwlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->fpwlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.fpwlr.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->fpwlr_def), STATIC_POSITION(2), log_record_contents).uint_value;

			lr.fpwlr.writerLSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->fpwlr_def), STATIC_POSITION(3), log_record_contents).large_uint_value;
			lr.fpwlr.page_contents = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->fpwlr_def), STATIC_POSITION(4), log_record_contents).blob_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case COMPENSATION_LOG :
		{
			log_record lr;
			lr.type = COMPENSATION_LOG;

			lr.clr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->clr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.clr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->clr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.clr.undo_of_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->clr_def), STATIC_POSITION(2), log_record_contents).large_uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case ABORT_MINI_TX :
		{
			log_record lr;
			lr.type = ABORT_MINI_TX;

			lr.amtlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->amtlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.amtlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->amtlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;
			lr.amtlr.abort_error = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->amtlr_def), STATIC_POSITION(2), log_record_contents).int_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case COMPLETE_MINI_TX :
		{
			log_record lr;
			lr.type = COMPLETE_MINI_TX;

			lr.cmtlr.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->cmtlr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.cmtlr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->cmtlr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;

			lr.cmtlr.is_aborted = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->cmtlr_def), STATIC_POSITION(2), log_record_contents).bit_field_value;

			user_value info = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->cmtlr_def), STATIC_POSITION(3), log_record_contents);
			if(is_user_value_NULL(&info))
				lr.cmtlr.info = NULL;
			else
			{
				lr.cmtlr.info = info.blob_value;
				lr.cmtlr.info_size = info.blob_size;
			}

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			log_record lr;
			lr.type = CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY;

			lr.ckptmttelr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;

			lr.ckptmttelr.mt.mini_transaction_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,0), log_record_contents).large_uint_value;
			lr.ckptmttelr.mt.lastLSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,1), log_record_contents).large_uint_value;
			lr.ckptmttelr.mt.state = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,2), log_record_contents).uint_value;
			lr.ckptmttelr.mt.abort_error = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,3), log_record_contents).int_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			log_record lr;
			lr.type = CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY;

			lr.ckptdptelr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptdptelr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;

			lr.ckptdptelr.dpte.page_id = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptdptelr_def), STATIC_POSITION(1,0), log_record_contents).uint_value;
			lr.ckptdptelr.dpte.recLSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptdptelr_def), STATIC_POSITION(1,1), log_record_contents).large_uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case CHECKPOINT_END :
		{
			log_record lr;
			lr.type = CHECKPOINT_END;

			lr.ckptelr.prev_log_record_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptelr_def), STATIC_POSITION(0), log_record_contents).large_uint_value;
			lr.ckptelr.begin_LSN = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->ckptelr_def), STATIC_POSITION(1), log_record_contents).large_uint_value;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case USER_INFO :
		{
			log_record lr;
			lr.type = USER_INFO;

			user_value info = getter_for_attribute_of_uncompressed_log_record_contents(&(lrtd_p->uilr_def), STATIC_POSITION(0), log_record_contents);
			if(is_user_value_NULL(&info))
				lr.uilr.info = NULL;
			else
			{
				lr.uilr.info = info.blob_value;
				lr.uilr.info_size = info.blob_size;
			}

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
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

// compression limit should be set in some 100s of bytes
#define COMPRESSION_LIMIT 250 // all log records with size greater than COMPRESSION_LIMIT will be compressed

// input is always consumed and freed
static void* compress_serialized_log_record_idempotently(void* input, uint32_t input_size, uint32_t* output_size)
{
	if(input_size == 0)
	{
		printf("ISSUE :: invalid serialized log record of size 0, requested to be compressed\n");
		exit(-1);
	}

	// if the log record is already compressed OR the input_size is smaller than 100 , return input as is
	if(((char*)input)[0] & (1<<7))
	{
		(*output_size) = input_size;
		return input;
	}

	if(input_size < COMPRESSION_LIMIT)
	{
		(*output_size) = input_size;
		return input;
	}

	{
		// initialize output
		uint32_t output_capacity = 50;
		void* output = malloc(50);
		if(output == NULL)
		{
			printf("ISSUE :: failure to allocate memory for compression of log record\n");
			exit(-1);
		}

		// consume first byte
		((char*)output)[0] = ((char*)input)[0] | (1<<7);

		z_stream zstrm;
		zstrm.zalloc = Z_NULL;
		zstrm.zfree = Z_NULL;
		zstrm.opaque = Z_NULL;

		zstrm.next_in = input + 1;
		zstrm.avail_in = input_size - 1;

		zstrm.next_out = output + 1;
		zstrm.avail_out = output_capacity - 1;

		if(Z_OK != deflateInit(&zstrm, Z_DEFAULT_COMPRESSION))
		{
			printf("ISSUE :: failure to initialize zlib compression stream for compressing log record\n");
			exit(-1);
		}

		while(1)
		{
			int res = deflate(&zstrm, Z_FINISH);

			if(res == Z_OK || res == Z_BUF_ERROR)
			{
				uint32_t new_output_capacity = output_capacity * 2;
				output = realloc(output, new_output_capacity);
				if(output == NULL)
				{
					printf("ISSUE :: failure to allocate memory for compression of log record\n");
					exit(-1);
				}

				zstrm.next_out = output + output_capacity;
				zstrm.avail_out = new_output_capacity - output_capacity;

				output_capacity = new_output_capacity;
			}
			else if(res == Z_STREAM_END)
				break;
			else
			{
				printf("ISSUE :: %d error encountered while compressing log record inside zlib\n", res);
				exit(-1);
			}
		}

		(*output_size) = zstrm.total_out + 1;

		deflateEnd(&zstrm);

		if((*output_size) >= input_size) // if for some reason data expanded instead then return input as is, freeing the allocated output buffer
		{
			free(output);
			(*output_size) = input_size;
			return input;
		}

		free(input);
		return output;
	}
}

const void* serialize_and_compress_log_record(const log_record_tuple_defs* lrtd_p, const mini_transaction_engine_stats* stats, const log_record* lr, uint32_t* result_size)
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

			result = malloc(capacity);
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
			break;
		}
		case PAGE_DEALLOCATION :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->palr_def));

			result = malloc(capacity);
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
			break;
		}
		case PAGE_INIT :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pilr_def)) + (4 + get_page_content_size_for_page(lr->pilr.page_id, stats)) + 16;

			result = malloc(capacity);
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
			break;
		}
		case PAGE_SET_HEADER :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pshlr_def)) + 2 * (4 + lr->pshlr.page_header_size);

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_APPEND :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->talr_def)) + 16;
			if(lr->talr.new_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple));

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_INSERT :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tilr_def)) + 16;
			if(lr->tilr.new_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple));

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_UPDATE :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tulr_def)) + 16;
			if(lr->tulr.old_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple));
			if(lr->tulr.new_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple));

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_DISCARD :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tdlr_def)) + 16;
			if(lr->tdlr.old_tuple != NULL)
				capacity += (4 + get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple));

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_DISCARD_ALL :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tdalr_def)) + 16 + (4 + get_page_content_size_for_page(lr->tdalr.page_id, stats));

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tdttlr_def)) + 16;

			result = malloc(capacity);
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
			break;
		}
		case TUPLE_SWAP :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->tslr_def)) + 16;

			result = malloc(capacity);
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
			break;
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

			result = malloc(capacity);
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
			break;
		}
		case PAGE_CLONE :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pclr_def)) + 16 + 2 * (4 + get_page_content_size_for_page(lr->pclr.page_id, stats));

			result = malloc(capacity);
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
			break;
		}
		case PAGE_COMPACTION :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->pcptlr_def)) + 16;

			result = malloc(capacity);
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
			break;
		}
		case FULL_PAGE_WRITE :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->fpwlr_def)) + 16 + (4 + get_page_content_size_for_page(lr->fpwlr.page_id, stats));

			result = malloc(capacity);
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

			if(!set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(3), result + 1, &(user_value){.large_uint_value = lr->fpwlr.writerLSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->fpwlr_def), STATIC_POSITION(4), result + 1, &(user_value){.blob_value = lr->fpwlr.page_contents, .blob_size = get_page_content_size_for_page(lr->fpwlr.page_id, stats)}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->fpwlr_def), result + 1) + 1;
			break;
		}
		case COMPENSATION_LOG :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->clr_def));

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = COMPENSATION_LOG;

			init_tuple(&(lrtd_p->clr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->clr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->clr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->clr_def), STATIC_POSITION(2), result + 1, &(user_value){.large_uint_value = lr->clr.undo_of_LSN}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->clr_def), result + 1) + 1;
			break;
		}
		case ABORT_MINI_TX :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->amtlr_def));

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = ABORT_MINI_TX;

			init_tuple(&(lrtd_p->amtlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->amtlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->amtlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->amtlr_def), STATIC_POSITION(2), result + 1, &(user_value){.int_value = lr->amtlr.abort_error}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->amtlr_def), result + 1) + 1;
			break;
		}
		case COMPLETE_MINI_TX :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->cmtlr_def));
			if(lr->cmtlr.info != NULL)
				capacity += (4 + lr->cmtlr.info_size);

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = COMPLETE_MINI_TX;

			init_tuple(&(lrtd_p->cmtlr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->cmtlr.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->cmtlr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(2), result + 1, &(user_value){.bit_field_value = lr->cmtlr.is_aborted}, UINT32_MAX))
				goto ERROR;

			if(lr->cmtlr.info == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(3), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->cmtlr_def), STATIC_POSITION(3), result + 1, &(user_value){.blob_value = lr->cmtlr.info, .blob_size = lr->cmtlr.info_size}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->cmtlr_def), result + 1) + 1;
			break;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->ckptmttelr_def));

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY;

			init_tuple(&(lrtd_p->ckptmttelr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->ckptmttelr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,0), result + 1, &(user_value){.large_uint_value = lr->ckptmttelr.mt.mini_transaction_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,1), result + 1, &(user_value){.large_uint_value = lr->ckptmttelr.mt.lastLSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,2), result + 1, &(user_value){.uint_value = lr->ckptmttelr.mt.state}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptmttelr_def), STATIC_POSITION(1,3), result + 1, &(user_value){.int_value = lr->ckptmttelr.mt.abort_error}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->ckptmttelr_def), result + 1) + 1;
			break;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->ckptdptelr_def));

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY;

			init_tuple(&(lrtd_p->ckptdptelr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->ckptdptelr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->ckptdptelr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptdptelr_def), STATIC_POSITION(1,0), result + 1, &(user_value){.uint_value = lr->ckptdptelr.dpte.page_id}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptdptelr_def), STATIC_POSITION(1,1), result + 1, &(user_value){.large_uint_value = lr->ckptdptelr.dpte.recLSN}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->ckptdptelr_def), result + 1) + 1;
			break;
		}
		case CHECKPOINT_END :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->ckptelr_def));

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = CHECKPOINT_END;

			init_tuple(&(lrtd_p->ckptelr_def), result + 1);

			if(!set_element_in_tuple(&(lrtd_p->ckptelr_def), STATIC_POSITION(0), result + 1, &(user_value){.large_uint_value = lr->ckptelr.prev_log_record_LSN}, UINT32_MAX))
				goto ERROR;

			if(!set_element_in_tuple(&(lrtd_p->ckptelr_def), STATIC_POSITION(1), result + 1, &(user_value){.large_uint_value = lr->ckptelr.begin_LSN}, UINT32_MAX))
				goto ERROR;

			(*result_size) = get_tuple_size(&(lrtd_p->ckptelr_def), result + 1) + 1;
			break;
		}
		case USER_INFO :
		{
			uint32_t capacity = 1 + get_minimum_tuple_size(&(lrtd_p->uilr_def));
			if(lr->uilr.info != NULL)
				capacity += (4 + lr->uilr.info_size);

			result = malloc(capacity);
			if(result == NULL)
				goto ERROR;

			((unsigned char*)result)[0] = USER_INFO;

			init_tuple(&(lrtd_p->uilr_def), result + 1);

			if(lr->uilr.info == NULL)
			{
				if(!set_element_in_tuple(&(lrtd_p->uilr_def), STATIC_POSITION(0), result + 1, NULL_USER_VALUE, UINT32_MAX))
					goto ERROR;
			}
			else
			{
				if(!set_element_in_tuple(&(lrtd_p->uilr_def), STATIC_POSITION(0), result + 1, &(user_value){.blob_value = lr->uilr.info, .blob_size = lr->uilr.info_size}, UINT32_MAX))
					goto ERROR;
			}

			(*result_size) = get_tuple_size(&(lrtd_p->uilr_def), result + 1) + 1;
			break;
		}
	}

	return compress_serialized_log_record_idempotently(result, (*result_size), result_size);

	ERROR :;
	if(result)
		free(result);
	result = NULL;
	(*result_size) = 0;
	return NULL;
}

static void print_blob(const void* data, uint32_t data_size)
{
	if(data == NULL)
	{
		printf("NULL");
		return;
	}
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
			printf("prev_log_record_LSN : "); print_uint256(lr->palr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->palr.page_id);
			return;
		}
		case PAGE_INIT :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pilr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pilr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pilr.page_id);
			printf("old_page_contents : "); print_blob(lr->pilr.old_page_contents, get_page_content_size_for_page(lr->pilr.page_id, stats)); printf("\n");
			printf("new_page_header_size : %"PRIu32"\n", lr->pilr.new_page_header_size);
			printf("new_size_def : \n"); print_tuple_size_def(&(lr->pilr.new_size_def)); printf("\n");
			return;
		}
		case PAGE_SET_HEADER :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pshlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pshlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pshlr.page_id);
			printf("old_page_header_contents : "); print_blob(lr->pshlr.old_page_header_contents, lr->pshlr.page_header_size); printf("\n");
			printf("new_page_header_contents : "); print_blob(lr->pshlr.new_page_header_contents, lr->pshlr.page_header_size); printf("\n");
			return;
		}
		case TUPLE_APPEND :
		{
			printf("mini_transaction_id : "); print_uint256(lr->talr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->talr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->talr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->talr.size_def)); printf("\n");
			printf("new_tuple : ");
			if(lr->talr.new_tuple != NULL)
				print_blob(lr->talr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple));
			else
				print_blob(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_INSERT :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tilr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tilr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tilr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tilr.size_def)); printf("\n");
			printf("insert_index : %"PRIu32"\n", lr->tilr.insert_index);
			printf("new_tuple : ");
			if(lr->tilr.new_tuple != NULL)
				print_blob(lr->tilr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple));
			else
				print_blob(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_UPDATE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tulr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tulr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tulr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tulr.size_def)); printf("\n");
			printf("update_index : %"PRIu32"\n", lr->tulr.update_index);
			printf("old_tuple : ");
			if(lr->tulr.old_tuple != NULL)
				print_blob(lr->tulr.old_tuple, get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple));
			else
				print_blob(NULL, 0);
			printf("\n");
			printf("new_tuple : ");
			if(lr->tulr.new_tuple != NULL)
				print_blob(lr->tulr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple));
			else
				print_blob(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_DISCARD :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tdlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdlr.size_def)); printf("\n");
			printf("discard_index : %"PRIu32"\n", lr->tdlr.discard_index);
			printf("old_tuple : ");
			if(lr->tdlr.old_tuple != NULL)
				print_blob(lr->tdlr.old_tuple, get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple));
			else
				print_blob(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_DISCARD_ALL :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdalr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tdalr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdalr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdalr.size_def)); printf("\n");
			printf("old_page_contents : "); print_blob(lr->tdalr.old_page_contents, get_page_content_size_for_page(lr->tdalr.page_id, stats)); printf("\n");
			return;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdttlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tdttlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdttlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdttlr.size_def)); printf("\n");
			printf("discarded_trailing_tomb_stones_count : %"PRIu32"\n", lr->tdttlr.discarded_trailing_tomb_stones_count);
			return;
		}
		case TUPLE_SWAP :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tslr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tslr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tslr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tslr.size_def)); printf("\n");
			printf("swap_index1 : %"PRIu32"\n", lr->tslr.swap_index1);
			printf("swap_index2 : %"PRIu32"\n", lr->tslr.swap_index2);
			return;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tueiplr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tueiplr.prev_log_record_LSN); printf("\n");
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
			printf("prev_log_record_LSN : "); print_uint256(lr->pclr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pclr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->pclr.size_def)); printf("\n");
			printf("old_page_contents : "); print_blob(lr->pclr.old_page_contents, get_page_content_size_for_page(lr->pclr.page_id, stats)); printf("\n");
			printf("new_page_contents : "); print_blob(lr->pclr.new_page_contents, get_page_content_size_for_page(lr->pclr.page_id, stats)); printf("\n");
			return;
		}
		case PAGE_COMPACTION :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pcptlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pcptlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pcptlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->pcptlr.size_def)); printf("\n");
			return;
		}
		case FULL_PAGE_WRITE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->fpwlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->fpwlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->fpwlr.page_id);
			printf("writerLSN : "); print_uint256(lr->fpwlr.writerLSN); printf("\n");
			printf("page_contents : "); print_blob(lr->fpwlr.page_contents, get_page_content_size_for_page(lr->fpwlr.page_id, stats)); printf("\n");
			return;
		}
		case COMPENSATION_LOG :
		{
			printf("mini_transaction_id : "); print_uint256(lr->clr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->clr.prev_log_record_LSN); printf("\n");
			printf("undo_of_LSN : "); print_uint256(lr->clr.undo_of_LSN); printf("\n");
			return;
		}
		case ABORT_MINI_TX :
		{
			printf("mini_transaction_id : "); print_uint256(lr->amtlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->amtlr.prev_log_record_LSN); printf("\n");
			printf("abort_error : %d\n", lr->amtlr.abort_error);
			return;
		}
		case COMPLETE_MINI_TX :
		{
			printf("mini_transaction_id : "); print_uint256(lr->cmtlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->cmtlr.prev_log_record_LSN); printf("\n");
			printf("is_aborted : %d\n", !!(lr->cmtlr.is_aborted));
			printf("info : "); print_blob(lr->cmtlr.info, lr->cmtlr.info_size); printf("\n");
			return;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			printf("prev_log_record_LSN : "); print_uint256(lr->ckptmttelr.prev_log_record_LSN); printf("\n");
			printf("mt :\n");
			printf("mini_transaction_id : "); print_uint256(lr->ckptmttelr.mt.mini_transaction_id); printf("\n");
			printf("lastLSN : "); print_uint256(lr->ckptmttelr.mt.lastLSN); printf("\n");
			printf("state : %d\n", lr->ckptmttelr.mt.state);
			printf("abort_error : %d\n", lr->ckptmttelr.mt.abort_error);
			return;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			printf("prev_log_record_LSN : "); print_uint256(lr->ckptdptelr.prev_log_record_LSN); printf("\n");
			printf("dpte :\n");
			printf("page_id : %"PRIu64"\n", lr->ckptdptelr.dpte.page_id);
			printf("recLSN : "); print_uint256(lr->ckptdptelr.dpte.recLSN); printf("\n");
			return;
		}
		case CHECKPOINT_END :
		{
			printf("prev_log_record_LSN : "); print_uint256(lr->ckptelr.prev_log_record_LSN); printf("\n");
			printf("begin_LSN : "); print_uint256(lr->ckptelr.begin_LSN); printf("\n");
			return;
		}
		case USER_INFO :
		{
			printf("info : "); print_blob(lr->uilr.info, lr->uilr.info_size); printf("\n");
			return;
		}
	}
}

#include<wale.h>

uint256 get_mini_transaction_id_for_log_record(const log_record* lr)
{
	switch(lr->type)
	{
		default :
			return INVALID_LOG_SEQUENCE_NUMBER;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			return lr->palr.mini_transaction_id;
		case PAGE_INIT :
			return lr->pilr.mini_transaction_id;
		case PAGE_SET_HEADER :
			return lr->pshlr.mini_transaction_id;
		case TUPLE_APPEND :
			return lr->talr.mini_transaction_id;
		case TUPLE_INSERT :
			return lr->tilr.mini_transaction_id;
		case TUPLE_UPDATE :
			return lr->tulr.mini_transaction_id;
		case TUPLE_DISCARD :
			return lr->tdlr.mini_transaction_id;
		case TUPLE_DISCARD_ALL :
			return lr->tdalr.mini_transaction_id;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			return lr->tdttlr.mini_transaction_id;
		case TUPLE_SWAP :
			return lr->tslr.mini_transaction_id;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			return lr->tueiplr.mini_transaction_id;
		case PAGE_CLONE :
			return lr->pclr.mini_transaction_id;
		case PAGE_COMPACTION :
			return lr->pcptlr.mini_transaction_id;
		case FULL_PAGE_WRITE :
			return lr->fpwlr.mini_transaction_id;
		case COMPENSATION_LOG :
			return lr->clr.mini_transaction_id;
		case ABORT_MINI_TX :
			return lr->amtlr.mini_transaction_id;
		case COMPLETE_MINI_TX :
			return lr->cmtlr.mini_transaction_id;
	}
}

int set_mini_transaction_id_for_log_record(log_record* lr, uint256 mini_transaction_id)
{
	switch(lr->type)
	{
		default :
			return 0;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			lr->palr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_INIT :
		{
			lr->pilr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_SET_HEADER :
		{
			lr->pshlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_APPEND :
		{
			lr->talr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_INSERT :
		{
			lr->tilr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_UPDATE :
		{
			lr->tulr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_DISCARD :
		{
			lr->tdlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_DISCARD_ALL :
		{
			lr->tdalr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			lr->tdttlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_SWAP :
		{
			lr->tslr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			lr->tueiplr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_CLONE :
		{
			lr->pclr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_COMPACTION :
		{
			lr->pcptlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case FULL_PAGE_WRITE :
		{
			lr->fpwlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case COMPENSATION_LOG :
		{
			lr->clr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case ABORT_MINI_TX :
		{
			lr->amtlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case COMPLETE_MINI_TX :
		{
			lr->cmtlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
	}
}

uint256 get_prev_log_record_LSN_for_log_record(const log_record* lr)
{
	switch(lr->type)
	{
		default :
			return INVALID_LOG_SEQUENCE_NUMBER;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			return lr->palr.prev_log_record_LSN;
		case PAGE_INIT :
			return lr->pilr.prev_log_record_LSN;
		case PAGE_SET_HEADER :
			return lr->pshlr.prev_log_record_LSN;
		case TUPLE_APPEND :
			return lr->talr.prev_log_record_LSN;
		case TUPLE_INSERT :
			return lr->tilr.prev_log_record_LSN;
		case TUPLE_UPDATE :
			return lr->tulr.prev_log_record_LSN;
		case TUPLE_DISCARD :
			return lr->tdlr.prev_log_record_LSN;
		case TUPLE_DISCARD_ALL :
			return lr->tdalr.prev_log_record_LSN;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			return lr->tdttlr.prev_log_record_LSN;
		case TUPLE_SWAP :
			return lr->tslr.prev_log_record_LSN;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			return lr->tueiplr.prev_log_record_LSN;
		case PAGE_CLONE :
			return lr->pclr.prev_log_record_LSN;
		case PAGE_COMPACTION :
			return lr->pcptlr.prev_log_record_LSN;
		case FULL_PAGE_WRITE :
			return lr->fpwlr.prev_log_record_LSN;
		case COMPENSATION_LOG :
			return lr->clr.prev_log_record_LSN;
		case ABORT_MINI_TX :
			return lr->amtlr.prev_log_record_LSN;
		case COMPLETE_MINI_TX :
			return lr->cmtlr.prev_log_record_LSN;
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
			return lr->ckptmttelr.prev_log_record_LSN;
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
			return lr->ckptdptelr.prev_log_record_LSN;
		case CHECKPOINT_END :
			return lr->ckptelr.prev_log_record_LSN;
	}
}

int set_prev_log_record_LSN_for_log_record(log_record* lr, uint256 prev_log_record_LSN)
{
	switch(lr->type)
	{
		default :
			return 0;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			lr->palr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_INIT :
		{
			lr->pilr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_SET_HEADER :
		{
			lr->pshlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_APPEND :
		{
			lr->talr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_INSERT :
		{
			lr->tilr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_UPDATE :
		{
			lr->tulr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_DISCARD :
		{
			lr->tdlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_DISCARD_ALL :
		{
			lr->tdalr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			lr->tdttlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_SWAP :
		{
			lr->tslr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			lr->tueiplr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_CLONE :
		{
			lr->pclr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_COMPACTION :
		{
			lr->pcptlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case FULL_PAGE_WRITE :
		{
			lr->fpwlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case COMPENSATION_LOG :
		{
			lr->clr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case ABORT_MINI_TX :
		{
			lr->amtlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case COMPLETE_MINI_TX :
		{
			lr->cmtlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			lr->ckptmttelr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			lr->ckptdptelr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case CHECKPOINT_END :
		{
			lr->ckptelr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
	}
}

uint64_t get_page_id_for_log_record(const log_record* lr)
{
	switch(lr->type)
	{
		default :
			return 0;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			return lr->palr.page_id;
		case PAGE_INIT :
			return lr->pilr.page_id;
		case PAGE_SET_HEADER :
			return lr->pshlr.page_id;
		case TUPLE_APPEND :
			return lr->talr.page_id;
		case TUPLE_INSERT :
			return lr->tilr.page_id;
		case TUPLE_UPDATE :
			return lr->tulr.page_id;
		case TUPLE_DISCARD :
			return lr->tdlr.page_id;
		case TUPLE_DISCARD_ALL :
			return lr->tdalr.page_id;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			return lr->tdttlr.page_id;
		case TUPLE_SWAP :
			return lr->tslr.page_id;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			return lr->tueiplr.page_id;
		case PAGE_CLONE :
			return lr->pclr.page_id;
		case PAGE_COMPACTION :
			return lr->pcptlr.page_id;
		case FULL_PAGE_WRITE :
			return lr->fpwlr.page_id;
	}
}