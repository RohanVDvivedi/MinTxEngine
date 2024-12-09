#include<log_record.h>

#include<tuple.h>

#include<stdlib.h>
#include<string.h>

int main()
{
	// construct a sample tuple_def
	data_type_info s3 = get_variable_length_string_type("", 100);
	data_type_info* tuple_type_info = alloca(sizeof_tuple_data_type_info(3));
	initialize_tuple_data_type_info(tuple_type_info, "tuple_type1", 1, 1024, 3);
	strcpy(tuple_type_info->containees[0].field_name, "0");
	tuple_type_info->containees[0].al.type_info = UINT_NON_NULLABLE[3];
	strcpy(tuple_type_info->containees[1].field_name, "1");
	tuple_type_info->containees[1].al.type_info = BIT_FIELD_NON_NULLABLE[5];
	strcpy(tuple_type_info->containees[2].field_name, "2");
	tuple_type_info->containees[2].al.type_info = &s3;
	tuple_def tpl_def;
	if(!initialize_tuple_def(&tpl_def, tuple_type_info))
	{
		printf("failed finalizing tuple definition\n");
		exit(-1);
	}

	char old_tuple[1024];
	init_tuple(&tpl_def, old_tuple);
	set_element_in_tuple(&tpl_def, STATIC_POSITION(0), old_tuple, &((user_value){.uint_value = 12}), UINT32_MAX);
	set_element_in_tuple(&tpl_def, STATIC_POSITION(1), old_tuple, &((user_value){.bit_field_value = 0x15}), UINT32_MAX);
	set_element_in_tuple(&tpl_def, STATIC_POSITION(2), old_tuple, &((user_value){.string_value = "hello", .string_size = strlen("hello")}), UINT32_MAX);

	printf("old_tuple : ");
	for(uint32_t i = 0; i < get_tuple_size(&tpl_def, old_tuple); i++)
		printf("%02hhx, ", old_tuple[i]);
	printf("\n");

	char new_tuple[1024];
	init_tuple(&tpl_def, new_tuple);
	set_element_in_tuple(&tpl_def, STATIC_POSITION(0), new_tuple, &((user_value){.uint_value = 124}), UINT32_MAX);
	set_element_in_tuple(&tpl_def, STATIC_POSITION(1), new_tuple, &((user_value){.bit_field_value = 0x0a}), UINT32_MAX);
	set_element_in_tuple(&tpl_def, STATIC_POSITION(2), new_tuple, &((user_value){.string_value = "world", .string_size = strlen("world")}), UINT32_MAX);

	printf("new_tuple : ");
	for(uint32_t i = 0; i < get_tuple_size(&tpl_def, new_tuple); i++)
		printf("%02hhx, ", new_tuple[i]);
	printf("\n");

	const mini_transaction_engine_stats stats = {
		.log_sequence_number_width = 2,
		.page_id_width = 2,
		.page_size = 100,
	};
	log_record_tuple_defs lrtd;
	initialize_log_record_tuple_defs(&lrtd, &stats);
	printf("\n\n");

	char old_page_contents[100];
	for(int i = 0; i < 100; i++)
		old_page_contents[i] = i;
	char new_page_contents[100];
	for(int i = 0; i < 100; i++)
		new_page_contents[i] = i + 50;

	{
		log_record a = {
			.type = PAGE_ALLOCATION,
			.palr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = PAGE_DEALLOCATION,
			.palr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = PAGE_INIT,
			.pilr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.old_page_contents = old_page_contents,
				.new_page_header_size = 3,
				.new_size_def = tpl_def.size_def,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = PAGE_SET_HEADER,
			.pshlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.old_page_header_contents = old_page_contents,
				.new_page_header_contents = new_page_contents,
				.page_header_size = 5,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_APPEND,
			.talr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.new_tuple = new_tuple,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_INSERT,
			.tilr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.insert_index = 44,
				.new_tuple = new_tuple,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_UPDATE,
			.tulr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.update_index = 44,
				.old_tuple = old_tuple,
				.new_tuple = new_tuple,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_DISCARD,
			.tdlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.discard_index = 44,
				.old_tuple = old_tuple,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_DISCARD_ALL,
			.tdalr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.old_page_contents = old_page_contents,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_DISCARD_TRAILING_TOMB_STONES,
			.tdttlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.discarded_trailing_tomb_stones_count = 44,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_SWAP,
			.tslr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.swap_index1 = 44,
				.swap_index2 = 45,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = PAGE_CLONE,
			.pclr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
				.old_page_contents = old_page_contents,
				.new_page_contents = new_page_contents,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = PAGE_COMPACTION,
			.pcptlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.size_def = tpl_def.size_def,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = FULL_PAGE_WRITE,
			.fpwlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.writerLSN = get_uint256(1773),
				.page_contents = old_page_contents,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = COMPENSATION_LOG,
			.clr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.undo_of_LSN = get_uint256(143),
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = ABORT_MINI_TX,
			.amtlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.abort_error = -55,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = COMPLETE_MINI_TX,
			.cmtlr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.is_aborted = 0,
				.info = (uint8_t [3]){1,2,3},
				.info_size = 3,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = TUPLE_UPDATE_ELEMENT_IN_PLACE,
			.tueiplr = {
				.mini_transaction_id = get_uint256(113),
				.prev_log_record_LSN = get_uint256(943),
				.page_id = 533,
				.tpl_def = tpl_def,
				.tuple_index = 44,
				.element_index = STATIC_POSITION(0),
				.old_element = (user_value){.uint_value = 35},
				.new_element = (user_value){.uint_value = 36},
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY,
			.ckptmttelr = {
				.prev_log_record_LSN = get_uint256(943),
				.mt.mini_transaction_id = get_uint256(113),
				.mt.lastLSN = get_uint256(391),
				.mt.state = MIN_TX_ABORTED,
				.mt.abort_error = -56,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY,
			.ckptdptelr = {
				.prev_log_record_LSN = get_uint256(943),
				.dpte.page_id = 533,
				.dpte.recLSN = get_uint256(853),
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = CHECKPOINT_END,
			.ckptelr = {
				.prev_log_record_LSN = get_uint256(943),
				.begin_LSN = get_uint256(789),
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	{
		log_record a = {
			.type = USER_INFO,
			.uilr = {
				.info = (uint8_t [4]){4,5,6,7},
				.info_size = 4,
			}
		};

		uint32_t serialized_size;
		const void* serialized = serialize_and_compress_log_record(&lrtd, &stats, &a, &serialized_size);
		if(serialized == NULL)
		{
			printf("serialization failed\n");
			exit(-1);
		}

		log_record b = uncompress_and_parse_log_record(&lrtd, serialized, serialized_size);

		printf("size = %"PRIu32"\n", serialized_size);

		printf("a :: \n");
		print_log_record(&a, &stats);
		printf("\nb :: \n");
		print_log_record(&b, &stats);
		printf("\n");

		destroy_and_free_parsed_log_record(&b);
	}
	printf("\n\n");

	deinitialize_log_record_tuple_defs(&lrtd);

	return 0;
}