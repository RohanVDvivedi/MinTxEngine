#include<mini_transaction_engine_checkpointer_util.h>

#include<mini_transaction_engine_util.h>

#include<log_record.h>

uint256 read_checkpoint_from_wal_UNSAFE(mini_transaction_engine* mte, uint256 checkpointLSN, checkpoint* ckpt)
{
	{
		if(!initialize_hashmap(&(ckpt->mini_transaction_table), ELEMENTS_AS_LINKEDLIST_INSERT_AT_TAIL, 10, &simple_hasher(hash_mini_transaction), &simple_comparator(compare_mini_transactions), offsetof(mini_transaction, enode)))
		{
			printf("ISSUE :: unable to initialize an checkpoint hashmap\n");
			exit(-1);
		}

		if(!initialize_hashmap(&(ckpt->dirty_page_table), ELEMENTS_AS_LINKEDLIST_INSERT_AT_TAIL, 10, &simple_hasher(hash_dirty_page_table_entry), &simple_comparator(compare_dirty_page_table_entries), offsetof(dirty_page_table_entry, enode)))
		{
			printf("ISSUE :: unable to initialize an checkpoint hashmap\n");
			exit(-1);
		}
	}

	uint256 read_at = checkpointLSN;
	uint256 begin_LSN = INVALID_LOG_SEQUENCE_NUMBER;

	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, read_at, &lr))
		{
			printf("ISSUE :: could not read checkpoint end log record\n");
			exit(-1);
		}

		if(lr.type != CHECKPOINT_END)
		{
			printf("ISSUE :: expected a checkpoint end log record type, but found something else, possibly a bug in mini transaction engine\n");
			exit(-1);
		}

		read_at = get_prev_log_record_LSN_for_log_record(&lr);

		// initialize begin_LSN
		begin_LSN = lr.ckptelr.begin_LSN;

		destroy_and_free_parsed_log_record(&lr);
	}

	// read while read_at != INVALID_LOG_SEQUENCE_NUMBER
	while(!are_equal_uint256(read_at, INVALID_LOG_SEQUENCE_NUMBER))
	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, read_at, &lr))
		{
			printf("ISSUE :: could not read checkpoint end log record\n");
			exit(-1);
		}

		if(lr.type == CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY)
		{
			mini_transaction* mt = get_new_mini_transaction();

			mt->mini_transaction_id = lr.ckptmttelr.mt.mini_transaction_id;
			mt->lastLSN = lr.ckptmttelr.mt.lastLSN;
			mt->state = lr.ckptmttelr.mt.state;

			insert_in_hashmap(&(ckpt->mini_transaction_table), mt);

			if(get_element_count_hashmap(&(ckpt->mini_transaction_table)) / 3 > get_bucket_count_hashmap(&(ckpt->mini_transaction_table)))
				expand_hashmap(&(ckpt->mini_transaction_table), 1.5);
		}
		else if(lr.type == CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY)
		{
			dirty_page_table_entry* dpte = get_new_dirty_page_table_entry();

			dpte->page_id = lr.ckptdptelr.dpte.page_id;
			dpte->recLSN = lr.ckptdptelr.dpte.recLSN;

			insert_in_hashmap(&(ckpt->dirty_page_table), dpte);

			if(get_element_count_hashmap(&(ckpt->dirty_page_table)) / 3 > get_bucket_count_hashmap(&(ckpt->dirty_page_table)))
				expand_hashmap(&(ckpt->dirty_page_table), 1.5);
		}
		else
		{
			printf("ISSUE :: expected a checkpoint entry log record type, but found something else, possibly a bug in mini transaction engine\n");
			exit(-1);
		}

		// if begin_LSN == read_at -> i.e. it is the oldest log record of the checkpoint
		// and the previous log record of this checkpoint is not INVALID, then the checkpoint is probably corrupted
		if(are_equal_uint256(begin_LSN, read_at) && !are_equal_uint256(get_prev_log_record_LSN_for_log_record(&lr), INVALID_LOG_SEQUENCE_NUMBER))
		{
			printf("ISSUE :: begin_LSN not the checkpoint's first log record, probably a bug in mini transaction engine\n");
			exit(-1);
		}

		read_at = get_prev_log_record_LSN_for_log_record(&lr);

		destroy_and_free_parsed_log_record(&lr);
	}

	return begin_LSN;
}

uint256 append_checkpoint_to_wal_UNSAFE(mini_transaction_engine* mte, const checkpoint* ckpt, uint256* begin_LSN)
{
	(*begin_LSN) = INVALID_LOG_SEQUENCE_NUMBER;
	uint256 lastLSN = INVALID_LOG_SEQUENCE_NUMBER;

	wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

	for(const mini_transaction* mt = get_first_of_in_hashmap(&(ckpt->mini_transaction_table), FIRST_OF_HASHMAP); mt != NULL; mt = get_next_of_in_hashmap(&(ckpt->mini_transaction_table), mt, ANY_IN_HASHMAP))
	{
		log_record lr = {
			.type = CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY,
			.ckptmttelr = {
				.prev_log_record_LSN = lastLSN,
				.mt = (*mt),
			},
		};

		uint32_t serialized_log_record_size = 0;
		const void* serialized_log_record = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_log_record_size);
		if(serialized_log_record == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_log_record, serialized_log_record_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		if(are_equal_uint256((*begin_LSN), INVALID_LOG_SEQUENCE_NUMBER))
			(*begin_LSN) = log_record_LSN;
		lastLSN = log_record_LSN;

		free((void*)serialized_log_record);
	}

	for(const dirty_page_table_entry* dpte = get_first_of_in_hashmap(&(ckpt->dirty_page_table), FIRST_OF_HASHMAP); dpte != NULL; dpte = get_next_of_in_hashmap(&(ckpt->dirty_page_table), dpte, ANY_IN_HASHMAP))
	{
		log_record lr = {
			.type = CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY,
			.ckptdptelr = {
				.prev_log_record_LSN = lastLSN,
				.dpte = (*dpte),
			},
		};

		uint32_t serialized_log_record_size = 0;
		const void* serialized_log_record = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_log_record_size);
		if(serialized_log_record == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_log_record, serialized_log_record_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		if(are_equal_uint256((*begin_LSN), INVALID_LOG_SEQUENCE_NUMBER))
			(*begin_LSN) = log_record_LSN;
		lastLSN = log_record_LSN;

		free((void*)serialized_log_record);
	}

	uint256 checkpointLSN = INVALID_LOG_SEQUENCE_NUMBER;
	{
		log_record lr = {
			.type = CHECKPOINT_END,
			.ckptelr = {
				.prev_log_record_LSN = lastLSN,
				.begin_LSN = (*begin_LSN),
			},
		};

		uint32_t serialized_log_record_size = 0;
		const void* serialized_log_record = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_log_record_size);
		if(serialized_log_record == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_log_record, serialized_log_record_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		if(are_equal_uint256((*begin_LSN), INVALID_LOG_SEQUENCE_NUMBER))
			(*begin_LSN) = log_record_LSN;
		lastLSN = log_record_LSN;

		free((void*)serialized_log_record);
	}

	return checkpointLSN;
}