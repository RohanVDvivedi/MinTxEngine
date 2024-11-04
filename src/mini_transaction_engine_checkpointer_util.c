#include<mini_transaction_engine_checkpointer_util.h>

#include<mini_transaction_engine_util.h>

#include<log_record.h>

uint256 read_checkpoint_from_wal_UNSAFE(mini_transaction_engine* mte, uint256 checkpointLSN, checkpoint* ckpt);

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