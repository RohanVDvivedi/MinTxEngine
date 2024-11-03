#include<mini_transaction_engine_allotment.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

mini_transaction* mte_allot_mini_tx(mini_transaction_engine* mte, uint64_t wait_timeout_in_microseconds)
{
	pthread_mutex_lock(&(mte->global_lock));

	int wait_error = 0;
	while(is_empty_linkedlist(&(mte->free_mini_transactions_list)) && !wait_error) // and not a shutdown
	{
		// get current time
		struct timespec now;
		clock_gettime(CLOCK_REALTIME, &now);

		{
			// compute the time to stop at
			struct timespec diff = {.tv_sec = (wait_timeout_in_microseconds / 1000000LL), .tv_nsec = (wait_timeout_in_microseconds % 1000000LL) * 1000LL};
			struct timespec stop_at = {.tv_sec = now.tv_sec + diff.tv_sec, .tv_nsec = now.tv_nsec + diff.tv_nsec};
			stop_at.tv_sec += stop_at.tv_nsec / 1000000000LL;
			stop_at.tv_nsec = stop_at.tv_nsec % 1000000000LL;

			// wait until atmost stop_at
			pthread_cond_timedwait(&(mte->conditional_to_wait_for_execution_slot), &(mte->global_lock), &stop_at);
		}

		{
			// compute the current time after wait is over
			struct timespec then;
			clock_gettime(CLOCK_REALTIME, &then);

			uint64_t microsecond_elapsed = ((int64_t)then.tv_sec - (int64_t)now.tv_sec) * 1000000LL + (((int64_t)then.tv_nsec - (int64_t)now.tv_nsec) / 1000LL);

			if(microsecond_elapsed > wait_timeout_in_microseconds)
				wait_timeout_in_microseconds = 0;
			else
				wait_timeout_in_microseconds -= microsecond_elapsed;
		}
	}

	mini_transaction* mt = NULL;
	if(!is_empty_linkedlist(&(mte->free_mini_transactions_list))) // and not a shutdown
	{
		// if there is a free mini_transaction then grab it
		mt = (mini_transaction*) get_head_of_linkedlist(&(mte->free_mini_transactions_list));
		remove_head_from_linkedlist(&(mte->free_mini_transactions_list));

		mt->mini_transaction_id = INVALID_LOG_SEQUENCE_NUMBER;
		mt->lastLSN = INVALID_LOG_SEQUENCE_NUMBER;
		mt->state = MIN_TX_IN_PROGRESS;
		mt->abort_error = 0;
		mt->reference_counter = 1;

		// in the begining every mini transaction is a reader_mini_transaction
		insert_head_in_linkedlist(&(mte->reader_mini_transactions), mt);
	}

	pthread_mutex_unlock(&(mte->global_lock));

	return mt;
}

static uint256 append_abortion_log_record_and_flush_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt)
{
	if(are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
	{
		printf("ISSUE :: attempting to log abort log record for a reader mini transaction\n");
		exit(-1);
	}

	// return value
	uint256 log_record_LSN = INVALID_LOG_SEQUENCE_NUMBER;

	{
		log_record lr = {
			.type = ABORT_MINI_TX,
			.amtlr = {
				.mini_transaction_id = mt->mini_transaction_id,
				.prev_log_record_LSN = mt->lastLSN,
				.abort_error = mt->abort_error,
			},
		};

		// serialize log record
		uint32_t serialized_lr_size = 0;
		const void* serialized_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_lr_size);
		if(serialized_lr == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		pthread_mutex_lock(&(mte->global_lock));

		{
			wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

			int wal_error = 0;
			log_record_LSN = append_log_record(wale_p, serialized_lr, serialized_lr_size, 0, &wal_error);
			if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
			{
				printf("ISSUE :: unable to append log record\n");
				exit(-1);
			}

			// if already ensured that it is a writer mini transaction, so it already has a mini_transaction_id

			// update lastLSN of the mini transaction
			mt->lastLSN = log_record_LSN;
		}

		pthread_mutex_unlock(&(mte->global_lock));

		free((void*)serialized_lr);
	}

	// you can not read committed log records without a flush
	pthread_mutex_lock(&(mte->global_lock));
	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);
	pthread_mutex_unlock(&(mte->global_lock));

	return log_record_LSN;
}

static uint256 append_completion_log_record_and_flush_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, const void* complete_info, uint32_t complete_info_size)
{
	if(are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
	{
		printf("ISSUE :: attempting to log completion log record for a reader mini transaction\n");
		exit(-1);
	}

	// return value
	uint256 log_record_LSN = INVALID_LOG_SEQUENCE_NUMBER;

	{
		log_record lr = {
			.type = COMPLETE_MINI_TX,
			.cmtlr = {
				.mini_transaction_id = mt->mini_transaction_id,
				.prev_log_record_LSN = mt->lastLSN,
				.is_aborted = (mt->state != MIN_TX_IN_PROGRESS), // it is surecly not completed, so any state except MIN_TX_IN_PROGRESS implies an aborted transaction
				.info = complete_info,
				.info_size = complete_info_size,
			},
		};

		// serialize log record
		uint32_t serialized_lr_size = 0;
		const void* serialized_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_lr_size);
		if(serialized_lr == NULL)
		{
			printf("ISSUE :: unable to serialize log record\n");
			exit(-1);
		}

		pthread_mutex_lock(&(mte->global_lock));

		{
			wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

			int wal_error = 0;
			log_record_LSN = append_log_record(wale_p, serialized_lr, serialized_lr_size, 0, &wal_error);
			if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
			{
				printf("ISSUE :: unable to append log record\n");
				exit(-1);
			}

			// if already ensured that it is a writer mini transaction, so it already has a mini_transaction_id

			// update lastLSN of the mini transaction
			mt->lastLSN = log_record_LSN;
		}

		pthread_mutex_unlock(&(mte->global_lock));

		free((void*)serialized_lr);
	}

	pthread_mutex_lock(&(mte->global_lock));
	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);
	pthread_mutex_unlock(&(mte->global_lock));

	return log_record_LSN;
}

// luckily all clr log records modify only contents on a single page, hence the simplicity of this function
static void append_compensation_log_record_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, uint256 undo_of_LSN, void* modified_page, uint64_t modified_page_id)
{
	// construct compensation log record
	log_record lr = {
		.type = COMPENSATION_LOG,
		.clr = {
			.mini_transaction_id = mt->mini_transaction_id,
			.prev_log_record_LSN = mt->lastLSN,
			.undo_of_LSN = undo_of_LSN,
		}
	};

	// serialize full page write log record
	uint32_t serialized_lr_size = 0;
	const void* serialized_lr = serialize_log_record(&(mte->lrtd), &(mte->stats), &lr, &serialized_lr_size);
	if(serialized_lr == NULL)
	{
		printf("ISSUE :: unable to serialize log record\n");
		exit(-1);
	}

	pthread_mutex_lock(&(mte->global_lock));

		wale* wale_p = &(((wal_accessor*)get_back_of_arraylist(&(mte->wa_list)))->wale_handle);

		int wal_error = 0;
		uint256 log_record_LSN = append_log_record(wale_p, serialized_lr, serialized_lr_size, 0, &wal_error);
		if(are_equal_uint256(log_record_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // exit with failure if you fail to append log record
		{
			printf("ISSUE :: unable to append log record\n");
			exit(-1);
		}

		// you had to undo a log record then there already exists a log record before it, so the mini transaction already has a mini_transaction_id

		// update lastLSN of the mini transaction
		mt->lastLSN = log_record_LSN;

		// set pageLSN of the page to the log_record_LSN
		set_pageLSN_for_page(modified_page, log_record_LSN, &(mte->stats));

		// do not set writerLSN, as we are undoing, we must have write locked the relevant contents on the page

		// mark the page as dirty in the bufferpool and dirty page table
		// we updated its pageLSN, this page is now considered dirty
		mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, modified_page, modified_page_id);

	pthread_mutex_unlock(&(mte->global_lock));

	// free serialized log record
	free((void*)serialized_lr);
}

#include<bitmap.h>

#include<tuple.h>
#include<page_layout.h>

// below function must be called with manager lock held but global lock not held
static void undo_log_record_and_append_clr_and_manage_state_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, uint256 undo_LSN, const log_record* undo_lr)
{
	switch(undo_lr->type)
	{
		default :
		{
			break;
		}

		// you must never encounter the below 4 types of log records and they can not be undone
		// UNIDENTIFIED, checkpoint log records and user log records must not be present in a mini transaction
		case UNIDENTIFIED :
		{
			printf("ISSUE :: encountered a log record that should not be present in a mini transaction, wal probably corrupted or existence of a bug in mini transaction engine\n");
			exit(-1);
		}

		case COMPENSATION_LOG :
		case ABORT_MINI_TX :
		case COMPLETE_MINI_TX :
		{
			printf("ISSUE :: encountered a log record that can not be undone, wal probably corrupted or existence of a bug in mini transaction engine\n");
			exit(-1);
		}

		// the undo of the below 2 types of lof records is just NOP so return early
		case PAGE_COMPACTION :
		case FULL_PAGE_WRITE :
		{
			return;
		}

		/*
		you need to take care of undo for only the below types of log records
			PAGE_ALLOCATION
			PAGE_DEALLOCATION
			PAGE_INIT
			PAGE_SET_HEADER
			TUPLE_APPEND
			TUPLE_INSERT
			TUPLE_UPDATE
			TUPLE_DISCARD
			TUPLE_DISCARD_ALL
			TUPLE_DISCARD_TRAILING_TOMB_STONES
			TUPLE_SWAP
			TUPLE_UPDATE_ELEMENT_IN_PLACE
			PAGE_CLONE
		*/
	}

	uint64_t page_id = get_page_id_for_log_record(undo_lr);

	if(undo_lr->type == PAGE_ALLOCATION || undo_lr->type == PAGE_DEALLOCATION)
	{
		uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));

		// loop continuously until you get latch on the page
		void* free_space_mapper_page = NULL;
		while(free_space_mapper_page == NULL)
		{
			pthread_mutex_lock(&(mte->global_lock));
			free_space_mapper_page = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, free_space_mapper_page_id, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
			pthread_mutex_unlock(&(mte->global_lock));
		}

		// perform full page write if required
		perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, free_space_mapper_page, free_space_mapper_page_id);

		// perform undo
		{
			uint64_t free_space_mapper_bit_pos = get_is_valid_bit_position_for_page(page_id, &(mte->stats));
			void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
			if(undo_lr->type == PAGE_ALLOCATION) // allocation set the bit to 1, so now reset it
			{
				if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos) != 1) // this should never happen if write locks were held
				{
					printf("ISSUE :: unable to undo page allocation\n");
					exit(-1);
				}
				reset_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
			}
			else
			{
				if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos) != 0) // this should never happen if write locks were held
				{
					printf("ISSUE :: unable to undo page deallocation\n");
					exit(-1);
				}
				set_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
			}
		}

		// append clr log record
		append_compensation_log_record_INTERNAL(mte,mt, undo_LSN, free_space_mapper_page, free_space_mapper_page_id);

		recalculate_page_checksum(free_space_mapper_page, &(mte->stats));

		pthread_mutex_lock(&(mte->global_lock));
		release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
		pthread_mutex_unlock(&(mte->global_lock));
	}
	else
	{
		// loop continuously until you get latch on the page
		void* page = NULL;
		while(page == NULL)
		{
			pthread_mutex_lock(&(mte->global_lock));
			page = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
			pthread_mutex_unlock(&(mte->global_lock));
		}

		// perform full page write if required
		perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);

		// perform undo
		{
			void* page_contents = get_page_contents_for_page(page, page_id, &(mte->stats));
			switch(undo_lr->type)
			{
				case PAGE_INIT :
				{
					memory_move(page_contents, undo_lr->pilr.old_page_contents, mte->user_stats.page_size);
					break;
				}
				case PAGE_SET_HEADER :
				{
					void* page_header = get_page_header(page_contents, mte->user_stats.page_size);
					uint32_t page_header_size = get_page_header_size(page_contents, mte->user_stats.page_size);
					if(page_header_size != undo_lr->pshlr.page_header_size) // this should never happen if write locks were held
					{
						printf("ISSUE :: unable to undo page set header\n");
						exit(-1);
					}
					memory_move(page_header, undo_lr->pshlr.old_page_header_contents, page_header_size);
					break;
				}
				case TUPLE_APPEND :
				{
					uint32_t tuple_count = get_tuple_count_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->talr.size_def));
					if(tuple_count == 0) //this should never happen if write locks were held
					{
						printf("ISSUE :: will not be able to undo tuple append, because current uple count is 0\n");
						exit(-1);
					}
					if(!discard_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->talr.size_def), tuple_count - 1)) // this should never happen if write locks were held
					{
						printf("ISSUE :: unable to undo tuple append\n");
						exit(-1);
					}
					break;
				}
				case TUPLE_INSERT :
				{
					if(!discard_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tilr.size_def), undo_lr->tilr.insert_index)) // this should never happen if write locks were held
					{
						printf("ISSUE :: unable to undo tuple insert\n");
						exit(-1);
					}
					break;
				}
				case TUPLE_UPDATE :
				{
					int undone = update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tulr.size_def), undo_lr->tulr.update_index, undo_lr->tulr.old_tuple);
					if(!undone)
					{
						if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tulr.size_def), undo_lr->tulr.update_index, NULL)) // this should never fail
						{
							printf("ISSUE :: unable to set NULL to a tuple :: this should never happen\n");
							exit(-1);
						}
						int memory_allocation_error = 0;
						run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr->tulr.size_def), &memory_allocation_error);
						if(memory_allocation_error) // malloc failed on compaction
						{
							printf("ISSUE :: unable to undo tuple update, due to failure to callocate memory for page compaction\n");
							exit(-1);
						}
						if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tulr.size_def), undo_lr->tulr.update_index, undo_lr->tulr.old_tuple)) // this should never happen if write locks were held
						{
							printf("ISSUE :: unable to undo tuple update\n");
							exit(-1);
						}
					}
					break;
				}
				case TUPLE_DISCARD :
				{
					int undone = insert_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tdlr.size_def), undo_lr->tdlr.discard_index, undo_lr->tdlr.old_tuple);
					if(!undone)
					{
						int memory_allocation_error = 0;
						run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr->tdlr.size_def), &memory_allocation_error);
						if(memory_allocation_error) // malloc failed on compaction
						{
							printf("ISSUE :: unable to undo tuple discard, due to failure to allocate memory for page compaction\n");
							exit(-1);
						}
						if(!insert_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tdlr.size_def), undo_lr->tdlr.discard_index, undo_lr->tdlr.old_tuple)) // this should never happen if write locks were held
						{
							printf("ISSUE :: unable to undo tuple discard, even after a compaction\n");
							exit(-1);
						}
					}
					break;
				}
				case TUPLE_DISCARD_ALL :
				{
					memory_move(page_contents, undo_lr->tdalr.old_page_contents, mte->user_stats.page_size);
					break;
				}
				case TUPLE_DISCARD_TRAILING_TOMB_STONES :
				{
					for(uint32_t i = 0; i < undo_lr->tdttlr.discarded_trailing_tomb_stones_count; i++)
					{
						int undone = append_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tdttlr.size_def), NULL);
						if(undone)
							continue;
						int memory_allocation_error = 0;
						run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr->tdttlr.size_def), &memory_allocation_error);
						if(memory_allocation_error) // malloc failed for compaction
						{
							printf("ISSUE :: unable to undo tuple discard trailing tombstones, due to failure to callocate memory for page compaction\n");
							exit(-1);
						}
						if(!append_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tdttlr.size_def), NULL)) // this should never happen if write locks were held
						{
							printf("ISSUE :: unable to undo tuple discard trailing tombstones, even after a compaction\n");
							exit(-1);
						}
					}
					break;
				}
				case TUPLE_SWAP :
				{
					if(!swap_tuples_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tslr.size_def), undo_lr->tslr.swap_index1, undo_lr->tslr.swap_index2)) // this should never happen if write locks were held
					{
						printf("ISSUE :: unable to undo tuple swap\n");
						exit(-1);
					}
					break;
				}
				case TUPLE_UPDATE_ELEMENT_IN_PLACE :
				{
					if(NULL == get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tueiplr.tpl_def.size_def), undo_lr->tueiplr.tuple_index))
					{
						printf("ISSUE :: unable to undo tuple update element in place, tuple itself is NULL\n");
						exit(-1);
					}
					int undone = set_element_in_tuple_in_place_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tueiplr.tpl_def), undo_lr->tueiplr.tuple_index, undo_lr->tueiplr.element_index, &(undo_lr->tueiplr.old_element));
					if(!undone)
					{
						void* new_tuple = NULL;
						{
							// get pointer to the current on page tuple
							const void* on_page_tuple = get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tueiplr.tpl_def.size_def), undo_lr->tueiplr.tuple_index);

							// clone it into new tuple
							new_tuple = malloc(mte->user_stats.page_size);
							if(new_tuple == NULL)
							{
								printf("ISSUE :: unable to undo tuple update element in place, memory allocation for new tuple failed\n");
								exit(-1);
							}
							memory_move(new_tuple, on_page_tuple, get_tuple_size(&(undo_lr->tueiplr.tpl_def), on_page_tuple));
						}

						// perform set element on the new tuple, this must succeed
						if(!set_element_in_tuple(&(undo_lr->tueiplr.tpl_def), undo_lr->tueiplr.element_index, new_tuple, &(undo_lr->tueiplr.old_element), UINT32_MAX))
						{
							printf("ISSUE :: unable to undo tuple update element in place, set tuple on fallback failed\n");
							exit(-1);
						}

						// discard old tuple on the page and run page compaction
						{
							if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tueiplr.tpl_def.size_def), undo_lr->tueiplr.tuple_index, NULL)) // this should never fail
							{
								printf("ISSUE :: unable to set NULL to a tuple :: this should never happen\n");
								exit(-1);
							}
							int memory_allocation_error = 0;
							run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr->tueiplr.tpl_def.size_def), &memory_allocation_error);
							if(memory_allocation_error) // malloc failed on compaction
							{
								printf("ISSUE :: unable to undo tuple update element in place, due to failure to allocate memory for page compaction\n");
								exit(-1);
							}
						}

						// perform update for new tuple on the page
						if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr->tueiplr.tpl_def.size_def), undo_lr->tueiplr.tuple_index, new_tuple)) // this should never happen if write locks were held
						{
							printf("ISSUE :: unable to undo tuple update element in place\n");
							exit(-1);
						}

						free(new_tuple);
					}
					break;
				}
				case PAGE_CLONE :
				{
					memory_move(page_contents, undo_lr->pclr.old_page_contents, mte->user_stats.page_size);
					break;
				}
				default : // if you reach here it is a bug
				{
					printf("ISSUE :: unable to undo log record of an illegal type, which was already filtered\n");
					exit(-1);
				}
			}
		}

		// append clr log record
		append_compensation_log_record_INTERNAL(mte,mt, undo_LSN, page, page_id);

		recalculate_page_checksum(page, &(mte->stats));

		pthread_mutex_lock(&(mte->global_lock));
		release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
		pthread_mutex_unlock(&(mte->global_lock));
	}
}

uint256 mte_complete_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, const void* complete_info, uint32_t complete_info_size)
{
	pthread_mutex_lock(&(mte->global_lock));

	shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

	// if it is a reader mini transaction, no matter what state it is in, there is nothing to be done
	if(are_equal_uint256(mt->mini_transaction_id, INVALID_LOG_SEQUENCE_NUMBER))
	{
		mt->state = MIN_TX_COMPLETED;
		pthread_cond_broadcast(&(mt->write_lock_wait));
		decrement_mini_transaction_reference_counter_UNSAFE(mte, mt);

		shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));
		return INVALID_LOG_SEQUENCE_NUMBER;
	}

	// only proceed further if it is a writer

	// if it is a successfull writer mini transaction, then append a complete mini transaction log record and flush all log records to make them persistent
	if(mt->state == MIN_TX_IN_PROGRESS)
	{
		// state change must happen only after logging it, the correct ordering it below
		pthread_mutex_unlock(&(mte->global_lock));
		uint256 completion_log_record_LSN = append_completion_log_record_and_flush_INTERNAL(mte, mt, complete_info, complete_info_size);
		pthread_mutex_lock(&(mte->global_lock));
		mt->state = MIN_TX_COMPLETED;
		pthread_cond_broadcast(&(mt->write_lock_wait));
		decrement_mini_transaction_reference_counter_UNSAFE(mte, mt);

		shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));
		return completion_log_record_LSN;
	}

	// if the mini transaction is in ABORTED state, then append abort log record and turn it into UNDOING_FOR_ABORT state
	if(mt->state == MIN_TX_ABORTED)
	{
		// state change must happen only after logging it, the correct ordering it below
		pthread_mutex_unlock(&(mte->global_lock));
		append_abortion_log_record_and_flush_INTERNAL(mte, mt);
		pthread_mutex_lock(&(mte->global_lock));
		mt->state = MIN_TX_UNDOING_FOR_ABORT;
	}

	if(mt->state != MIN_TX_UNDOING_FOR_ABORT)
	{
		printf("ISSUE :: the correct state for the mini transaction here must be MIN_TX_UNDOING_FOR_ABORT\n");
		exit(-1);
	}

	// undo everything you did for this transaction until now except FULL_PAGE_WRITE and PAGE_COMPACTION as their undo is NO-OP
	{
		uint256 undo_LSN; // this attribute tracks the log record to be undone

		// initialize undo_LSN
		{
			// fetch the most recent log record
			log_record lr;

			// fetch the last non full page write log record
			uint256 temp = mt->lastLSN;
			while(1)
			{
				if(!get_parsed_log_record_UNSAFE(mte, temp, &lr))
				{
					printf("ISSUE :: error reading log record\n");
					exit(-1);
				}

				if(lr.type != FULL_PAGE_WRITE)
					break;

				temp = get_prev_log_record_LSN_for_log_record(&lr);
				destroy_and_free_parsed_log_record(&lr);
			}

			if(lr.type == ABORT_MINI_TX) // its previous log record is where we start undoing from
			{
				undo_LSN = lr.amtlr.prev_log_record_LSN;
				destroy_and_free_parsed_log_record(&lr);

				if(are_equal_uint256(undo_LSN, INVALID_LOG_SEQUENCE_NUMBER))
				{
					printf("ISSUE :: detected the presence of ABORT_MINI_TX log record for a reader mini transaction\n");
					exit(-1);
				}
			}
			else if(lr.type == COMPENSATION_LOG) // we start from the previous log record of the last log record that was undone
			{
				temp = lr.clr.undo_of_LSN;
				destroy_and_free_parsed_log_record(&lr);

				if(!get_parsed_log_record_UNSAFE(mte, temp, &lr))
				{
					printf("ISSUE :: error reading log record\n");
					exit(-1);
				}

				undo_LSN = get_prev_log_record_LSN_for_log_record(&lr);
				destroy_and_free_parsed_log_record(&lr);
			}
			else // it can not be any other type of log record
			{
				printf("ISSUE :: the mini transaction state is in MIN_TX_UNDOING_FOR_ABORT, the most recent not FULL_PAGE_WRITE log record is not ABORT_MINI_TX or COMPENSATION_LOG\n");
				exit(-1);
			}
		}

		shared_unlock(&(mte->manager_lock));

		while(!are_equal_uint256(undo_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // keep on doing undo until you do not have any log record to undo
		{
			log_record undo_lr;
			if(!get_parsed_log_record_UNSAFE(mte, undo_LSN, &undo_lr))
			{
				printf("ISSUE :: error reading log record\n");
				exit(-1);
			}

			shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

			pthread_mutex_unlock(&(mte->global_lock));

			// undo undo_lr
			undo_log_record_and_append_clr_and_manage_state_INTERNAL(mte, mt, undo_LSN, &undo_lr);

			// prepare for next iteration
			undo_LSN = get_prev_log_record_LSN_for_log_record(&undo_lr);
			destroy_and_free_parsed_log_record(&undo_lr);

			pthread_mutex_lock(&(mte->global_lock));

			shared_unlock(&(mte->manager_lock));
		}
	}

	shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

	// mark it completed and exit
	// state change must happen only after logging it, the correct ordering it below
	pthread_mutex_unlock(&(mte->global_lock));
	uint256 completion_log_record_LSN = append_completion_log_record_and_flush_INTERNAL(mte, mt, complete_info, complete_info_size);
	pthread_mutex_lock(&(mte->global_lock));
	mt->state = MIN_TX_COMPLETED;
	pthread_cond_broadcast(&(mt->write_lock_wait));
	decrement_mini_transaction_reference_counter_UNSAFE(mte, mt);

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));
	return completion_log_record_LSN;
}

int mark_aborted_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, int abort_error)
{
	// abort_error can not be non-negative
	if(abort_error >= 0)
		return 0;

	pthread_mutex_lock(&(mte->global_lock));
	shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

	// fail if the mini transaction is not in IN_PROGRESS state
	if(mt->state != MIN_TX_IN_PROGRESS)
	{
		shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));
		return 0;
	}

	mt->state = MIN_TX_ABORTED;
	mt->abort_error = abort_error;

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));
	return 1;
}

int is_aborted_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt)
{
	pthread_mutex_lock(&(mte->global_lock));
	shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

	int abort_error = 0;
	if(mt->state == MIN_TX_ABORTED || mt->state == MIN_TX_UNDOING_FOR_ABORT)
		abort_error = mt->abort_error;

	shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));
	return abort_error;
}