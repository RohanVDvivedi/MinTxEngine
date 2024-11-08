#include<mini_transaction_engine_recovery_util.h>

#include<mini_transaction_engine_checkpointer_util.h>
#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

checkpoint analyze(mini_transaction_engine* mte)
{
	// this lock is customary to be taken only to access bufferpool and wale
	pthread_mutex_lock(&(mte->global_lock));

	// read OR initialize checkpoint
	uint256 analyze_at;
	checkpoint ckpt = {};
	if(are_equal_uint256(mte->checkpointLSN, INVALID_LOG_SEQUENCE_NUMBER))
	{
		if(!initialize_mini_transaction_table(&(ckpt.mini_transaction_table), 3))
		{
			printf("ISSUE :: unable to initialize a new empty checkpoint hashmap\n");
			exit(-1);
		}
		if(!initialize_dirty_page_table(&(ckpt.dirty_page_table), 3))
		{
			printf("ISSUE :: unable to initialize a new empty checkpoint hashmap\n");
			exit(-1);
		}
		analyze_at = FIRST_LOG_SEQUENCE_NUMBER;
	}
	else
	{
		read_checkpoint_from_wal_UNSAFE(mte, mte->checkpointLSN, &ckpt);
		analyze_at = get_next_LSN_for_LSN_UNSAFE(mte, mte->checkpointLSN);
	}

	// start analyzing from analyze_at, reconstructing the mini_transaction_table and dirty_page_table as it was during the crash
	while(!are_equal_uint256(analyze_at, INVALID_LOG_SEQUENCE_NUMBER))
	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, analyze_at, &lr))
		{
			printf("ISSUE :: unable to read log record during recovery\n");
			exit(-1);
		}

		// maintain and update dirty_page_table in checkpoint
		switch(lr.type)
		{
			case UNIDENTIFIED :
			{
				printf("ISSUE :: encountered unidentified log record while analyzing for recovery\n");
				exit(-1);
			}

			case PAGE_ALLOCATION :
			case PAGE_DEALLOCATION :
			{
				uint64_t page_id = get_page_id_for_log_record(&lr);
				uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));

				{
					dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(ckpt.dirty_page_table), &((dirty_page_table_entry){.page_id = page_id}));
					if(dpte == NULL)
					{
						dpte = get_new_dirty_page_table_entry();
						dpte->page_id = page_id;
						dpte->recLSN = analyze_at;
						insert_in_hashmap(&(ckpt.dirty_page_table), dpte);
					}
				}

				{
					dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(ckpt.dirty_page_table), &((dirty_page_table_entry){.page_id = free_space_mapper_page_id}));
					if(dpte == NULL)
					{
						dpte = get_new_dirty_page_table_entry();
						dpte->page_id = free_space_mapper_page_id;
						dpte->recLSN = analyze_at;
						insert_in_hashmap(&(ckpt.dirty_page_table), dpte);
					}
				}

				break;
			}

			case PAGE_INIT :
			case PAGE_SET_HEADER :
			case TUPLE_APPEND :
			case TUPLE_INSERT :
			case TUPLE_UPDATE :
			case TUPLE_DISCARD :
			case TUPLE_DISCARD_ALL :
			case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			case TUPLE_SWAP :
			case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			case PAGE_CLONE :
			case PAGE_COMPACTION :
			case FULL_PAGE_WRITE :
			{
				uint64_t page_id = get_page_id_for_log_record(&lr);

				{
					dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(ckpt.dirty_page_table), &((dirty_page_table_entry){.page_id = page_id}));
					if(dpte == NULL)
					{
						dpte = get_new_dirty_page_table_entry();
						dpte->page_id = page_id;
						dpte->recLSN = analyze_at;
						insert_in_hashmap(&(ckpt.dirty_page_table), dpte);
					}
				}

				break;
			}

			case COMPENSATION_LOG :
			{
				// grab undo of lr
				log_record undo_lr;
				if(!get_parsed_log_record_UNSAFE(mte, lr.clr.undo_of_LSN, &undo_lr))
				{
					printf("ISSUE :: unable to read undo log record during recovery\n");
					exit(-1);
				}

				// take decissions based on undo_lr
				switch(undo_lr.type)
				{
					case PAGE_ALLOCATION :
					case PAGE_DEALLOCATION :
					{
						uint64_t page_id = get_page_id_for_log_record(&undo_lr);
						uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));

						{
							dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(ckpt.dirty_page_table), &((dirty_page_table_entry){.page_id = free_space_mapper_page_id}));
							if(dpte == NULL)
							{
								dpte = get_new_dirty_page_table_entry();
								dpte->page_id = free_space_mapper_page_id;
								dpte->recLSN = analyze_at;
								insert_in_hashmap(&(ckpt.dirty_page_table), dpte);
							}
						}

						break;
					}

					case PAGE_INIT :
					case PAGE_SET_HEADER :
					case TUPLE_APPEND :
					case TUPLE_INSERT :
					case TUPLE_UPDATE :
					case TUPLE_DISCARD :
					case TUPLE_DISCARD_ALL :
					case TUPLE_DISCARD_TRAILING_TOMB_STONES :
					case TUPLE_SWAP :
					case TUPLE_UPDATE_ELEMENT_IN_PLACE :
					case PAGE_CLONE :
					case PAGE_COMPACTION :
					{
						uint64_t page_id = get_page_id_for_log_record(&undo_lr);

						{
							dirty_page_table_entry* dpte = (dirty_page_table_entry*)find_equals_in_hashmap(&(ckpt.dirty_page_table), &((dirty_page_table_entry){.page_id = page_id}));
							if(dpte == NULL)
							{
								dpte = get_new_dirty_page_table_entry();
								dpte->page_id = page_id;
								dpte->recLSN = analyze_at;
								insert_in_hashmap(&(ckpt.dirty_page_table), dpte);
							}
						}

						break;
					}

					default :
					{
						printf("ISSUE :: encountered a CLR log record of a non forward change log record for a mini transaction, plausible bug or corruption\n");
						exit(-1);
					}
				}

				destroy_and_free_parsed_log_record(&undo_lr);
				break;
			}

			// if it is abort_mini_tx, completed_mini_tx or some later checkpoint log record then do nothing
			default :
			{
				break;
			}
		}

		// expand dirty page table if necessary
		if(get_element_count_hashmap(&(ckpt.dirty_page_table)) / 3 > get_bucket_count_hashmap(&(ckpt.dirty_page_table)))
			expand_hashmap(&(ckpt.dirty_page_table), 1.5);

		// maintain and update mini_transaction_table in checkpoint
		// only mini_transaction related log records are to be analyzed here
		switch(lr.type)
		{
			case UNIDENTIFIED :
			{
				printf("ISSUE :: encountered unidentified log record while analyzing for recovery\n");
				exit(-1);
			}

			case PAGE_ALLOCATION :
			case PAGE_DEALLOCATION :
			case PAGE_INIT :
			case PAGE_SET_HEADER :
			case TUPLE_APPEND :
			case TUPLE_INSERT :
			case TUPLE_UPDATE :
			case TUPLE_DISCARD :
			case TUPLE_DISCARD_ALL :
			case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			case TUPLE_SWAP :
			case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			case PAGE_CLONE :
			case PAGE_COMPACTION :
			{
				uint256 mini_transaction_id = get_mini_transaction_id_for_log_record(&lr);

				mini_transaction* mt = (mini_transaction*)find_equals_in_hashmap(&(ckpt.mini_transaction_table), &(mini_transaction){.mini_transaction_id = mini_transaction_id});

				if(mt == NULL)
				{
					mt = get_new_mini_transaction();
					mt->mini_transaction_id = mini_transaction_id;
					mt->state = MIN_TX_IN_PROGRESS;
					insert_in_hashmap(&(ckpt.mini_transaction_table), mt);
				}

				if(mt->state != MIN_TX_IN_PROGRESS)
				{
					printf("ISSUE :: encountered a change log record for a non in_progress mini transaction\n");
					exit(-1);
				}

				mt->lastLSN = analyze_at;

				break;
			}

			case FULL_PAGE_WRITE :
			{
				uint256 mini_transaction_id = get_mini_transaction_id_for_log_record(&lr);

				mini_transaction* mt = (mini_transaction*)find_equals_in_hashmap(&(ckpt.mini_transaction_table), &(mini_transaction){.mini_transaction_id = mini_transaction_id});

				if(mt == NULL)
				{
					mt = get_new_mini_transaction();
					mt->mini_transaction_id = mini_transaction_id;
					mt->state = MIN_TX_IN_PROGRESS;
					insert_in_hashmap(&(ckpt.mini_transaction_table), mt);
				}

				mt->lastLSN = analyze_at;

				break;
			}

			case ABORT_MINI_TX :
			{
				uint256 mini_transaction_id = get_mini_transaction_id_for_log_record(&lr);

				mini_transaction* mt = (mini_transaction*)find_equals_in_hashmap(&(ckpt.mini_transaction_table), &(mini_transaction){.mini_transaction_id = mini_transaction_id});

				if(mt == NULL)
				{
					printf("ISSUE :: encountered an abort log record for a non existing mini transaction\n");
					exit(-1);
				}

				if(mt->state != MIN_TX_IN_PROGRESS && mt->state != MIN_TX_ABORTED)
				{
					printf("ISSUE :: encountered a abort log record for a non in_progress and a non aborted mini transaction\n");
					exit(-1);
				}

				mt->abort_error = lr.amtlr.abort_error;
				mt->state = MIN_TX_UNDOING_FOR_ABORT;

				mt->lastLSN = analyze_at;

				break;
			}

			case COMPENSATION_LOG :
			{
				uint256 mini_transaction_id = get_mini_transaction_id_for_log_record(&lr);

				mini_transaction* mt = (mini_transaction*)find_equals_in_hashmap(&(ckpt.mini_transaction_table), &(mini_transaction){.mini_transaction_id = mini_transaction_id});

				if(mt == NULL)
				{
					printf("ISSUE :: encountered a compensation log record for a non existing mini transaction\n");
					exit(-1);
				}

				if(mt->state != MIN_TX_UNDOING_FOR_ABORT)
				{
					printf("ISSUE :: encountered a clr log record for a non undoing for abort - state mini transaction\n");
					exit(-1);
				}

				mt->lastLSN = analyze_at;

				break;
			}

			case COMPLETE_MINI_TX :
			{
				uint256 mini_transaction_id = get_mini_transaction_id_for_log_record(&lr);

				mini_transaction* mt = (mini_transaction*)find_equals_in_hashmap(&(ckpt.mini_transaction_table), &(mini_transaction){.mini_transaction_id = mini_transaction_id});

				if(mt == NULL)
				{
					printf("ISSUE :: encountered a completion log record for a non existing mini transaction\n");
					exit(-1);
				}

				remove_from_hashmap(&(ckpt.mini_transaction_table), mt);
				delete_mini_transaction(mt);

				break;
			}

			// for checkpoint log records nothing needs to be done
			default :
			{
				break;
			}
		}

		// expand mini transaction table if necessary
		if(get_element_count_hashmap(&(ckpt.mini_transaction_table)) / 3 > get_bucket_count_hashmap(&(ckpt.mini_transaction_table)))
			expand_hashmap(&(ckpt.mini_transaction_table), 1.5);

		// prepare for next iteration
		analyze_at = get_next_LSN_for_LSN_UNSAFE(mte, analyze_at);
		destroy_and_free_parsed_log_record(&lr);
	}

	pthread_mutex_unlock(&(mte->global_lock));
	return ckpt;
}

void redo(mini_transaction_engine* mte, checkpoint* ckpt)
{
	// this lock is customary to be taken only to access bufferpool and wale
	pthread_mutex_lock(&(mte->global_lock));

	// TODO

	pthread_mutex_unlock(&(mte->global_lock));

	// destroy checkpoint we do not need it now
	remove_all_from_hashmap(&(ckpt->mini_transaction_table), AND_DELETE_MINI_TRANSACTIONS_NOTIFIER);
	deinitialize_hashmap(&(ckpt->mini_transaction_table));
	remove_all_from_hashmap(&(ckpt->dirty_page_table), AND_DELETE_DIRTY_PAGE_TABLE_ENTRIES_NOTIFIER);
	deinitialize_hashmap(&(ckpt->dirty_page_table));
}

void undo(mini_transaction_engine* mte)
{
	// TODO
}

void recover(mini_transaction_engine* mte)
{
	checkpoint ckpt = analyze(mte); // runs to generate the checkpoint for the last state of the mini_transaction_engine's mini transaction table and dirty page table at the time of crash

	// TODO to be removed, only left here for testing analyze
	printf("printing checkpoint after analyze ::: \n");
	print_checkpoint(&ckpt);
	printf("\n");
	//exit(-1);

	redo(mte, &ckpt);				// consumes checkpoint and deinitizlizes it, and redos all log records from the minimum recLSN in the checkpoint

	undo(mte); 						// undos uncommitted mini transactions
}