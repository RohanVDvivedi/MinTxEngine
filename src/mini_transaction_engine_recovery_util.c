#include<mini_transaction_engine_recovery_util.h>

#include<mini_transaction_engine_checkpointer_util.h>
#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

static checkpoint analyze(mini_transaction_engine* mte)
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

	printf("checkpoint at : "); print_uint256(mte->checkpointLSN); printf("\n");
	printf("analyze from  : "); print_uint256(analyze_at); printf("\n");

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
					mt->abort_error = 0;
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
					mt->abort_error = 0;
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

// page_id of only the page involved with the changes on this page for the given log record lr at LSN must be provided
static void* acquire_writer_latch_only_if_redo_required_UNSAFE(mini_transaction_engine* mte, const checkpoint* ckpt, uint256 LSN, const log_record* lr, uint64_t page_id)
{
	// a FULL_PAGE_WRITE lg record has to be always redone
	if(lr->type == FULL_PAGE_WRITE)
	{
		void* page = NULL;
		while(page == NULL)
			page = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
		return page;
	}

	const dirty_page_table_entry* dpte = find_equals_in_hashmap(&(ckpt->dirty_page_table), &(dirty_page_table_entry){.page_id = page_id});

	// if the page is not dirty at the checkpoint of crash nothing needs to be done
	if(dpte == NULL)
		return NULL;

	// if the page was dirtied in future nothing needs to be done
	if(compare_uint256(LSN, dpte->recLSN) < 0)
		return NULL;

	// grab latch on the page
	void* page = NULL;
	while(page == NULL)
		page = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1, 0); // evict_dirty_if_necessary -> not to be overwritten

	// if the page on disk is upto date, release latch and return
	if(compare_uint256(LSN, get_pageLSN_for_page(page, &(mte->stats))) <= 0)
	{
		release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
		return NULL;
	}

	return page;
}

static void redo(mini_transaction_engine* mte, checkpoint* ckpt)
{
	// this lock is customary to be taken only to access bufferpool and wale
	pthread_mutex_lock(&(mte->global_lock));

	// get the lsn to start redoing from
	uint256 redo_at = get_minimum_recLSN_for_dirty_page_table(&(ckpt->dirty_page_table));
	if(are_equal_uint256(redo_at, INVALID_LOG_SEQUENCE_NUMBER)) // this happens if there are no dirty page table entries in the checkpoint at the time of crash
		goto EXIT;

	// perform redo until you reach the end of the log records
	while(!are_equal_uint256(redo_at, INVALID_LOG_SEQUENCE_NUMBER))
	{
		log_record lr;
		if(!get_parsed_log_record_UNSAFE(mte, redo_at, &lr))
		{
			printf("ISSUE :: unable to read log record during recovery\n");
			exit(-1);
		}

		switch(lr.type)
		{
			case UNIDENTIFIED :
			{
				printf("ISSUE :: encountered unidentified log record while redoing for recovery\n");
				exit(-1);
			}

			// TODO
		}

		// prepare for next iteration
		redo_at = get_next_LSN_for_LSN_UNSAFE(mte, redo_at);
		destroy_and_free_parsed_log_record(&lr);
	}

	EXIT:;
	// destroy checkpoint we do not need it now
	// here we transfer all mini transactions directly to the mte->writer_mini_transactions
	remove_all_from_hashmap(&(ckpt->mini_transaction_table), AND_TRANSFER_TO_MINI_TRANSACTION_TABLE_NOTIFIER(&(mte->writer_mini_transactions)));
	deinitialize_hashmap(&(ckpt->mini_transaction_table));
	// while we directly delete all the dirty page table entries of the checkpoint as we already constructed a new reliable one while redoing
	remove_all_from_hashmap(&(ckpt->dirty_page_table), AND_DELETE_DIRTY_PAGE_TABLE_ENTRIES_NOTIFIER);
	deinitialize_hashmap(&(ckpt->dirty_page_table));

	pthread_mutex_unlock(&(mte->global_lock));
}

static void undo(mini_transaction_engine* mte)
{
	// this lock is customary to be taken only to access bufferpool and wale
	pthread_mutex_lock(&(mte->global_lock));

	// there should be no readers at this point in time
	if(!is_empty_linkedlist(&(mte->reader_mini_transactions)))
	{
		printf("ISSUE :: existence of some reader mini transactions, while performing recovery undo\n");
		exit(-1);
	}

	// initialize yet more uninitialized attributes of the mini transactions active at the time of crash
	for(mini_transaction* mt = (mini_transaction*) get_first_of_in_hashmap(&(mte->writer_mini_transactions), FIRST_OF_HASHMAP); mt != NULL; mt = (mini_transaction*) get_next_of_in_hashmap(&(mte->writer_mini_transactions), mt, ANY_IN_HASHMAP))
	{
		// mini transaction attributes like mini_transaction_id, lastLSN, state and abort_error are already initialized

		// now we need to initialize the below attributes
		mt->page_latches_held_counter = 0;
		mt->reference_counter = 1;

		// and if it is in progress, then abort it, with reason as aborted after crash
		if(mt->state == MIN_TX_IN_PROGRESS)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ABORTED_AFTER_CRASH;
		}
		else if(mt->state == MIN_TX_COMPLETED)
		{
			printf("ISSUE :: existence of a completed mini transaction, encountered while performing recovery undo\n");
			exit(-1);
		}
	}

	// now while there are writer mini transactions
	// NOTE :: we can not run a regular loop over writer mini transaxtions here, because we are releasing lock in the middle of the loop and the iterator could get invalidated
	while(!is_empty_hashmap(&(mte->writer_mini_transactions)))
	{
		// fetch the first one from this set and complete it
		mini_transaction* mt = (mini_transaction*) get_first_of_in_hashmap(&(mte->writer_mini_transactions), FIRST_OF_HASHMAP);
		pthread_mutex_unlock(&(mte->global_lock));

		// formally complete mt
		mte_complete_mini_tx(mte, mt, NULL, 0);

		pthread_mutex_lock(&(mte->global_lock));
	}

	// flush wal and bufferpool
	flush_wal_logs_and_wake_up_bufferpool_waiters_UNSAFE(mte);
	flush_all_possible_dirty_pages(&(mte->bufferpool_handle));

	// now at this point we can clean up the free lists
	// this is required as we allocated plenty of then and now they are redundant
	remove_all_from_linkedlist(&(mte->free_mini_transactions_list), AND_DELETE_MINI_TRANSACTIONS_NOTIFIER);
	remove_all_from_linkedlist(&(mte->free_dirty_page_entries_list), AND_DELETE_DIRTY_PAGE_TABLE_ENTRIES_NOTIFIER);

	pthread_mutex_unlock(&(mte->global_lock));
}

void recover(mini_transaction_engine* mte)
{
	checkpoint ckpt = analyze(mte); // runs to generate the checkpoint for the last state of the mini_transaction_engine's mini transaction table and dirty page table at the time of crash

	// TODO to be removed, only left here for testing analyze
	printf("printing checkpoint after analyze ::: \n");
	print_checkpoint(&ckpt);
	printf("\n");

	//redo(mte, &ckpt);				// consumes checkpoint and deinitizlizes it, and redos all log records from the minimum recLSN in the checkpoint

	//undo(mte); 						// undos uncommitted mini transactions
}