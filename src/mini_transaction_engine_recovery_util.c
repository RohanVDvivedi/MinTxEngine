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
					// here page compaction is missing, it is never undone
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
						printf("ISSUE :: encountered a CLR log record of a log record for a mini transaction that can not have a CLR record, while performing analyze of recovery, plausible bug or corruption\n");
						exit(-1);
					}
				}

				destroy_and_free_parsed_log_record(&undo_lr);
				break;
			}

			// if it is abort_mini_tx, completed_mini_tx or some later checkpoint log record OR user_info then do nothing
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

			// for checkpoint and user_info log records nothing needs to be done
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
	// a FULL_PAGE_WRITE log record has to be always redone
	if(lr->type == FULL_PAGE_WRITE)
	{
		void* page = NULL;
		while(page == NULL)
			page = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1, 1); // evict_dirty_if_necessary -> to be overwritten -> full page writes are to be overwritten by default
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

	// changes of lr need to be redone on the page, so return with latch on the page
	return page;
}

#include<bitmap.h>
#include<page_layout.h>

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

			case PAGE_ALLOCATION :
			case PAGE_DEALLOCATION :
			{
				uint64_t page_id = get_page_id_for_log_record(&lr);
				uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));

				{void* page = acquire_writer_latch_only_if_redo_required_UNSAFE(mte, ckpt, redo_at, &lr, page_id);
				if(page != NULL)
				{
					// lock the modification bit and the page
					set_writerLSN_for_page(page, get_mini_transaction_id_for_log_record(&lr), &(mte->stats));

					// set pageLSN on the page
					set_pageLSN_for_page(page, redo_at, &(mte->stats));

					// update checksum and release latch, while marking the page as dirty in mini transaction engine -> this reconstructs the dirty page table
					recalculate_page_checksum(page, &(mte->stats));
					mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
					release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
				}}

				{void* free_space_mapper_page = acquire_writer_latch_only_if_redo_required_UNSAFE(mte, ckpt, redo_at, &lr, free_space_mapper_page_id);
				if(free_space_mapper_page != NULL)
				{
					// actual redo
					{void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
					uint64_t free_space_mapper_bit_pos = get_is_valid_bit_position_for_page(page_id, &(mte->stats));
					if(lr.type == PAGE_ALLOCATION)
					{
						if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos) != 0) // this should never happen if write locks were held
						{
							printf("ISSUE :: unable to redo page allocation\n");
							exit(-1);
						}
						set_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
					}
					else if(lr.type == PAGE_DEALLOCATION)
					{
						if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos) != 1) // this should never happen if write locks were held
						{
							printf("ISSUE :: unable to redo page deallocation\n");
							exit(-1);
						}
						reset_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
					}
					else
					{
						printf("ISSUE :: this should never happen\n");
						exit(-1);
					}}

					// set pageLSN on the page
					set_pageLSN_for_page(free_space_mapper_page, redo_at, &(mte->stats));

					// update checksum and release latch, while marking the page as dirty in mini transaction engine -> this reconstructs the dirty page table
					recalculate_page_checksum(free_space_mapper_page, &(mte->stats));
					mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, free_space_mapper_page, free_space_mapper_page_id);
					release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
				}}

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
				uint64_t page_id = get_page_id_for_log_record(&lr);

				{void* page = acquire_writer_latch_only_if_redo_required_UNSAFE(mte, ckpt, redo_at, &lr, page_id);
				if(page != NULL)
				{
					// actual redo
					{void* page_contents = get_page_contents_for_page(page, page_id, &(mte->stats));
					switch(lr.type)
					{
						case PAGE_INIT :
						{
							if(!init_page(page_contents, mte->user_stats.page_size, lr.pilr.new_page_header_size, &(lr.pilr.new_size_def)))
							{
								printf("ISSUE :: unable to redo page init\n");
								exit(-1);
							}
							zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, &(lr.pilr.new_size_def)); // this is done on success in wale_only_functions so do it here as well
							break;
						}
						case PAGE_SET_HEADER :
						{
							void* page_header_contents = get_page_header(page_contents, mte->user_stats.page_size);
							{uint32_t page_header_size = get_page_header_size(page_contents, mte->user_stats.page_size);
							if(page_header_size != lr.pshlr.page_header_size) // this should never happen if write locks were held
							{
								printf("ISSUE :: unable to redo page set header, header size of the page and that of the log record does not match\n");
								exit(-1);
							}}
							memory_move(page_header_contents, lr.pshlr.new_page_header_contents, lr.pshlr.page_header_size);
							break;
						}
						case TUPLE_APPEND :
						{
							if(!append_tuple_on_page(page_contents, mte->user_stats.page_size, &(lr.talr.size_def), lr.talr.new_tuple))
							{
								printf("ISSUE :: unable to redo tuple append\n");
								exit(-1);
							}
							break;
						}
						case TUPLE_INSERT :
						{
							if(!insert_tuple_on_page(page_contents, mte->user_stats.page_size, &(lr.tilr.size_def), lr.tilr.insert_index, lr.tilr.new_tuple))
							{
								printf("ISSUE :: unable to redo tuple insert\n");
								exit(-1);
							}
							break;
						}
						case TUPLE_UPDATE :
						{
							if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(lr.tulr.size_def), lr.tulr.update_index, lr.tulr.new_tuple))
							{
								printf("ISSUE :: unable to redo tuple update\n");
								exit(-1);
							}
							break;
						}
						case TUPLE_DISCARD :
						{
							if(!discard_tuple_on_page(page_contents, mte->user_stats.page_size, &(lr.tdlr.size_def), lr.tdlr.discard_index))
							{
								printf("ISSUE :: unable to redo tuple discard\n");
								exit(-1);
							}
							break;
						}
						case TUPLE_DISCARD_ALL :
						{
							discard_all_tuples_on_page(page_contents, mte->user_stats.page_size, &(lr.tdalr.size_def));
							break;
						}
						case TUPLE_DISCARD_TRAILING_TOMB_STONES :
						{
							discard_trailing_tomb_stones_on_page(page_contents, mte->user_stats.page_size, &(lr.tdttlr.size_def));
							break;
						}
						case TUPLE_SWAP :
						{
							if(!swap_tuples_on_page(page_contents, mte->user_stats.page_size, &(lr.tslr.size_def), lr.tslr.swap_index1, lr.tslr.swap_index2))
							{
								printf("ISSUE :: unable to redo tuple swap\n");
								exit(-1);
							}
							break;
						}
						case TUPLE_UPDATE_ELEMENT_IN_PLACE :
						{
							if(!set_element_in_tuple_in_place_on_page(page_contents, mte->user_stats.page_size, &(lr.tueiplr.tpl_def), lr.tueiplr.tuple_index, lr.tueiplr.element_index, &(lr.tueiplr.new_element)))
							{
								printf("ISSUE :: unable to redo tuple update element in place\n");
								exit(-1);
							}
							break;
						}
						case PAGE_CLONE :
						{
							clone_page(page_contents, mte->user_stats.page_size, &(lr.pclr.size_def), lr.pclr.new_page_contents);
							break;
						}
						case PAGE_COMPACTION :
						{
							int memory_allocation_error = 0;
							run_page_compaction(page_contents, mte->user_stats.page_size, &(lr.pcptlr.size_def), &memory_allocation_error);
							if(memory_allocation_error)
							{
								printf("ISSUE :: memory allocation error while compacting the page for redo phase of recovery\n");
								exit(-1);
							}
							zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, &(lr.pcptlr.size_def));
							break;
						}
						default :
						{
							printf("ISSUE :: this should never happen\n");
							exit(-1);
						}
					}}

					// lock the modified page
					set_writerLSN_for_page(page, get_mini_transaction_id_for_log_record(&lr), &(mte->stats));

					// set pageLSN on the page
					set_pageLSN_for_page(page, redo_at, &(mte->stats));

					// update checksum and release latch, while marking the page as dirty in mini transaction engine -> this reconstructs the dirty page table
					recalculate_page_checksum(page, &(mte->stats));
					mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
					release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
				}}

				break;
			}

			case FULL_PAGE_WRITE :
			{
				uint64_t page_id = get_page_id_for_log_record(&lr);

				{void* page = acquire_writer_latch_only_if_redo_required_UNSAFE(mte, ckpt, redo_at, &lr, page_id);
				if(page != NULL)
				{
					// actual redo
					{void* page_contents = get_page_contents_for_page(page, page_id, &(mte->stats));
					uint32_t page_content_size = get_page_content_size_for_page(page_id, &(mte->stats));
					memory_move(page_contents, lr.fpwlr.page_contents, page_content_size);}

					// if it is not a free space mapper page, then set writerLSN from the log record
					if(!is_free_space_mapper_page(page_id, &(mte->stats)))
						set_writerLSN_for_page(page, lr.fpwlr.writerLSN, &(mte->stats));

					// set pageLSN on the page
					set_pageLSN_for_page(page, redo_at, &(mte->stats));

					// update checksum and release latch, while marking the page as dirty in mini transaction engine -> this reconstructs the dirty page table
					recalculate_page_checksum(page, &(mte->stats));
					mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
					release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
				}}

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
				// never set writerLSN here
				switch(undo_lr.type)
				{
					case PAGE_ALLOCATION :
					case PAGE_DEALLOCATION :
					{
						uint64_t page_id = get_page_id_for_log_record(&undo_lr);
						uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));

						{void* free_space_mapper_page = acquire_writer_latch_only_if_redo_required_UNSAFE(mte, ckpt, redo_at, &lr, free_space_mapper_page_id);
						if(free_space_mapper_page != NULL)
						{
							// actual redo
							{void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
							uint64_t free_space_mapper_bit_pos = get_is_valid_bit_position_for_page(page_id, &(mte->stats));
							if(undo_lr.type == PAGE_ALLOCATION)
							{
								if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos) != 1) // this should never happen if write locks were held
								{
									printf("ISSUE :: unable to redo the undo of page allocation\n");
									exit(-1);
								}
								reset_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
							}
							else if(undo_lr.type == PAGE_DEALLOCATION)
							{
								if(get_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos) != 0) // this should never happen if write locks were held
								{
									printf("ISSUE :: unable to redo the undo of page deallocation\n");
									exit(-1);
								}
								set_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
							}
							else
							{
								printf("ISSUE :: this should never happen\n");
								exit(-1);
							}}

							// set pageLSN on the page
							set_pageLSN_for_page(free_space_mapper_page, redo_at, &(mte->stats));

							// update checksum and release latch, while marking the page as dirty in mini transaction engine -> this reconstructs the dirty page table
							recalculate_page_checksum(free_space_mapper_page, &(mte->stats));
							mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, free_space_mapper_page, free_space_mapper_page_id);
							release_writer_lock_on_page(&(mte->bufferpool_handle), free_space_mapper_page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
						}}

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
					// here page compaction is mission it is never undone
					{
						uint64_t page_id = get_page_id_for_log_record(&undo_lr);

						{void* page = acquire_writer_latch_only_if_redo_required_UNSAFE(mte, ckpt, redo_at, &lr, page_id);
						if(page != NULL)
						{
							// actual redo
							{void* page_contents = get_page_contents_for_page(page, page_id, &(mte->stats));
								switch(undo_lr.type)
								{
									case PAGE_INIT :
									{
										memory_move(page_contents, undo_lr.pilr.old_page_contents, mte->user_stats.page_size);
										break;
									}
									case PAGE_SET_HEADER :
									{
										void* page_header = get_page_header(page_contents, mte->user_stats.page_size);
										uint32_t page_header_size = get_page_header_size(page_contents, mte->user_stats.page_size);
										if(page_header_size != undo_lr.pshlr.page_header_size) // this should never happen if write locks were held
										{
											printf("ISSUE :: unable to redo the undo of page set header, header size of the page and that of the log record does not match\n");
											exit(-1);
										}
										memory_move(page_header, undo_lr.pshlr.old_page_header_contents, page_header_size);
										break;
									}
									case TUPLE_APPEND :
									{
										uint32_t tuple_count = get_tuple_count_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.talr.size_def));
										if(tuple_count == 0) //this should never happen if write locks were held
										{
											printf("ISSUE :: will not be able to redo the undo of tuple append, because current uple count is 0\n");
											exit(-1);
										}
										if(!discard_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.talr.size_def), tuple_count - 1)) // this should never happen if write locks were held
										{
											printf("ISSUE :: unable to redo the undo of tuple append\n");
											exit(-1);
										}
										break;
									}
									case TUPLE_INSERT :
									{
										if(!discard_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tilr.size_def), undo_lr.tilr.insert_index)) // this should never happen if write locks were held
										{
											printf("ISSUE :: unable to redo the undo of tuple insert\n");
											exit(-1);
										}
										break;
									}
									case TUPLE_UPDATE :
									{
										int undone = update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tulr.size_def), undo_lr.tulr.update_index, undo_lr.tulr.old_tuple);
										if(!undone)
										{
											if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tulr.size_def), undo_lr.tulr.update_index, NULL)) // this should never fail
											{
												printf("ISSUE :: unable to set NULL to a tuple :: this should never happen\n");
												exit(-1);
											}
											int memory_allocation_error = 0;
											run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr.tulr.size_def), &memory_allocation_error);
											if(memory_allocation_error) // malloc failed on compaction
											{
												printf("ISSUE :: unable to redo the undo of tuple update, due to failure to callocate memory for page compaction\n");
												exit(-1);
											}
											zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tulr.size_def));
											if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tulr.size_def), undo_lr.tulr.update_index, undo_lr.tulr.old_tuple)) // this should never happen if write locks were held
											{
												printf("ISSUE :: unable to redo the undo of tuple update\n");
												exit(-1);
											}
										}
										break;
									}
									case TUPLE_DISCARD :
									{
										int undone = insert_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tdlr.size_def), undo_lr.tdlr.discard_index, undo_lr.tdlr.old_tuple);
										if(!undone)
										{
											int memory_allocation_error = 0;
											run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr.tdlr.size_def), &memory_allocation_error);
											if(memory_allocation_error) // malloc failed on compaction
											{
												printf("ISSUE :: unable to redo the undo of tuple discard, due to failure to allocate memory for page compaction\n");
												exit(-1);
											}
											zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tdlr.size_def));
											if(!insert_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tdlr.size_def), undo_lr.tdlr.discard_index, undo_lr.tdlr.old_tuple)) // this should never happen if write locks were held
											{
												printf("ISSUE :: unable to redo the undo of tuple discard, even after a compaction\n");
												exit(-1);
											}
										}
										break;
									}
									case TUPLE_DISCARD_ALL :
									{
										memory_move(page_contents, undo_lr.tdalr.old_page_contents, mte->user_stats.page_size);
										break;
									}
									case TUPLE_DISCARD_TRAILING_TOMB_STONES :
									{
										for(uint32_t i = 0; i < undo_lr.tdttlr.discarded_trailing_tomb_stones_count; i++)
										{
											int undone = append_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tdttlr.size_def), NULL);
											if(undone)
												continue;
											int memory_allocation_error = 0;
											run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr.tdttlr.size_def), &memory_allocation_error);
											if(memory_allocation_error) // malloc failed for compaction
											{
												printf("ISSUE :: unable to redo the undo of tuple discard trailing tombstones, due to failure to callocate memory for page compaction\n");
												exit(-1);
											}
											zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tdttlr.size_def));
											if(!append_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tdttlr.size_def), NULL)) // this should never happen if write locks were held
											{
												printf("ISSUE :: unable to redo the undo of tuple discard trailing tombstones, even after a compaction\n");
												exit(-1);
											}
										}
										break;
									}
									case TUPLE_SWAP :
									{
										if(!swap_tuples_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tslr.size_def), undo_lr.tslr.swap_index1, undo_lr.tslr.swap_index2)) // this should never happen if write locks were held
										{
											printf("ISSUE :: unable to redo the undo of tuple swap\n");
											exit(-1);
										}
										break;
									}
									case TUPLE_UPDATE_ELEMENT_IN_PLACE :
									{
										if(NULL == get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def.size_def), undo_lr.tueiplr.tuple_index))
										{
											printf("ISSUE :: unable to redo the undo of tuple update element in place, tuple itself is NULL\n");
											exit(-1);
										}
										int undone = set_element_in_tuple_in_place_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def), undo_lr.tueiplr.tuple_index, undo_lr.tueiplr.element_index, &(undo_lr.tueiplr.old_element));
										if(!undone)
										{
											void* new_tuple = NULL;
											{
												// get pointer to the current on page tuple
												const void* on_page_tuple = get_nth_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def.size_def), undo_lr.tueiplr.tuple_index);

												// clone it into new tuple
												new_tuple = malloc(mte->user_stats.page_size);
												if(new_tuple == NULL)
												{
													printf("ISSUE :: unable to redo the undo of tuple update element in place, memory allocation for new tuple failed\n");
													exit(-1);
												}
												memory_move(new_tuple, on_page_tuple, get_tuple_size(&(undo_lr.tueiplr.tpl_def), on_page_tuple));
											}

											// perform set element on the new tuple, this must succeed
											if(!set_element_in_tuple(&(undo_lr.tueiplr.tpl_def), undo_lr.tueiplr.element_index, new_tuple, &(undo_lr.tueiplr.old_element), UINT32_MAX))
											{
												printf("ISSUE :: unable to redo the undo of tuple update element in place, set tuple on fallback failed\n");
												exit(-1);
											}

											// discard old tuple on the page and run page compaction
											{
												if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def.size_def), undo_lr.tueiplr.tuple_index, NULL)) // this should never fail
												{
													printf("ISSUE :: unable to set NULL to a tuple :: this should never happen\n");
													exit(-1);
												}
												int memory_allocation_error = 0;
												run_page_compaction(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def.size_def), &memory_allocation_error);
												if(memory_allocation_error) // malloc failed on compaction
												{
													printf("ISSUE :: unable to redo the undo of tuple update element in place, due to failure to allocate memory for page compaction\n");
													exit(-1);
												}
												zero_out_free_space_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def.size_def));
											}

											// perform update for new tuple on the page
											if(!update_tuple_on_page(page_contents, mte->user_stats.page_size, &(undo_lr.tueiplr.tpl_def.size_def), undo_lr.tueiplr.tuple_index, new_tuple)) // this should never happen if write locks were held
											{
												printf("ISSUE :: unable to redo the undo of tuple update element in place\n");
												exit(-1);
											}

											free(new_tuple);
										}
										break;
									}
									case PAGE_CLONE :
									{
										memory_move(page_contents, undo_lr.pclr.old_page_contents, mte->user_stats.page_size);
										break;
									}
									default : // if you reach here it is a bug
									{
										printf("ISSUE :: unable to redo the undo of log record of an illegal type, which was already filtered\n");
										exit(-1);
									}
								}
							}

							// never set writer lsn here

							// set pageLSN on the page
							set_pageLSN_for_page(page, redo_at, &(mte->stats));

							// update checksum and release latch, while marking the page as dirty in mini transaction engine -> this reconstructs the dirty page table
							recalculate_page_checksum(page, &(mte->stats));
							mark_page_as_dirty_in_bufferpool_and_dirty_page_table_UNSAFE(mte, page, page_id);
							release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
						}}

						break;
					}

					default :
					{
						printf("ISSUE :: encountered a CLR log record of a log record for a mini transaction that can not have a CLR record while performing redo of recovery, plausible bug or corruption\n");
						exit(-1);
					}
				}

				destroy_and_free_parsed_log_record(&undo_lr);
				break;
			}

			// on any other log record like checkpoint OR user_info, then nothing needs to be done
			default :
			{
				break;
			}
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
		// flush_on_completion = 0
		mte_complete_mini_tx(mte, mt, 0, NULL, 0);

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

	redo(mte, &ckpt);				// consumes checkpoint and deinitizlizes it, and redos all log records from the minimum recLSN in the checkpoint

	undo(mte); 						// undos uncommitted mini transactions
}