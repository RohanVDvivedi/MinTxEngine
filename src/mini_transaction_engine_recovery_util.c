#include<mini_transaction_engine_recovery_util.h>

#include<mini_transaction_engine_checkpointer_util.h>
#include<mini_transaction_engine_util.h>

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
		// TODO

		// maintain and update mini_transaction_table in checkpoint
		// TODO

		// prepare for next iteration
		analyze_at = get_next_LSN_for_LSN_UNSAFE(mte, analyze_at);
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
	redo(mte, &ckpt);				// consumes checkpoint and deinitizlizes it, and redos all log records from the minimum recLSN in the checkpoint
	undo(mte); 						// undos uncommitted mini transactions
}