#include<mini_transaction_engine_allotment.h>

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

		// in the begining every mini transaction is a reader_mini_transaction
		insert_head_in_linkedlist(&(mte->reader_mini_transactions), mt);
	}

	pthread_mutex_unlock(&(mte->global_lock));

	return mt;
}