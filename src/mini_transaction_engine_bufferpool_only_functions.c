#include<mini_transaction_engine_bufferpool_only_functions.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

void* acquire_page_with_reader_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	pthread_mutex_lock(&(mte->global_lock));

		// mini transaction is not in progres, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// if you are attempting to lock a free space mapper page, then quit
		if(is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// you must not cross max page count
		if(page_id >= mte->user_stats.max_page_count)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

		// check to ensure that you are not attempting to allocate a page that is out of bounds for the current page count

		void* latched_page = NULL;
		int attempts = 3;
		while(attempts > 0)
		{
			// attempt to acquire latch on this page with page_id
			latched_page = acquire_page_with_reader_lock(&(mte->bufferpool_handle), page_id, mte->latch_wait_timeout_in_milliseconds, 1);
			if(latched_page == NULL)
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
				break;
			}

			// check who has persistent write lock on it
			mini_transaction* mt_locked_by = get_mini_transaction_that_last_persistent_write_locked_this_page_UNSAFE(mte, latched_page);
			if(mt_locked_by == NULL || mt_locked_by == mt) // if locked by self or NULL, we are done
				break;

			// release latch on the latched page, this must succeed
			release_reader_lock_on_page(&(mte->bufferpool_handle), latched_page);
			latched_page = NULL;

			// wait for completion of a mt_locked_by mini transaction
			if(!wait_for_mini_transaction_completion_UNSAFE(mte, mt_locked_by))
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = PLAUSIBLE_DEADLOCK;
				break;
			}

			attempts--;
		}

		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	if(latched_page == NULL)
		return NULL;
	return latched_page - get_system_header_size_for_data_pages(&(mte->stats));
}

void* acquire_page_with_writer_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	// TODO
}

int downgrade_writer_latch_to_reader_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents)
{
	// TODO
}

int upgrade_reader_latch_to_writer_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents)
{
	// TODO
}

int release_reader_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page)
{
	// TODO
}

int release_writer_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page)
{
	// TODO
}