#include<mini_transaction_engine_bufferpool_only_functions.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

void* acquire_page_with_reader_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

		// mini transaction is not in progress, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		// if you are attempting to lock a free space mapper page, then abort and quit
		if(is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		// you must not cross max page count, else abort and quit
		if(page_id >= mte->user_stats.max_page_count)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		// check to ensure that you are not attempting to latch a page that is out of bounds for the current page count
		if(page_id >= mte->database_page_count) // this check must be done with manager_lock held
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		void* latched_page = NULL;
		uint64_t write_lock_wait_timeout_in_microseconds_LEFT = mte->write_lock_wait_timeout_in_microseconds;
		while(1)
		{
			// attempt to acquire latch on this page with page_id
			latched_page = acquire_page_with_reader_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1); // evict_dirty_if_necessary
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

			if(write_lock_wait_timeout_in_microseconds_LEFT == 0) // no wait attempts left
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = PLAUSIBLE_DEADLOCK;
				break;
			}

			// wait for completion of a mt_locked_by mini transaction
			if(!wait_for_mini_transaction_completion_UNSAFE(mte, mt_locked_by, &write_lock_wait_timeout_in_microseconds_LEFT))
			{
				// comes here when we time out, this could be because of a PLAUSIBLE_DEADLOCK
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = PLAUSIBLE_DEADLOCK;
				break;
			}

			// we waited until completion of a mini transaction, we must try again and test if the latch could be acquired without any contention, so continue
			continue;
		}

		// a new page latch is granted, so increment the latch counter for this mini transaction
		if(latched_page != NULL)
			mt->page_latches_held_counter++;

		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	if(latched_page == NULL)
		return NULL;
	return latched_page + get_system_header_size_for_data_pages(&(mte->stats));
}

void* acquire_page_with_writer_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

		// mini transaction is not in progress, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		// if you are attempting to lock a free space mapper page, then abort and quit
		if(is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		// you must not cross max page count, else abort and quit
		if(page_id >= mte->user_stats.max_page_count)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		// check to ensure that you are not attempting to latch a page that is out of bounds for the current page count
		if(page_id >= mte->database_page_count) // this check must be done with manager_lock held
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}

		void* latched_page = NULL;
		uint64_t write_lock_wait_timeout_in_microseconds_LEFT = mte->write_lock_wait_timeout_in_microseconds;
		while(1)
		{
			// attempt to acquire latch on this page with page_id
			latched_page = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
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
			// this page was never movidied, so no need to recalculate_checksum here
			release_writer_lock_on_page(&(mte->bufferpool_handle), latched_page, 0, 0); // was_modified = 0, force_flush = 0 -> so that global lock is not released while we are working
			latched_page = NULL;

			if(write_lock_wait_timeout_in_microseconds_LEFT == 0) // no wait attempts left
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = PLAUSIBLE_DEADLOCK;
				break;
			}

			// wait for completion of a mt_locked_by mini transaction
			if(!wait_for_mini_transaction_completion_UNSAFE(mte, mt_locked_by, &write_lock_wait_timeout_in_microseconds_LEFT))
			{
				// comes here when we time out, this could be because of a PLAUSIBLE_DEADLOCK
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = PLAUSIBLE_DEADLOCK;
				break;
			}

			// we waited until completion of a mini transaction, we must try again and test if the latch could be acquired without any contention, so continue
			continue;
		}

		// a new page latch is granted, so increment the latch counter for this mini transaction
		if(latched_page != NULL)
			mt->page_latches_held_counter++;

		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	if(latched_page == NULL)
		return NULL;
	return latched_page + get_system_header_size_for_data_pages(&(mte->stats));
}

int downgrade_writer_latch_to_reader_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents)
{
	int result = 0;

	pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

		// mini transaction is not in progress, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// recalculate page checksum, prior to downgrading the latch, as the user might have modified the page
		pthread_mutex_unlock(&(mte->global_lock));
		recalculate_page_checksum(page, &(mte->stats));
		pthread_mutex_lock(&(mte->global_lock));

		result = downgrade_writer_lock_to_reader_lock(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0

		if(!result)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = UNABLE_TO_TRANSITION_LOCK;
		}

		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

int upgrade_reader_latch_to_writer_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents)
{
	int result = 0;

	pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

		// mini transaction is not in progress, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
		uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
		if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		result = upgrade_reader_lock_to_writer_lock(&(mte->bufferpool_handle), page);

		if(!result)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = UNABLE_TO_TRANSITION_LOCK;
		}

		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}

#include<mini_transaction_engine_page_alloc_util.h>

int release_reader_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page)
{
	if(!free_page) // simple release latch
	{
		int result = 0;

		pthread_mutex_lock(&(mte->global_lock));

			// you can release latches with mini transaction being in any state

			shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

			void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
			if(mt != NULL) // you can only possibly abort a non-null mini transaction
			{
				uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
				if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
				{
					mt->state = MIN_TX_ABORTED;
					mt->abort_error = ILLEGAL_PAGE_ID;
					shared_unlock(&(mte->manager_lock));
					pthread_mutex_unlock(&(mte->global_lock));
					return 0;
				}
			}

			result = release_reader_lock_on_page(&(mte->bufferpool_handle), page);

			if(mt != NULL) // you can only possibly abort a non-null mini transaction
			{
				if(!result)
				{
					mt->state = MIN_TX_ABORTED;
					mt->abort_error = UNABLE_TO_TRANSITION_LOCK;
				}
				else // latch release was a success so decrement the latch counter for this mini transaction
					mt->page_latches_held_counter--;
			}

			shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));

		return result;
	}
	else // release latch + free page
	{
		int result = 0;

		pthread_mutex_lock(&(mte->global_lock));
			shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

			// mini transaction is not in progress, then quit
			if(mt->state != MIN_TX_IN_PROGRESS)
			{
				shared_unlock(&(mte->manager_lock));
				pthread_mutex_unlock(&(mte->global_lock));
				return 0;
			}

			void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
			uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
			if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = ILLEGAL_PAGE_ID;
				shared_unlock(&(mte->manager_lock));
				pthread_mutex_unlock(&(mte->global_lock));
				return 0;
			}

			// attempt to upgrade to writer lock on the page
			if(!upgrade_reader_lock_to_writer_lock(&(mte->bufferpool_handle), page))
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = UNABLE_TO_TRANSITION_LOCK;
				shared_unlock(&(mte->manager_lock));
				pthread_mutex_unlock(&(mte->global_lock));
				return 0;
			}

			pthread_mutex_unlock(&(mte->global_lock));

			// perform the actual free here
			result = free_write_latched_page_INTERNAL(mte, mt, page, page_id);

			pthread_mutex_lock(&(mte->global_lock));

			// on failure do downgrade the lock back, no need to recalculate the checksum as the pages are as if never modified
			if(!result)
				// this must succeed if the prior upgrade call succeeded
				downgrade_writer_lock_to_reader_lock(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0
			else
				// page was freed, so latch release was a success so decrement the latch counter for this mini transaction
				mt->page_latches_held_counter--;

			shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));

		return result;
	}
}

int release_writer_latch_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, int free_page)
{
	if(!free_page) // simple release latch
	{
		int result = 0;

		pthread_mutex_lock(&(mte->global_lock));

			// you can release latches with mini transaction being in any state

			shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

			void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
			if(mt != NULL) // you can only possibly abort a non-null mini transaction
			{
				uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
				if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
				{
					mt->state = MIN_TX_ABORTED;
					mt->abort_error = ILLEGAL_PAGE_ID;
					shared_unlock(&(mte->manager_lock));
					pthread_mutex_unlock(&(mte->global_lock));
					return 0;
				}
			}

			// recalculate page checksum, prior to releasing the latch, as the user might have modified the page
			pthread_mutex_unlock(&(mte->global_lock));
			recalculate_page_checksum(page, &(mte->stats));
			pthread_mutex_lock(&(mte->global_lock));

			result = release_writer_lock_on_page(&(mte->bufferpool_handle), page, 0, 0); // marking was_modified to 0, as all updates are already marking it dirty, and force_flush = 0

			if(mt != NULL) // you can only possibly abort a non-null mini transaction
			{
				if(!result)
				{
					mt->state = MIN_TX_ABORTED;
					mt->abort_error = UNABLE_TO_TRANSITION_LOCK;
				}
				else // latch release was a success so decrement the latch counter for this mini transaction
					mt->page_latches_held_counter--;
			}

			shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));

		return result;
	}
	else // release latch + free page
	{
		int result = 0;

		pthread_mutex_lock(&(mte->global_lock));
			shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

			// mini transaction is not in progress, then quit
			if(mt->state != MIN_TX_IN_PROGRESS)
			{
				shared_unlock(&(mte->manager_lock));
				pthread_mutex_unlock(&(mte->global_lock));
				return 0;
			}

			void* page = page_contents - get_system_header_size_for_data_pages(&(mte->stats));
			uint64_t page_id = get_page_id_for_locked_page(&(mte->bufferpool_handle), page);
			if(page_id >= mte->database_page_count || is_free_space_mapper_page(page_id, &(mte->stats)))
			{
				mt->state = MIN_TX_ABORTED;
				mt->abort_error = ILLEGAL_PAGE_ID;
				shared_unlock(&(mte->manager_lock));
				pthread_mutex_unlock(&(mte->global_lock));
				return 0;
			}

			// recalculate page checksum, prior to releasing the latch, as the user might have modified the page
			pthread_mutex_unlock(&(mte->global_lock));

			recalculate_page_checksum(page, &(mte->stats)); // free call below will surely free the page and release latch, and if it succeeds it will update checksums, but we are also doing it here just to be carefull, so this line can be commented
			result = free_write_latched_page_INTERNAL(mte, mt, page, page_id);

			pthread_mutex_lock(&(mte->global_lock));

			if(result)
				// page was freed, so latch release was a success so decrement the latch counter for this mini transaction
				mt->page_latches_held_counter--;

			shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));

		return result;
	}
}