#include<mini_transaction_engine_page_alloc.h>

#include<system_page_header_util.h>
#include<mini_transaction_engine_util.h>
#include<mini_transaction_engine_page_alloc_util.h>

int free_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	pthread_mutex_lock(&(mte->global_lock));

		// mini transaction is not in progress, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// if you are attempting to free a free space mapper page, then abort and quit
		if(is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// you must not cross max page count, else abort and quit
		if(page_id >= mte->user_stats.max_page_count)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		shared_lock(&(mte->manager_lock), WRITE_PREFERRING, BLOCKING);

		// check to ensure that you are not attempting to latch a page that is out of bounds for the current page count
		if(page_id >= mte->database_page_count) // this check must be done with manager_lock held
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		void* page_to_free = acquire_page_with_writer_lock(&(mte->bufferpool_handle), page_id, mte->latch_wait_timeout_in_microseconds, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
		if(page_to_free == NULL)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// check who has persistent write lock on it
		mini_transaction* mt_locked_by = get_mini_transaction_that_last_persistent_write_locked_this_page_UNSAFE(mte, page_to_free);
		if(mt_locked_by == NULL || mt_locked_by == mt) // if locked by self or NULL, we are done
		{
			release_writer_lock_on_page(&(mte->bufferpool_handle), page_to_free, 0, 0); // was_modified = 0, force_flush = 0
			mt->state = PAGE_TO_BE_FREED_IS_LOCKED;
			mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		pthread_mutex_unlock(&(mte->global_lock));
		int result = free_write_latched_page_INTERNAL(mte, mt, page_to_free, page_id);
		pthread_mutex_lock(&(mte->global_lock));

		// if free was unsuccessfull, release latch on the page_to_free
		if(!result)
			release_writer_lock_on_page(&(mte->bufferpool_handle), page_to_free, 0, 0); // was_modified = 0, force_flush = 0

		shared_unlock(&(mte->manager_lock));
	pthread_mutex_unlock(&(mte->global_lock));

	return result;
}