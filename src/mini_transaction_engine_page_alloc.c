#include<mintxengine/mini_transaction_engine_page_alloc.h>

#include<mintxengine/system_page_header_util.h>
#include<mintxengine/mini_transaction_engine_util.h>
#include<mintxengine/mini_transaction_engine_page_alloc_util.h>

int free_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t page_id)
{
	pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);

		// mini transaction is not in progress, then quit
		if(mt->state != MIN_TX_IN_PROGRESS)
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// if you are attempting to free a free space mapper page, then abort and quit
		if(is_free_space_mapper_page(page_id, &(mte->stats)))
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// you must not cross max page count, else abort and quit
		if(page_id >= mte->user_stats.max_page_count)
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		// check to ensure that you are not attempting to latch a page that is out of bounds for the current page count
		if(page_id >= mte->database_page_count) // this check must be done with manager_lock held
		{
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = ILLEGAL_PAGE_ID;
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return 0;
		}

		void* page_to_free = acquire_page_with_writer_latch_N_flush_wal_if_necessary_UNSAFE(mte, page_id, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
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
		if(mt_locked_by != NULL && mt_locked_by != mt) // if locked by an active transaction, we abort and quit
		{
			release_writer_lock_on_page(&(mte->bufferpool_handle), page_to_free, 0, 0); // was_modified = 0, force_flush = 0
			mt->state = MIN_TX_ABORTED;
			mt->abort_error = PAGE_TO_BE_FREED_IS_LOCKED;
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

void* get_new_page_with_write_latch_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, uint64_t* page_id_returned)
{
	void* new_page = NULL;

	// strategy : 1
	// allocate a new page firstly by attempting to do it without database expansion
	{
		pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		if(mt->state != MIN_TX_IN_PROGRESS) // mini transaction is not in progress, then quit
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			return NULL;
		}
		pthread_mutex_unlock(&(mte->global_lock));

		// this function needs to be called with shared_lock on manager_lock
		new_page = allocate_page_without_database_expansion_INTERNAL(mte, mt, page_id_returned);

		pthread_mutex_lock(&(mte->global_lock));
		if(new_page != NULL)// page was allocated, so latch acquisition was a success so increment the latch counter for this mini transaction
			mt->page_latches_held_counter++;
		shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));
	}

	if(new_page != NULL) // if we were successfull, quit
		return get_page_contents_for_page(new_page, (*page_id_returned), &(mte->stats));

	// strategy : 2
	// allocate a new page secondly by attempting to do it with database expansion
	{
		pthread_mutex_lock(&(mte->database_expansion_lock)); // database expansion lock should be taken before you take manager lock in shared mode, this allows these waiters not block the checkpointer

		pthread_mutex_lock(&(mte->global_lock));
		shared_lock(&(mte->manager_lock), READ_PREFERRING, BLOCKING);
		if(mt->state != MIN_TX_IN_PROGRESS) // mini transaction is not in progress, then quit
		{
			shared_unlock(&(mte->manager_lock));
			pthread_mutex_unlock(&(mte->global_lock));
			pthread_mutex_unlock(&(mte->database_expansion_lock)); // do not forget to release the database_expansion_lock, after an early return
			return NULL;
		}
		pthread_mutex_unlock(&(mte->global_lock));

		// this function needs to be called with shared_lock on manager_lock
		new_page = allocate_page_with_database_expansion_INTERNAL(mte, mt, page_id_returned);

		pthread_mutex_lock(&(mte->global_lock));
		if(new_page != NULL)// page was allocated, so latch acquisition was a success so increment the latch counter for this mini transaction
			mt->page_latches_held_counter++;
		shared_unlock(&(mte->manager_lock));
		pthread_mutex_unlock(&(mte->global_lock));

		pthread_mutex_unlock(&(mte->database_expansion_lock));
	}

	if(new_page != NULL) // if we were successfull, quit
		return get_page_contents_for_page(new_page, (*page_id_returned), &(mte->stats));

	return NULL;
}