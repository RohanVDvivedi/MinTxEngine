#include<mini_transaction_engine_page_alloc_util.h>

#include<mini_transaction_engine_util.h>
#include<system_page_header_util.h>

#include<bitmap.h>

int free_write_latched_page_INTERNAL(mini_transaction_engine* mte, mini_transaction* mt, void* page, uint64_t page_id)
{
	// fetch the free space mapper page an bit position that we need to flip
	uint64_t free_space_mapper_page_id = get_is_valid_bit_page_id_for_page(page_id, &(mte->stats));
	uint64_t free_space_mapper_bit_pos = get_is_valid_bit_position_for_page(page_id, &(mte->stats));
	void* free_space_mapper_page = acquire_page_with_writer_lock(&(mte->bufferpool_handle), free_space_mapper_page_id, mte->latch_wait_timeout_in_microseconds, 1, 0); // evict_dirty_if_necessary -> not to be overwritten
	if(free_space_mapper_page == NULL) // could not lock free_space_mapper_page, so abort
	{
		mt->state = MIN_TX_ABORTED;
		mt->abort_error = OUT_OF_BUFFERPOOL_MEMORY;
		return 0;
	}

	// perform full page writes for both the pages, if necessary
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, page, page_id);
	perform_full_page_write_for_page_if_necessary_and_manage_state_INTERNAL(mte, mt, free_space_mapper_page, free_space_mapper_page_id);

	// reset the free_space_mapper_bit_pos on the free_space_mapper_page
	{
		void* free_space_mapper_page_contents = get_page_contents_for_page(free_space_mapper_page, free_space_mapper_page_id, &(mte->stats));
		reset_bit(free_space_mapper_page_contents, free_space_mapper_bit_pos);
	}

	// construct page_deallocation log record


	// log the page deallocation log record and manage state
	pthread_mutex_lock(&(mte->global_lock));

	pthread_mutex_unlock(&(mte->global_lock));

	return 1;
}