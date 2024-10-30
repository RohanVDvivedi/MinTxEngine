#include<page_access_methods.h>
#include<page_modification_methods.h>

#define GENERATE_TRACE

void* get_new_page_with_write_lock_mtx(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p get_new_page_with_write_lock_mtx\n", transaction_id);
	#endif
	void* result = get_new_page_with_write_latch_for_mini_tx(context, (void*)transaction_id, page_id_returned);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void* acquire_page_with_reader_lock_mtx(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p acquire_page_with_reader_lock_mtx\n", transaction_id);
	#endif
	void* result = acquire_page_with_reader_latch_for_mini_tx(context, (void*)transaction_id, page_id);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void* acquire_page_with_writer_lock_mtx(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p acquire_page_with_writer_lock_mtx\n", transaction_id);
	#endif
	void* result = acquire_page_with_writer_latch_for_mini_tx(context, (void*)transaction_id, page_id);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int downgrade_writer_lock_to_reader_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p downgrade_writer_lock_to_reader_lock_on_page_mtx\n", transaction_id);
	#endif
	int result = downgrade_writer_latch_to_reader_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int upgrade_reader_lock_to_writer_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p upgrade_reader_lock_to_writer_lock_on_page_mtx\n", transaction_id);
	#endif
	int result = upgrade_reader_latch_to_writer_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int release_reader_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p release_reader_lock_on_page_mtx\n", transaction_id);
	#endif
	int result = release_reader_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr, !!(opts & FREE_PAGE));
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int release_writer_lock_on_page_mtx(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p release_writer_lock_on_page_mtx\n", transaction_id);
	#endif
	int result = release_writer_latch_on_page_for_mini_tx(context, (void*)transaction_id, pg_ptr, !!(opts & FREE_PAGE));
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int free_page_mtx(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p free_page_mtx\n", transaction_id);
	#endif
	int result = free_page_for_mini_tx(context, (void*)transaction_id, page_id);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}

page_access_methods pam;

void init_pam_for_mini_tx_engine(mini_transaction_engine* mte)
{
	pam = (page_access_methods){
		.get_new_page_with_write_lock = get_new_page_with_write_lock_mtx,
		.acquire_page_with_reader_lock = acquire_page_with_reader_lock_mtx,
		.acquire_page_with_writer_lock = acquire_page_with_writer_lock_mtx,
		.downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page_mtx,
		.upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page_mtx,
		.release_reader_lock_on_page = release_reader_lock_on_page_mtx,
		.release_writer_lock_on_page = release_writer_lock_on_page_mtx,
		.free_page = free_page_mtx,
		.pas = (page_access_specs){
			.page_id_width = mte->user_stats.page_id_width,
			.page_size = mte->user_stats.page_size,
			.NULL_PAGE_ID = mte->user_stats.NULL_PAGE_ID,
			.system_header_size = 0,
		},
		.context = mte,
	};
	pam.pas.page_id_type_info = define_uint_non_nullable_type("page_id", pam.pas.page_id_width);
	initialize_tuple_def(&(pam.pas.page_id_tuple_def), &(pam.pas.page_id_type_info));
}

int init_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, uint32_t page_header_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p init_page_mtx\n", transaction_id);
	#endif
	int result = init_page_for_mini_tx(context, (void*)transaction_id, ppage.page, page_header_size, tpl_sz_d);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void set_page_header_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const void* hdr, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p set_page_header_mtx\n", transaction_id);
	#endif
	set_page_header_for_mini_tx(context, (void*)transaction_id, ppage.page, hdr);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return ;
}
int append_tuple_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, const void* external_tuple, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p append_tuple_on_page_mtx\n", transaction_id);
	#endif
	int result = append_tuple_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d, external_tuple);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int insert_tuple_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p insert_tuple_on_page_mtx\n", transaction_id);
	#endif
	int result = insert_tuple_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d, index, external_tuple);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int update_tuple_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p update_tuple_on_page_mtx\n", transaction_id);
	#endif
	int result = update_tuple_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d, index, external_tuple);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int discard_tuple_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t index, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p discard_tuple_on_page_mtx\n", transaction_id);
	#endif
	int result = discard_tuple_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d, index);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void discard_all_tuples_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p discard_all_tuples_on_page_mtx\n", transaction_id);
	#endif
	discard_all_tuples_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return ;
}
uint32_t discard_trailing_tomb_stones_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p discard_trailing_tomb_stones_on_page_mtx\n", transaction_id);
	#endif
	uint32_t result = discard_trailing_tomb_stones_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int swap_tuples_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p swap_tuples_on_page_mtx\n", transaction_id);
	#endif
	int result = swap_tuples_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d, i1, i2);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int set_element_in_tuple_in_place_on_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const user_value* value, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p set_element_in_tuple_in_place_on_page_mtx\n", transaction_id);
	#endif
	int result = set_element_in_tuple_in_place_on_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_d, tuple_index, element_index, value);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void clone_page_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, persistent_page ppage_src, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p clone_page_mtx\n", transaction_id);
	#endif
	clone_page_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d, ppage_src.page);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return ;
}
int run_page_compaction_mtx(void* context, const void* transaction_id, persistent_page ppage, uint32_t page_size, const tuple_size_def* tpl_sz_d, int* abort_error)
{
	#ifdef GENERATE_TRACE
		printf("%p run_page_compaction_mtx\n", transaction_id);
	#endif
	int result = run_page_compaction_for_mini_tx(context, (void*)transaction_id, ppage.page, tpl_sz_d);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}

page_modification_methods pmm;

void init_pmm_for_mini_tx_engine(mini_transaction_engine* mte)
{
	pmm = (page_modification_methods){
		.init_page = init_page_mtx,
		.set_page_header = set_page_header_mtx,
		.append_tuple_on_page = append_tuple_on_page_mtx,
		.insert_tuple_on_page = insert_tuple_on_page_mtx,
		.update_tuple_on_page = update_tuple_on_page_mtx,
		.discard_tuple_on_page = discard_tuple_on_page_mtx,
		.discard_all_tuples_on_page = discard_all_tuples_on_page_mtx,
		.discard_trailing_tomb_stones_on_page = discard_trailing_tomb_stones_on_page_mtx,
		.swap_tuples_on_page = swap_tuples_on_page_mtx,
		.set_element_in_tuple_in_place_on_page = set_element_in_tuple_in_place_on_page_mtx,
		.clone_page = clone_page_mtx,
		.run_page_compaction = run_page_compaction_mtx,
		.context = mte,
	};
}