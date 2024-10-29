#include<page_access_methods.h>
#include<page_modification_methods.h>

void* get_new_page_with_write_lock(void* context, const void* transaction_id, uint64_t* page_id_returned, int* abort_error)
{
	void* result = get_new_page_with_write_latch_for_mini_tx(context, transaction_id, page_id_returned);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void* acquire_page_with_reader_lock(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	void* result = acquire_page_with_reader_latch_for_mini_tx(context, transaction_id, page_id);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
void* acquire_page_with_writer_lock(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	void* result = acquire_page_with_writer_latch_for_mini_tx(context, transaction_id, page_id);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int downgrade_writer_lock_to_reader_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	int result = downgrade_writer_latch_to_reader_latch_on_page_for_mini_tx(context, transaction_id, pg_ptr, int opts);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int upgrade_reader_lock_to_writer_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int* abort_error)
{
	int result = upgrade_reader_latch_to_writer_latch_on_page_for_mini_tx(context, transaction_id, pg_ptr);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int release_reader_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	int result = release_reader_latch_on_page_for_mini_tx(context, transaction_id, pg_ptr, int opts);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int release_writer_lock_on_page(void* context, const void* transaction_id, void* pg_ptr, int opts, int* abort_error)
{
	int result = release_writer_latch_on_page_for_mini_tx(context, transaction_id, pg_ptr, int opts);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}
int free_page(void* context, const void* transaction_id, uint64_t page_id, int* abort_error)
{
	int result = free_page_for_mini_tx(context, transaction_id, page_id);
	(*abort_error) = ((mini_transaction*)transaction_id)->abort_error;
	return result;
}

page_access_methods pam;

void init_pam_for_mini_tx_engine(mini_transaction_engine* mte)
{
	pam = (page_access_methods){
		.get_new_page_with_write_lock = get_new_page_with_write_lock,
		.acquire_page_with_reader_lock = acquire_page_with_reader_lock,
		.acquire_page_with_writer_lock = acquire_page_with_writer_lock,
		.downgrade_writer_lock_to_reader_lock_on_page = downgrade_writer_lock_to_reader_lock_on_page,
		.upgrade_reader_lock_to_writer_lock_on_page = upgrade_reader_lock_to_writer_lock_on_page,
		.release_reader_lock_on_page = release_reader_lock_on_page,
		.release_writer_lock_on_page = release_writer_lock_on_page,
		.free_page = free_page,
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