#ifndef MINI_TRANSACTION_ENGINE_WALE_ONLY_FUNCTIONS_H
#define MINI_TRANSACTION_ENGINE_WALE_ONLY_FUNCTIONS_H

#include<mintxengine/mini_transaction_engine.h>

int init_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, uint32_t page_header_size, const tuple_size_def* tpl_sz_d);

void set_page_header_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const void* hdr);

int append_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, const void* external_tuple);

int insert_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

int update_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index, const void* external_tuple);

int discard_tuple_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t index);

void discard_all_tuples_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d);

uint32_t discard_trailing_tomb_stones_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d);

int swap_tuples_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, uint32_t i1, uint32_t i2);

int set_element_in_tuple_in_place_on_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_def* tpl_d, uint32_t tuple_index, positional_accessor element_index, const datum* value);

void clone_page_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d, const void* page_contents_src);

int run_page_compaction_for_mini_tx(mini_transaction_engine* mte, mini_transaction* mt, void* page_contents, const tuple_size_def* tpl_sz_d);

#endif