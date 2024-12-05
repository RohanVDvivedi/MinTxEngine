#ifndef PAGE_IO_MODULE_H
#define PAGE_IO_MODULE_H

#include<mini_transaction_engine.h>

int read_page_from_database_file(mini_transaction_engine* mte, void* frame_dest, uint64_t page_id);

int write_page_to_database_file(mini_transaction_engine* mte, const void* frame_src, uint64_t page_id);

#endif