#ifndef SYSTEM_PAGE_HEADER_UTIL_H
#define SYSTEM_PAGE_HEADER_UTIL_H

#include<serint/large_uints.h>

#include<mintxengine/mini_transaction_engine_stats.h>

/*
	system header consists of three things
	checksum - 32 bits/ 4 bytes wide on all pages, checksum of all the bytes on the page, except the first 4 bytes, which is the checksum itself
	pageLSN - as wide as log sequence number width on all pages, it is the last LSN that modified the page
	        - the page can be flushed to disk only if flushedLSN (of the system) >= pageLSN (of the page)
	writerLSN - as wide as log sequence number width on all data pages, it is the first LSN of the mini transaction that modified the page
              - it is not present on the free space bitmap pages
              - if a mini transaction with a writerLSN on the page exists and is in IN_PROGRESS or ABORTING state then we must release latch on that page and then wait for the writerLSN transaction to complete before accessing the page
*/

/*
	checksums are used only while read and writing data from-to disk, (update checksum on page while writng to disk and validate it while reading from disk)
	the checksums functions here must only be called while reading/writing data to-from disk
	we only protect you against disk corruption and never against main-memory corruption
	if you experience main-memory corruption you are on your own

	and as expected due to subsequent writes the in-memory copy of the page provided by the bufferpool may not have the most updated checksum, so there is no point in validating it when you receive the page from bufferpool
*/

// recalculates 32 bit page checksum and puts it on the page at designated location
uint32_t recalculate_page_checksum(void* page, const mini_transaction_engine_stats* stats);

// returns true if validation succeeds
int validate_page_checksum(const void* page, const mini_transaction_engine_stats* stats);

// get/set pageLSN on to the page, this is the last LSN that modified the page, present on all pages
uint256 get_pageLSN_for_page(const void* page, const mini_transaction_engine_stats* stats);
int set_pageLSN_for_page(void* page, uint256 pageLSN, const mini_transaction_engine_stats* stats);

// page_id = 0, is always a free_space_mapper_page
int is_free_space_mapper_page(uint64_t page_id, const mini_transaction_engine_stats* stats);

// this function can be used to iterate over free_space_mapper_pages sequentially
// fails with 0, if curr_free_space_mapper_page_id is not a free_space_mapper_page OR if the next_free_space_mapper_page_id is beyond the addressable range (i.e. overflows)
// page_id = 0, is always a free_space_mapper_page, then next_free_space_mapper_page_id is also the size of the extent in pages managed by any of the free_space_mapper_pages
int get_next_free_space_mapper_page_id(uint64_t curr_free_space_mapper_page_id, uint64_t* next_free_space_mapper_page_id, const mini_transaction_engine_stats* stats);

uint64_t is_valid_bits_count_on_free_space_mapper_page(const mini_transaction_engine_stats* stats);

// get page_id and bit_index for the is_valid bit of the page
// the result are invalid if the page itself is a free_space_mapper_page
uint64_t get_is_valid_bit_page_id_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats);
uint64_t get_is_valid_bit_position_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats);

// logically !is_free_space_mapper_page(), free space mapper page does not have writerLSN
int has_writerLSN_on_page(uint64_t page_id, const mini_transaction_engine_stats* stats);

// get/set writerLSN on to the page, this is the first LSN of the mini transaction that lastly modified the page
// i.e. writerLSN is the id of the mini_transaction that made latest modification to this page, we must wait for that mini transaction to complete, before accessing this page
uint256 get_writerLSN_for_page(const void* page, const mini_transaction_engine_stats* stats);
int set_writerLSN_for_page(void* page, uint256 writerLSN, const mini_transaction_engine_stats* stats);

// if it is a free_space_mapper page, then it contains 4 byte check_sum, and pageLSN
// else it contains 4 byte checksum, pageLSN and writerLSN
uint32_t get_system_header_size_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats);
uint32_t get_page_content_size_for_page(uint64_t page_id, const mini_transaction_engine_stats* stats);

uint32_t get_system_header_size_for_data_pages(const mini_transaction_engine_stats* stats);

uint32_t get_page_content_size_for_data_pages(const mini_transaction_engine_stats* stats);
uint32_t get_page_content_size_for_free_space_mapper_pages(const mini_transaction_engine_stats* stats);

// adds system header size for the page to the page
void* get_page_contents_for_page(void* page, uint64_t page_id, const mini_transaction_engine_stats* stats);
void* get_page_for_page_contents(void* page_contents, uint64_t page_id, const mini_transaction_engine_stats* stats);

// now each free_space_mapper_page is followed by a fixed number of data pages (which are allocatable), this collective group of a free_space_mapper_page and it's dependent data pages is from now on called as an extent

// below function converts a page_id into it's extent_id, extents are numbered from 0 onwards
// extent_id of a page also represents the total number of complete extents that exists before it
uint64_t get_extent_id_for_page_id(uint64_t page_id, const mini_transaction_engine_stats* stats);

#endif