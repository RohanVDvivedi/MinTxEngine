#ifndef SYSTEM_PAGE_HEADER_UTIL_H
#define SYSTEM_PAGE_HEADER_UTIL_H

#include<large_uints.h>

/*
	system header consists of three things
	checksum - 32 bits/ 4 bytes wide on all pages
	pageLSN - as wide as log sequence number width on all pages, it is the last LSN that modified the page
	        - the page can be flushed to disk only if flushedLSN (of the system) >= pageLSN (of the page)
	writerLSN - as wide as log sequence number width on all data pages, it is the first LSN of the mini transaction that modified the page
              - it is not present on the free space bitmap pages
              - if a mini transaction with a writerLSN on the page exists and is in IN_PROGRESS or ABORTING state then we must release latch on that page and then wait for the writerLSN transaction to complete before accessing the page
*/

// recalculates 32 bit page checksum and puts it on the page at designated location
uint32_t recalculate_page_checksum(void* page);

// returns true if validation succeeds
int validate_page_checksum(const void* page);

// get/set pageLSN on to the page, this is the last LSN that modified the page, present on all pages
uint256 get_pageLSN_for_page(const void* page);
int set_pageLSN_for_page(void* page, uint256 LSN);

int has_writerLSN_on_page(uint64_t page_id);

// get/set writerLSN on to the page, this is the first LSN of the mini transaction that lastly modified the page
// i.e. writerLSN is the id of the mini_transaction that made latest modification to this page, we must wait for that mini transaction to complete, before accessing this page
uint256 get_writerLSN_for_page(const void* page);
int set_writerLSN_for_page(const void* page, uint256 writerLSN);

#endif