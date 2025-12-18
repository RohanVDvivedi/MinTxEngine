#ifndef PAGE_ALLOCATION_HINTS_H
#define PAGE_ALLOCATION_HINTS_H

/*
	This is a separate module only converned with managing hints for page allocation over extents in the actual database file
	The term extent here is used for the first time in this project

	The extent is defined as a single free_space_mapper_page + data_pages of count = is_valid_bits_count_on_free_space_mapper_page(stats) number of pages

	This extent takes up 1 bit in the leaf most page in this hierarchial bitmap stored and managed by the page_allocation_hints (this) module

	Each page in this module is 4096 bytes bif, regardless of the page size in the actual database file

	This module is optional and may not be part of your actual page allocation flow
	It's pages are hints and not mini-transaction controlled ACID comliant, i.e. the do not get WAL-logged OR even stay in bufferpool for long, they are made to be flushed as quickly as possible
	And they are hints, and not souce of truth, they only suggest of a particular extent has any free pages in it, the actual source of truth of this information is the free space mapper page in the extent itself
*/

/*
	the page_allocation_hints module only deals with extent_id, which is a 64 bit integer (hard limit)
	and pages are allocated and extended as necessary
	each of its page is as said 4096 bytes big, i.e. have 4096*8 bits on them

	each of the bit is 0 if free and 1 if allocated, any page that is the last page and has only 0s is frees and released back to the disk/OS to repurpose
*/

/*
	The highest level page here becomes level 4, and go upto level 0
	the level 0 page has (4096*8) bits suggesting if any of those extents have free space in them (0 = has free page in it, 1 means all pages are allocated)
*/

/*
	page organization
	page_no -> level
	0    4
	1    3
	2    2
	3    1
	4    0
	5    0
	6    0
	7
	8
	9
	.
	.
	. after P 0 level pages
	.
	X    1
	. again P 0 level pages

	each level L page is (including it's own page) followed by 1 level L page, (4096*8) level L-1 pages, (4096*8)^2 level L-2 pages, until you reach 0 level pages

	lets define a function called K(L) = P^0 + P^1 + P^2 + ... + P^L where P = (4096*8)
	number of L level pages + L-1 level pages + L-2 level pages, ... sum until number of 0 level pages

	for any computation we only need the page_id = X and it's level Y and it's index in it's parent's subtree Z

	the page_id of the next sibling = X + K(Y) -> where 0 <= Z < P-1
	the page_id of the previous sibling = X - K(Y) -> where 0 < Z < P
	the page_id of its ith child = X + 1 + K(Y-1) -> Y != 0 and 0 < i < P
	the page_id of its parent = X - Z * K(Y) - 1

	to find the indices of the extent no E, just write the integer E in base P
	level 0 = E % P
	level 1 = (E / P) % P
	level 2 = (E / (P^2)) % P
	level 3 = (E / (P^3)) % P
	and so on until 4, which would obviously be (E / (P^4)) % P

	we will start descending from root page page_id = 0, at level 4, and (being at index 0 of its parent which does not exists)
	using the indices we found in the previous step
*/

// this value is fixed now for this implementation and there no scope of changing it, because we are going to have only 5 levels for now and for ever
#define PAGE_ALLOCATION_HINTS_PAGE_SIZE UINT64_C(4096)

// this was our P we discussed earlier
#define PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE (PAGE_ALLOCATION_HINTS_PAGE_SIZE * UINT64_C(8))

#include<blockio/block_io.h>
#include<bufferpool/bufferpool.h>

typedef struct page_allocation_hints page_allocation_hints;
struct page_allocation_hints
{
	// block_file consisting of the hints pages
	block_file extent_allocation_hints_file;

	// bufferpool for the above file, no need of any steal/force policy here, the pages are not WAL-logged and not undo-able
	bufferpool* bf;
};

#endif