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
	the page_id of its ith child = X + 1 + i * K(Y-1) -> Y != 0 and 0 < i < P
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

// this value is fixed now (and forever!!) for this implementation and there no scope of changing it, because we are going to have only 5 levels for now and for ever
#define PAGE_ALLOCATION_HINTS_PAGE_SIZE UINT64_C(4096)

// this was our P we discussed earlier
#define PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE (PAGE_ALLOCATION_HINTS_PAGE_SIZE * UINT64_C(8))

// this is equal to smallest integral value where, 2^64 <= PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE ^ (MAX_LEVEL + 1)
// tjhis will remain fixed forever!!
#define MAX_LEVEL 4

#include<blockio/block_io.h>
#include<bufferpool/bufferpool.h>
#include<lockking/rwlock.h>
#include<cutlery/bst.h>

typedef struct page_allocation_hints page_allocation_hints;
struct page_allocation_hints
{
	// block_file consisting of the hints pages
	block_file extent_allocation_hints_file;

	// bufferpool for the above file, no need of any steal/force policy here, the pages are not WAL-logged and not undo-able
	// this bufferpool is small about mo more than 25 pages, each as expected 4KB in size
	bufferpool bf;

	// presistent memory structures end here and in-memory structures start from here

	// rwlock for the attributes below
	rwlock in_mem_lock;

	// recently allocated or free (->having any free page) extents are captured here (extent_ids in increasing order) before sent to the hint pages on the disk
	// these are the writes batched for the calls to update_hints_in_page_allocation_hints() function calls
	bst free_extents_set;
	bst full_extents_set;

	uint64_t write_batching_size;

	// max capacity for the free_extents_set and full_extents_set collectively, after which they must be flushed
	uint64_t write_batching_capacity;

	// maximum capacity possible and the size for the results_set, a cache for finding allocatable page extents quickly, without traversing bitmaps on disk
	uint64_t results_capacity;
	uint64_t results_size;

	// cached results for free extents, the reads first touch here, only later the bufferpool itself
	// this is the caches results for suggest_extents_from_page_allocation_hints() function calls
	bst results_set;
};

// fails if disk block size for extent_allocation_hints_file does not divide PAGE_ALLOCATION_HINTS_PAGE_SIZE
// the parameter is the name of the file for this module to be managed, ideally it should be the database_file_name.free_space_hints
page_allocation_hints* get_new_page_allocation_hints(uint64_t max_pages_to_buffer, char* extent_allocation_hints_file_path, uint64_t write_batching_capacity, uint64_t results_capacity);

void update_hints_in_page_allocation_hints(page_allocation_hints* pah_p, uint64_t extent_id, uint64_t free_pages_count_in_extent);

// result_extent_ids is the output parameter, and results_size is the in-out parameter suggesting the size of the array OR the size of the returned result
// (*results_size) = 0, is essentially a NOP
void suggest_extents_from_page_allocation_hints(page_allocation_hints* pah_p, uint64_t* result_extent_ids, uint32_t* results_size);

void flush_and_delete_page_allocation_hints(page_allocation_hints* pah_p);

// test functions below, they directly touch the hint file on the disk (through the bufferpool), and are only meant for debugging purpose and are not the api of this module, please refrain from using them directly
// the use functions instead use cached results for reads and batch writes for efficient disk usage and minimize write aplification

void update_hints_for_extents(page_allocation_hints* pah_p, uint64_t* free_extents_ids, uint64_t free_extents_ids_count, uint64_t* full_extent_ids, uint64_t full_extent_ids_count);

void find_free_extents(page_allocation_hints* pah_p, uint64_t from_extent_id, uint64_t* free_extents_ids, uint64_t* free_extents_ids_count);

#endif