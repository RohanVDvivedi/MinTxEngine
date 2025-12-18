#include<mintxengine/page_allocation_hints.h>

// each hint page node, in the free space hint hierarchy of the tree is identified by the hint_node_id
// below are the utility functions for managing the hint_node_ids

typedef struct hint_node_id hint_node_id;
struct hint_node_id
{
	uint8_t level; // values from 4 to 0, both inclusive

	uint64_t page_id; // page_id of the hint_page we are working with, each page is 4096 bytes big

	uint64_t child_index; // this is the index of this node in it's immediate parent's subtree
};

static inline hint_node_id get_root_page_hint_node_id()
{
	return (hint_node_id) {
		.level = 4,				// root page is at level 4 and page_id = 0
		.page_id = 0,
		.child_index = 0,		//and it is at 0th index in its non existent parent
	};
}

// returns PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE ^ p
// caches values statically, on first call
static inline uint64_t get_power_for_bits_count_on_the_node_page(uint8_t p, int* overflowed)
{
	(*overflowed) = 0;

	// only 0 to 4 values do not overflow
	switch(p)
	{
		case 0 :
			return UINT64_C(1);
		case 1 :
			return PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
		case 2 :
			return PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
		case 3 :
			return PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
		case 4 :
			return PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
	}

	// 5 and onwards it overflows
	(*overflowed) = 1;
	return 0;
}

// returns summation (from 0 to p, both inclusive) for PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE ^ i
// caches values statically, on first call
static inline uint64_t get_sum_of_powers_for_bits_count_on_the_node_page(uint8_t p, int* overflowed)
{
	(*overflowed) = 0;

	#define get_power_for_bits_count_on_the_node_page pow_bits

	// only 0 to 4 values do not overflow
	switch(p)
	{
		case 0 :
			return pow_bits(0);
		case 1 :
			return pow_bits(0) + pow_bits(1);
		case 2 :
			return pow_bits(0) + pow_bits(1) + pow_bits(2);
		case 3 :
			return pow_bits(0) + pow_bits(1) + pow_bits(2) + pow_bits(3);
		case 4 :
			return pow_bits(0) + pow_bits(1) + pow_bits(2) + pow_bits(3) + pow_bits(4);
	}

	#undef get_power_for_bits_count_on_the_node_page

	// 5 and onwards it overflows
	(*overflowed) = 1;
	return 0;
}

static inline hint_node_id get_next_sibling_for_hint_node_id(hint_node_id x);

static inline hint_node_id get_prev_sibling_for_hint_node_id(hint_node_id x);

static inline hint_node_id get_ith_child_for_hint_node_id(hint_node_id x, uint64_t i);

static inline hint_node_id get_paret_for_hint_node_id(hint_node_id x);

// hint_node_id utility functions complete