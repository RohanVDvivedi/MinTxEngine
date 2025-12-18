#include<mintxengine/page_allocation_hints.h>

#include<stdint.h>

// each hint page node, in the free space hint hierarchy of the tree is identified by the hint_node_id
// below are the utility functions for managing the hint_node_ids

typedef struct hint_node_id hint_node_id;
struct hint_node_id
{
	uint8_t level; // values from 4 to 0, both inclusive

	uint64_t page_id; // page_id of the hint_page we are working with, each page is 4096 bytes big

	uint64_t child_index; // this is the index of this node in it's immediate parent's subtree

	// this is the smallest extent_id that this hint_node manages
	uint64_t smallest_managed_extent_id;

	// the largest extent_id managed by this hint_node = (smallest_managed_extent_id + (PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE ^ (level + 1)) - 1)
};

static inline hint_node_id get_root_page_hint_node_id()
{
	return (hint_node_id) {
		.level = 4,				// root page is at level 4 and page_id = 0
		.page_id = 0,
		.child_index = 0,		//and it is at 0th index in its non existent parent
		.smallest_managed_extent_id = 0,
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

	#define pow_bits get_power_for_bits_count_on_the_node_page

	// only 0 to 4 values do not overflow
	switch(p)
	{
		case 0 :
			return pow_bits(0, overflowed);
		case 1 :
			return pow_bits(0, overflowed) + pow_bits(1, overflowed);
		case 2 :
			return pow_bits(0, overflowed) + pow_bits(1, overflowed) + pow_bits(2, overflowed);
		case 3 :
			return pow_bits(0, overflowed) + pow_bits(1, overflowed) + pow_bits(2, overflowed) + pow_bits(3, overflowed);
		case 4 :
			return pow_bits(0, overflowed) + pow_bits(1, overflowed) + pow_bits(2, overflowed) + pow_bits(3, overflowed) + pow_bits(4, overflowed);
	}

	#undef get_power_for_bits_count_on_the_node_page

	// 5 and onwards it overflows
	(*overflowed) = 1;
	return 0;
}

static inline hint_node_id get_next_sibling_for_hint_node_id(hint_node_id x, int* error)
{
	// last child can not have a next sibling
	if(x.child_index == (PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE-1))
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level,
		.page_id = x.page_id + get_sum_of_powers_for_bits_count_on_the_node_page(x.level, error),
		.child_index = x.child_index + 1,
		.smallest_managed_extent_id = x.smallest_managed_extent_id + get_power_for_bits_count_on_the_node_page(x.level + 1, error),
	};
}

static inline hint_node_id get_prev_sibling_for_hint_node_id(hint_node_id x, int* error)
{
	// 0th child can not have a prev sibling
	if(x.child_index == 0)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level,
		.page_id = x.page_id - get_sum_of_powers_for_bits_count_on_the_node_page(x.level, error),
		.child_index = x.child_index - 1,
		.smallest_managed_extent_id = x.smallest_managed_extent_id - get_power_for_bits_count_on_the_node_page(x.level + 1, error),
	};
}

static inline hint_node_id get_ith_child_for_hint_node_id(hint_node_id x, uint64_t i, int* error)
{
	// level 0 can not have a child
	if(x.level == 0)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level - 1,
		.page_id = x.page_id + 1 + i * get_sum_of_powers_for_bits_count_on_the_node_page(x.level - 1, error),
		.child_index = i,
		.smallest_managed_extent_id = x.smallest_managed_extent_id + i * get_power_for_bits_count_on_the_node_page(x.level, error),
	};
}

static inline hint_node_id get_parent_for_hint_node_id(hint_node_id x, int* error)
{
	// level 5 can not have a parent
	if(x.level == 5)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level + 1,
		.page_id = x.page_id - x.child_index * get_sum_of_powers_for_bits_count_on_the_node_page(x.level, error) - 1,
		.child_index = (x.smallest_managed_extent_id / get_power_for_bits_count_on_the_node_page(x.level + 1, error)) % PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE, // the parent will also be relevant for the x.smallest_managed_extent_id, though it will not be its smallest_managed_extent_id
		.smallest_managed_extent_id = x.smallest_managed_extent_id - i * get_power_for_bits_count_on_the_node_page(x.level + 1, error),
	};
}

// hint_node_id utility functions complete