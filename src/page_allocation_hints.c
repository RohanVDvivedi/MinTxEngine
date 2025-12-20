#include<mintxengine/page_allocation_hints.h>

#include<stdint.h>
#include<inttypes.h>

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
		.level = MAX_LEVEL,		// root page is at level MAX_LEVEL and page_id = 0
		.page_id = 0,
		.child_index = 0,		//and it is at 0th index in its non existent parent
		.smallest_managed_extent_id = 0,
	};
}

// returns PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE ^ p
// powers[L] = number of extents that a bit in a hint page with level L porvides hints for
static const uint64_t powers[5] = {
	[0] = UINT64_C(1),
	[1] = PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE,
	[2] = PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE,
	[3] = PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE,
	[4] = PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE * PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE,
};

// returns summation (from 0 to p, both inclusive) for PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE ^ i
// subtree_sizes[L] = number of pages in the subtree rooted at a page of level L
static const uint64_t subtree_sizes[5] = {
	[0] = powers[0],
	[1] = powers[0] + powers[1],
	[2] = powers[0] + powers[1] + powers[2],
	[3] = powers[0] + powers[1] + powers[2] + powers[3],
	[4] = powers[0] + powers[1] + powers[2] + powers[3] + powers[4],
};

static inline hint_node_id get_next_sibling_for_hint_node_id(const hint_node_id x, int* error)
{
	// root page does not have any siblings
	if(x.level == MAX_LEVEL)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	// last child can not have a next sibling
	if(x.child_index == (PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE-1))
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level,
		.page_id = x.page_id + subtree_sizes[x.level],
		.child_index = x.child_index + 1,
		.smallest_managed_extent_id = x.smallest_managed_extent_id + powers[x.level + 1],
	};
}

static inline hint_node_id get_prev_sibling_for_hint_node_id(const hint_node_id x, int* error)
{
	// root page does not have any siblings
	if(x.level == MAX_LEVEL)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	// 0th child can not have a prev sibling
	if(x.child_index == 0)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level,
		.page_id = x.page_id - subtree_sizes[x.level],
		.child_index = x.child_index - 1,
		.smallest_managed_extent_id = x.smallest_managed_extent_id - powers[x.level + 1],
	};
}

static inline hint_node_id get_ith_child_for_hint_node_id(const hint_node_id x, uint64_t i, int* error)
{
	// level 0 can not have a child
	if(x.level == 0)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level - 1,
		.page_id = x.page_id + 1 + i * subtree_sizes[x.level - 1],
		.child_index = i,
		.smallest_managed_extent_id = x.smallest_managed_extent_id + i * powers[x.level],
	};
}

static inline hint_node_id get_parent_for_hint_node_id(const hint_node_id x, int* error)
{
	// node at MAX_LEVEL can not have a parent
	if(x.level == MAX_LEVEL)
	{
		(*error) = 1;
		return (hint_node_id){};
	}

	return (hint_node_id) {
		.level = x.level + 1,
		.page_id = x.page_id - x.child_index * subtree_sizes[x.level] - 1,
		.child_index = ((x.level + 1) == MAX_LEVEL) ? 0 : (x.smallest_managed_extent_id / powers[x.level + 2]) % PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE, // the parent will also be relevant for the x.smallest_managed_extent_id, though it will not be its smallest_managed_extent_id
		// we only need the child index of this new x.level+1 guy in it's parent
		// if the x.level of the new page is the MAX_LEVEL, then it is root page and will always be the 0th child of it's parent
		.smallest_managed_extent_id = x.smallest_managed_extent_id - x.child_index * powers[x.level + 1],
	};
}

// debug function
#include<stdio.h>
static inline void print_hint_node_id(const hint_node_id x)
{
    printf("level = %"PRIu8", page_id = %"PRIu64", child_index = %"PRIu64", first_extent_id = %"PRIu64"\n", x.level, x.page_id, x.child_index, x.smallest_managed_extent_id);
}

// here the indices by level array must be atleast 5 uint64_t's long, only indices corresponding to levels 0 to 4 (both inclusive are used)
static inline void get_child_indices_by_level_responsible_for_extent_id(uint64_t extent_id, uint64_t* indices_by_level)
{
	for(uint8_t level = 0; level < 5; level++)
		indices_by_level[level] = (extent_id / powers[level]) % PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
}

// hint_node_id utility functions complete
