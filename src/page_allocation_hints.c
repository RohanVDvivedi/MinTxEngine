#include<mintxengine/page_allocation_hints.h>

#include<cutlery/bitmap.h>

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

static inline uint64_t get_largest_managed_extent_id(const hint_node_id x)
{
	// root node always manages the complete range
	if(x.level == MAX_LEVEL)
		return UINT64_MAX;

	if(will_unsigned_sum_overflow(uint64_t, x.smallest_managed_extent_id, powers[x.level + 1]))
		return UINT64_MAX;

	return (x.smallest_managed_extent_id + powers[x.level + 1]) - 1;
}

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

	// make sure that the smallest_managed_extent_id does not overflow
	if(will_unsigned_sum_overflow(uint64_t, x.smallest_managed_extent_id, powers[x.level + 1]))
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

	// make sure that the smallest_managed_extent_id does not overflow
	if(will_unsigned_mul_overflow(uint64_t, i, powers[x.level]) ||
		will_unsigned_sum_overflow(uint64_t, x.smallest_managed_extent_id, (i * powers[x.level])))
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

// gets the child index that is responsible for the extent_id, at a node for a given level
// level must only be between 0 to 4 inclusive
static inline uint64_t get_child_index_at_level_responsible_for_extent_id(uint64_t extent_id, uint64_t level)
{
	return (extent_id / powers[level]) % PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
}

// debug function
#include<stdio.h>
static inline void print_hint_node_id(const hint_node_id x)
{
	for(int i = 0; i < (MAX_LEVEL - x.level); i++)
		printf("\t");
    printf("level = %"PRIu8", page_id = %"PRIu64", child_index = %"PRIu64", first_extent_id = %"PRIu64", last_extent_id = %"PRIu64"\n", x.level, x.page_id, x.child_index, x.smallest_managed_extent_id, get_largest_managed_extent_id(x));
}

/*
// pseudocde to show how to loop over all the pages hierarhially starting the the root page at 0 to the last possible leaf
void loop_over_all_hint_node_ids_by_page_id()
{
	hint_node_id c = get_root_page_hint_node_id();
	while(1)
	{
		// print the c
		print_hint_node_id(c);

		// below logic just helps us iterate to the next in page_id

		int error = 0;
		hint_node_id n;

		error = 0;
		n = get_ith_child_for_hint_node_id(c, 0, &error);
		if(!error)
		{
			c = n;
			continue;
		}

		error = 0;
		n = get_next_sibling_for_hint_node_id(c, &error);
		if(!error)
		{
			c = n;
			continue;
		}

		n = c;
		while(1)
		{
			error = 0;
			n = get_parent_for_hint_node_id(n, &error);
			if(error)
				break;

			error = 0;
			hint_node_id temp = get_next_sibling_for_hint_node_id(n, &error);
			if(!error)
			{
				n = temp;
				c = n;
				break;
			}
		}

		if(error)
			break;
	}
}
*/

// hint_node_id utility functions complete

// bufferpool callbacks here

static int read_hint_page(const void* page_io_ops_handle, void* frame_dest, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = (page_id * page_size) / block_size;
	size_t block_count = page_size / block_size;
	int res = read_blocks_from_block_file(((block_file*)(page_io_ops_handle)), frame_dest, block_id, block_count);
	if(!res)
		memory_set(frame_dest, 0, page_size);
	return 1;
}

static int write_hint_page(const void* page_io_ops_handle, const void* frame_src, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = (page_id * page_size) / block_size;
	size_t block_count = page_size / block_size;
	int res = write_blocks_to_block_file(((block_file*)(page_io_ops_handle)), frame_src, block_id, block_count);
	if(!res)
	{
		printf("FREE SPACE HINTS FILE WRITE IO FAILED\n");
		exit(-1);
	}
	return res;
}

static int flush_all_hint_pages(const void* page_io_ops_handle)
{
	int res = flush_all_writes_to_block_file(((block_file*)(page_io_ops_handle)));
	if(!res)
	{
		printf("FREE SPACE HINTS FILE FLUSH FAILED\n");
		exit(-1);
	}
	return res;
}

// input to the below macro is a pointer to the mini transaction engine
#define get_page_io_ops(file_handle) (page_io_ops){ \
					.page_io_ops_handle = (file_handle), \
					.page_size = PAGE_ALLOCATION_HINTS_PAGE_SIZE, \
					.page_frame_alignment = PAGE_ALLOCATION_HINTS_PAGE_SIZE, \
					.read_page = read_hint_page, \
					.write_page = write_hint_page, \
					.flush_all_writes = flush_all_hint_pages, \
				}

// can always flush to disk
static int can_hint_page_be_flushed_to_disk(void* flush_callback_handle, uint64_t page_id, const void* frame)
{
	return 1;
}

// nothing to be done if a page was flushed
static void hint_page_was_flushed_to_disk(void* flush_callback_handle, uint64_t page_id, const void* frame)
{
	return;
}

// bufferpool callbacks end

// extent free space extents_sets utility functions

typedef struct extents_set_entry extents_set_entry;
struct extents_set_entry
{
	uint64_t extent_id; // module expects extent_id to be the first attribute
	bstnode embed_node;
};

static int compare_extents_set_entry(const void* e1, const void* e2)
{
	return compare_numbers(((const extents_set_entry*)e1)->extent_id, ((const extents_set_entry*)e2)->extent_id);
}

static inline void initialize_extents_set(bst* extents_set)
{
	initialize_bst(extents_set, RED_BLACK_TREE, &simple_comparator(compare_extents_set_entry), offsetof(extents_set_entry, embed_node));
}

// fails only if extent_id already exists
static inline int insert_in_extents_set(bst* extents_set, uint64_t extent_id)
{
	// if exists fail
	if(find_equals_in_bst(extents_set, &extent_id, FIRST_OCCURENCE)) // this is doable because extent_id is the first attribute
		return 0;

	// else insert a new entry

	extents_set_entry* e = malloc(sizeof(extents_set_entry));
	if(e == NULL)
		exit(-1);
	e->extent_id = extent_id;
	initialize_bstnode(&(e->embed_node));

	insert_in_bst(extents_set, e);

	return 1;
}

// fails only if the extent to be removed does not exists
static inline int remove_from_extents_set(bst* extents_set, uint64_t extent_id)
{
	extents_set_entry* e = (extents_set_entry*)find_equals_in_bst(extents_set, &extent_id, FIRST_OCCURENCE); // this is doable because extent_id is the first attribute

	// if not exists fail
	if(e == NULL)
		return 0;

	// else remove and free it

	remove_from_bst(extents_set, e);

	free(e);

	return 1;
}

static void notify_for_remove_all(void* resource_p, const void* data_p)
{
	free((void*)data_p);
}

static inline void deinitialize_extents_set(bst* extents_set)
{
	remove_all_from_bst(extents_set, &((notifier_interface){NULL, notify_for_remove_all}));
}

typedef struct extents_set_iterator extents_set_iterator;
struct extents_set_iterator
{
	const bst* extents_set;
	const extents_set_entry* curr_entry; // if this is NULL, we are at the end
};

static extents_set_iterator get_new_extents_set_iterator(const bst* extents_set)
{
	// if extents_set is not provided, return empty, failing all further operations
	if(extents_set == NULL)
		return (extents_set_iterator){};

	return (extents_set_iterator) {
		.extents_set = extents_set,
		.curr_entry = (extents_set_entry*) find_smallest_in_bst(extents_set),
	};
}

static const uint64_t* get_curr_extent_from_extents_set_iterator(const extents_set_iterator* esi)
{
	if(esi->extents_set == NULL || esi->curr_entry == NULL)
		return NULL;

	// return the pointer to the extent_id of the current extents_set_entry
	return &(esi->curr_entry->extent_id);
}

static void go_next_in_extents_set_iterator(extents_set_iterator* esi)
{
	if(esi->extents_set == NULL || esi->curr_entry == NULL)
		return;

	// go to in-order next
	esi->curr_entry = get_inorder_next_of_in_bst(esi->extents_set, esi->curr_entry);
}

// extent free space extents_sets utility functions ended

// utility functions to update hints file in bulk 

static int get_parent_hint_bit_for_page(const void* page)
{
	// if even a single bit is not 1, i.e. has a some free extent in it's children, then return 0
	for(uint64_t i = 0; i < PAGE_ALLOCATION_HINTS_PAGE_SIZE; i++)
		if(((const char*)page)[i] != 0xff)
			return 0;

	// if all are 1 bits (all children extents are full), return 1
	return 1;
}

// returns the bit value for the parent to set in it's page for the child
static int fix_hint_bits_recursive(bufferpool* bf, hint_node_id node_id, extents_set_iterator* esi_free, extents_set_iterator* esi_full)
{
	// TODO: debug print to be removed
	print_hint_node_id(node_id);

	int was_modified = 0;
	void* page = acquire_page_with_writer_lock(bf, node_id.page_id, BLOCKING, 1, 0);

	int is_confirmed_zero_parent_bit = 0;

	uint64_t largest_managed_extent_id = get_largest_managed_extent_id(node_id);

	while(1)
	{
		extents_set_iterator* esi_select = NULL;
		if(get_curr_extent_from_extents_set_iterator(esi_free) == NULL && get_curr_extent_from_extents_set_iterator(esi_full) == NULL) // both have reached end, break out of the loop
			break;
		else if(get_curr_extent_from_extents_set_iterator(esi_free) != NULL && get_curr_extent_from_extents_set_iterator(esi_full) == NULL)
			esi_select = esi_free;
		else if(get_curr_extent_from_extents_set_iterator(esi_free) == NULL && get_curr_extent_from_extents_set_iterator(esi_full) != NULL)
			esi_select = esi_full;
		else
		{
			// if both are not null select the lower of the two's value
			if((*get_curr_extent_from_extents_set_iterator(esi_free)) < (*get_curr_extent_from_extents_set_iterator(esi_full)))
				esi_select = esi_free;
			else
				esi_select = esi_full;
		}

		// already handled case
		if(esi_select == NULL || get_curr_extent_from_extents_set_iterator(esi_select) == NULL)
			break;

		// grab the extent id in context
		uint64_t extent_id = *get_curr_extent_from_extents_set_iterator(esi_select);

		// if this extent_id is not managed by this node break out
		if(extent_id < node_id.smallest_managed_extent_id || largest_managed_extent_id < extent_id)
			break;

		// get the child_index we need to work with
		uint64_t child_index = get_child_index_at_level_responsible_for_extent_id(extent_id, node_id.level);

		if(node_id.level == 0) // if it is level 0, set/reset the corresponding bit
		{
			if(esi_select == esi_free)
			{
				// TODO: debug print to be removed
				printf("\t\t\t\t\t%"PRIu64",%"PRIu64" -> 0\n", child_index, node_id.smallest_managed_extent_id + child_index);

				reset_bit(page, child_index);
				is_confirmed_zero_parent_bit = 1; // we just did a reset on our bit, so the parent bit to be returned must be 0
				was_modified = 1;
			}
			else
			{
				// TODO: debug print to be removed
				printf("\t\t\t\t\t%"PRIu64",%"PRIu64" -> 1\n", child_index, node_id.smallest_managed_extent_id + child_index);

				set_bit(page, child_index);
				was_modified = 1;
			}

			// make the selected iterator to go next
			// only the level 0 node, can make it go next
			go_next_in_extents_set_iterator(esi_select);
		}
		else
		{
			int error = 0;
			hint_node_id child_node_id = get_ith_child_for_hint_node_id(node_id, child_index, &error);
			if(error)
				break;

			// get the bit that we should set in ourself
			int self_bit = fix_hint_bits_recursive(bf, child_node_id, esi_free, esi_full);

			// set it, or reset it
			if(self_bit)
			{
				set_bit(page, child_index);
				was_modified = 1;
			}
			else
			{
				reset_bit(page, child_index);
				is_confirmed_zero_parent_bit = 1; // we just did a reset on our bit, so the parent bit to be returned must be 0
				was_modified = 1;
			}
		}
	}

	int parent_bit;
	if(node_id.level == MAX_LEVEL) // noone cares about the parent_bit of the root node, so why not set it to -1
		parent_bit = -1;
	else if(is_confirmed_zero_parent_bit)
		parent_bit = 0;
	else
		parent_bit = get_parent_hint_bit_for_page(page);

	release_writer_lock_on_page(bf, page, was_modified, 0);

	return parent_bit;
}

static void fix_hint_bits(bufferpool* bf, const bst* set_free, const bst* set_full)
{
	extents_set_iterator esi_free = get_new_extents_set_iterator(set_free);
	extents_set_iterator esi_full = get_new_extents_set_iterator(set_full);

	fix_hint_bits_recursive(bf, get_root_page_hint_node_id(), &esi_free, &esi_full);
}

static uint64_t find_free_hint_extent_ids_recursive(bufferpool* bf, hint_node_id node_id, uint64_t from_extent_id, bst* result, uint64_t* result_count_remaining)
{
	// TODO: debug print to be removed
	print_hint_node_id(node_id);

	uint64_t free_extent_ids_captured = 0;

	if((*result_count_remaining) == 0)
		return free_extent_ids_captured;

	if(get_largest_managed_extent_id(node_id) < from_extent_id)
		return free_extent_ids_captured;

	void* page =  acquire_page_with_reader_lock(bf, node_id.page_id, BLOCKING, 1);

	uint64_t from_child_index = (from_extent_id <= node_id.smallest_managed_extent_id) ? 0 : get_child_index_at_level_responsible_for_extent_id(from_extent_id, node_id.level);

	for(uint64_t child_index = from_child_index; child_index < PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE && (*result_count_remaining) > 0; child_index++)
	{
		if(get_bit(page, child_index) == 1) // if child is full, skip it
			continue;

		if(node_id.level == 0)
		{
			// TODO: debug print to be removed
			printf("\t\t\t\t\t%"PRIu64",%"PRIu64"\n", child_index, node_id.smallest_managed_extent_id + child_index);

			free_extent_ids_captured += insert_in_extents_set(result, node_id.smallest_managed_extent_id + child_index);
			(*result_count_remaining)--;
		}
		else
		{
			int error = 0;
			hint_node_id child_node_id = get_ith_child_for_hint_node_id(node_id, child_index, &error);
			if(error)
				break;

			free_extent_ids_captured += find_free_hint_extent_ids_recursive(bf, child_node_id, from_extent_id, result, result_count_remaining);
		}
	}

	release_reader_lock_on_page(bf, page);

	return free_extent_ids_captured;
}

static uint64_t find_free_hint_extent_ids(bufferpool* bf, uint64_t from_extent_id, bst* result, uint64_t result_count_requested)
{
	if(result_count_requested == 0)
		return 0;

	return find_free_hint_extent_ids_recursive(bf, get_root_page_hint_node_id(), from_extent_id, result, &result_count_requested);
}

// utility functions to update hints file in bulk complete

page_allocation_hints* get_new_page_allocation_hints(uint64_t max_pages_to_buffer, char* extent_allocation_hints_file_path, uint64_t write_batching_capacity, uint64_t results_capacity)
{
	page_allocation_hints* pah_p = malloc(sizeof(page_allocation_hints));
	if(pah_p == NULL)
		return NULL;

	// open the block file, if not create it
	if(!open_block_file(&(pah_p->extent_allocation_hints_file), extent_allocation_hints_file_path, 0) &&
		!create_and_open_block_file(&(pah_p->extent_allocation_hints_file), extent_allocation_hints_file_path, 0))
	{
		printf("FREE SPACE HINTS FILE COULD NOT BE CREATED OR OPENNED\n");
		free(pah_p);
		return NULL;
	}

	// ensure that it has (PAGE_ALLOCATION_HINTS_PAGE_SIZE % block_size) == 0, else close it and return NULL
	if((PAGE_ALLOCATION_HINTS_PAGE_SIZE % get_block_size_for_block_file(&(pah_p->extent_allocation_hints_file))) != 0)
	{
		printf("FREE SPACE HINTS FILE COULD NOT BE USED as => (PAGE_ALLOCATION_HINTS_PAGE_SIZE %% disk.block_size = %zu) == 0\n", get_block_size_for_block_file(&(pah_p->extent_allocation_hints_file)));
		free(pah_p);
		return NULL;
	}

	// create a bufferpool
	if(!initialize_bufferpool(&(pah_p->bf), max_pages_to_buffer, NULL, get_page_io_ops((&(pah_p->extent_allocation_hints_file))), can_hint_page_be_flushed_to_disk, hint_page_was_flushed_to_disk, NULL, 60 * 1000000, max_pages_to_buffer)) // flush all frames every minute, unless there is overload
	{
		close_block_file(&(pah_p->extent_allocation_hints_file));
		free(pah_p);
		return NULL;
	}

	// initialize extents_sets
	initialize_extents_set(&(pah_p->free_extents_set));
	initialize_extents_set(&(pah_p->full_extents_set));
	initialize_extents_set(&(pah_p->results_set));

	initialize_rwlock(&(pah_p->in_mem_lock), NULL);

	pah_p->write_batching_capacity = max(results_capacity, 15);
	pah_p->write_batching_size = 0;
	pah_p->results_capacity = max(results_capacity, 15);
	pah_p->results_size = 0;

	// populate the results
	pah_p->results_size = find_free_hint_extent_ids(&(pah_p->bf), 0, &(pah_p->results_set), pah_p->results_capacity);

	return pah_p;
}

void flush_and_delete_page_allocation_hints(page_allocation_hints* pah_p)
{
	blockingly_flush_all_possible_dirty_pages(&(pah_p->bf));

	deinitialize_bufferpool(&(pah_p->bf));

	close_block_file(&(pah_p->extent_allocation_hints_file));

	deinitialize_extents_set(&(pah_p->free_extents_set));
	deinitialize_extents_set(&(pah_p->full_extents_set));
	deinitialize_extents_set(&(pah_p->results_set));

	deinitialize_rwlock(&(pah_p->in_mem_lock));

	free(pah_p);
}

void update_hints_in_page_allocation_hints(page_allocation_hints* pah_p, uint64_t extent_id, int is_full)
{
	write_lock(&(pah_p->in_mem_lock), BLOCKING);

	// update caches based on whether it is full or free
	if(is_full)
	{
		pah_p->write_batching_size += insert_in_extents_set(&(pah_p->full_extents_set), extent_id);
		pah_p->write_batching_size -= remove_from_extents_set(&(pah_p->free_extents_set), extent_id);

		// only update the results_set cache if it falls within it's bounds
		if(extent_id <= ((const extents_set_entry*)find_largest_in_bst(&(pah_p->results_set)))->extent_id)
			pah_p->results_size -= remove_from_extents_set(&(pah_p->results_set), extent_id);
	}
	else
	{
		pah_p->write_batching_size -= remove_from_extents_set(&(pah_p->full_extents_set), extent_id);
		pah_p->write_batching_size += insert_in_extents_set(&(pah_p->free_extents_set), extent_id);

		// only update the results_set cache if it falls within it's bounds
		if(extent_id <= ((const extents_set_entry*)find_largest_in_bst(&(pah_p->results_set)))->extent_id)
			pah_p->results_size += insert_in_extents_set(&(pah_p->results_set), extent_id);
	}

	// if write batches are full (OR if the results_capacity is too low), then persist them to disk
	int persisted_batch = 0;
	if(pah_p->write_batching_size >= pah_p->write_batching_capacity || pah_p->results_size < (pah_p->results_capacity * 0.75))
	{
		// persist batches to the disk
		fix_hint_bits(&(pah_p->bf), &(pah_p->free_extents_set), &(pah_p->full_extents_set));

		persisted_batch = 1;

		// reintialize all the write batches
		deinitialize_extents_set(&(pah_p->free_extents_set));
		deinitialize_extents_set(&(pah_p->full_extents_set));
		initialize_extents_set(&(pah_p->free_extents_set));
		initialize_extents_set(&(pah_p->full_extents_set));
		pah_p->write_batching_size = 0;

		// we may even need to upate the results_set cache, if it goes too low in size
		if(pah_p->results_size < (pah_p->results_capacity * 0.75))
		{
			deinitialize_extents_set(&(pah_p->results_set));
			initialize_extents_set(&(pah_p->results_set));
			pah_p->results_size = find_free_hint_extent_ids(&(pah_p->bf), 0, &(pah_p->results_set), pah_p->results_capacity);
		}
	}

	// if the results size is already too huge (huge by 25%)
	if(pah_p->results_size > (pah_p->results_capacity * 1.25))
	{
		const extents_set_entry* largest_entry = find_largest_in_bst(&(pah_p->results_set));
		while(pah_p->results_size > pah_p->results_capacity && largest_entry != NULL) // keep on going as long as there are excess elements and there is some largest entry
		{
			// find the what next largest will be
			const extents_set_entry* new_largest_entry = get_inorder_prev_of_in_bst(&(pah_p->results_set), largest_entry);

			// remove largest_entry
			remove_from_bst(&(pah_p->results_set), largest_entry);
			free(((extents_set_entry*)largest_entry));

			// make the new_one the largest one
			largest_entry = new_largest_entry;

			// decrement the result_size
			pah_p->results_size--;
		}
	}

	write_unlock(&(pah_p->in_mem_lock));

	// if we persisted to the disk, then do an async flush
	if(persisted_batch)
		trigger_flush_all_possible_dirty_pages(&(pah_p->bf));
}

uint64_t suggest_extents_from_page_allocation_hints(page_allocation_hints* pah_p, uint64_t* result_extent_ids, uint64_t result_extent_ids_capacity)
{
	read_lock(&(pah_p->in_mem_lock), READ_PREFERRING, BLOCKING);

	uint64_t result_extent_ids_size = 0;
	for(extents_set_entry* e = (extents_set_entry*) find_smallest_in_bst(&(pah_p->results_set)); e != NULL && result_extent_ids_size < result_extent_ids_capacity; e = (extents_set_entry*) get_inorder_next_of_in_bst(&(pah_p->results_set), e))
		result_extent_ids[result_extent_ids_size++] = e->extent_id;

	read_unlock(&(pah_p->in_mem_lock));

	return result_extent_ids_size;
}

void update_hints_for_extents(page_allocation_hints* pah_p, uint64_t* free_extent_ids, uint64_t free_extent_ids_count, uint64_t* full_extent_ids, uint64_t full_extent_ids_count)
{
	bst set_free;
	initialize_extents_set(&set_free);

	bst set_full;
	initialize_extents_set(&set_full);

	for(uint64_t i = 0; i < free_extent_ids_count; i++)
		insert_in_extents_set(&set_free, free_extent_ids[i]);

	for(uint64_t i = 0; i < full_extent_ids_count; i++)
		insert_in_extents_set(&set_full, full_extent_ids[i]);

	fix_hint_bits(&(pah_p->bf), &set_free, &set_full);

	deinitialize_extents_set(&set_free);
	deinitialize_extents_set(&set_full);
}

void find_free_extents(page_allocation_hints* pah_p, uint64_t from_extent_id, uint64_t* free_extent_ids, uint64_t* free_extent_ids_count)
{
	bst get_free;
	initialize_extents_set(&get_free);

	(*free_extent_ids_count) = find_free_hint_extent_ids(&(pah_p->bf), from_extent_id, &get_free, (*free_extent_ids_count));

	uint64_t i = 0;
	for(extents_set_entry* e = (extents_set_entry*) find_smallest_in_bst(&get_free); e != NULL; e = (extents_set_entry*) get_inorder_next_of_in_bst(&get_free, e))
		free_extent_ids[i++] = e->extent_id;

	deinitialize_extents_set(&get_free);
}