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

// here the indices by level array must be atleast 5 uint64_t's long, only indices corresponding to levels 0 to 4 (both inclusive are used)
static inline void get_child_indices_by_level_responsible_for_extent_id(uint64_t extent_id, uint64_t* indices_by_level)
{
	for(uint8_t level = 0; level < 5; level++)
		indices_by_level[level] = (extent_id / powers[level]) % PAGE_ALLOCATION_HINTS_BITS_COUNT_PER_NODE;
}

// hint_node_id utility functions complete

// bufferpool callbacks here

static int read_hint_page(const void* page_io_ops_handle, void* frame_dest, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = (page_id * page_size) / block_size;
	size_t block_count = page_size / block_size;
	return read_blocks_from_block_file(((block_file*)(page_io_ops_handle)), frame_dest, block_id, block_count);
}

static int write_hint_page(const void* page_io_ops_handle, const void* frame_src, uint64_t page_id, uint32_t page_size)
{
	size_t block_size = get_block_size_for_block_file(((block_file*)(page_io_ops_handle)));
	off_t block_id = (page_id * page_size) / block_size;
	size_t block_count = page_size / block_size;
	return write_blocks_to_block_file(((block_file*)(page_io_ops_handle)), frame_src, block_id, block_count);
}

static int flush_all_hint_pages(const void* page_io_ops_handle)
{
	return flush_all_writes_to_block_file(((block_file*)(page_io_ops_handle)));
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

// extent free space caches utility functions

typedef struct cache_entry cache_entry;
struct cache_entry
{
	uint64_t extent_id; // module expects extent_id to be the first attribute
	bstnode embed_node;
};

static int compare_cache_entry(const void* e1, const void* e2)
{
	return compare_numbers(((const cache_entry*)e1)->extent_id, ((const cache_entry*)e2)->extent_id);
}

static inline void initialize_cache(bst* cache)
{
	initialize_bst(cache, RED_BLACK_TREE, &simple_comparator(compare_cache_entry), offsetof(cache_entry, embed_node));
}

static inline void insert_in_cache(bst* cache, uint64_t extent_id)
{
	// if exists fail
	if(find_equals_in_bst(cache, &extent_id, FIRST_OCCURENCE)) // this is doable because extent_id is the first attribute
		return;

	// else insert a new entry

	cache_entry* e = malloc(sizeof(cache_entry));
	e->extent_id = extent_id;
	initialize_bstnode(&(e->embed_node));

	insert_in_bst(cache, e);
}

static inline void remove_from_cache(bst* cache, uint64_t extent_id)
{
	cache_entry* e = (cache_entry*)find_equals_in_bst(cache, &extent_id, FIRST_OCCURENCE); // this is doable because extent_id is the first attribute

	// if not exists fail
	if(e == NULL)
		return;

	// else remove and free it

	remove_from_bst(cache, e);

	free(e);
}

static void notify_for_remove_all(void* resource_p, const void* data_p)
{
	free((void*)data_p);
}

static inline void deinitialize_cache(bst* cache)
{
	remove_all_from_bst(cache, &((notifier_interface){NULL, notify_for_remove_all}));
}

// extent free space caches utility functions ended

page_allocation_hints* get_new_page_allocation_hints(char* extent_allocation_hints_file_path)
{
	page_allocation_hints* pah_p = malloc(sizeof(page_allocation_hints));

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
	if(!initialize_bufferpool(&(pah_p->bf), HINTS_FRAMES_TO_CACHE, NULL, get_page_io_ops((&(pah_p->extent_allocation_hints_file))), can_hint_page_be_flushed_to_disk, hint_page_was_flushed_to_disk, NULL, 60 * 1000000, HINTS_FRAMES_TO_CACHE)) // flush all frames every minute, unless there is overload
	{
		close_block_file(&(pah_p->extent_allocation_hints_file));
		free(pah_p);
		return NULL;
	}

	// initialize caches
	initialize_cache(&(pah_p->free_cache));
	initialize_cache(&(pah_p->full_cache));

	return pah_p;
}

void flush_and_delete_page_allocation_hints(page_allocation_hints* pah_p)
{
	blockingly_flush_all_possible_dirty_pages(&(pah_p->bf));

	deinitialize_bufferpool(&(pah_p->bf));

	close_block_file(&(pah_p->extent_allocation_hints_file));

	deinitialize_cache(&(pah_p->free_cache));
	deinitialize_cache(&(pah_p->full_cache));

	free(pah_p);
}