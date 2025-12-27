#include<mintxengine/page_allocation_hints.h>

#include<stdio.h>
#include<stdlib.h>
#include<inttypes.h>

#define MAX_EXTENT_ID 100

#define OUTER_ITERATIONS 10
#define INNER_ITERATIONS 10

#define RESULTS_SIZE 20

int main()
{
	page_allocation_hints* pah_p = get_new_page_allocation_hints(100, "./test.db_hints", 1024, 1024);

	for(int i = 0; i < OUTER_ITERATIONS; i++)
	{
		for(int j = 0; j < INNER_ITERATIONS; j++)
		{
			uint64_t extent_id = ((unsigned int)rand()) % MAX_EXTENT_ID;
			uint64_t free_pages_count_in_extent = rand() % 2;
			printf("%"PRIu64" -> %d\n", extent_id, (free_pages_count_in_extent == 0));
			update_hints_in_page_allocation_hints(pah_p, extent_id, free_pages_count_in_extent);
		}

		uint64_t free_extent_ids[RESULTS_SIZE];
		uint64_t free_extent_ids_count = suggest_extents_from_page_allocation_hints(pah_p, free_extent_ids, RESULTS_SIZE);
		for(uint64_t i = 0; i < free_extent_ids_count; i++)
			printf("%"PRIu64" ", free_extent_ids[i]);
		printf("\n\n");
	}

	flush_and_delete_page_allocation_hints(pah_p);

	return 0;
}