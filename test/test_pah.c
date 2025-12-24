#include<mintxengine/page_allocation_hints.h>

#include<stdio.h>
#include<inttypes.h>

int main()
{
	page_allocation_hints* pah_p = get_new_page_allocation_hints(100, "./test.db_hints", 1000, 1000);

	update_hints_for_extents(pah_p, (uint64_t[]){0,2,3,65000,95000,95001,400000,400003, UINT64_C(11564439971254894642)}, 9, (uint64_t[]){1,9,12,65002,94999,95002,400040,400055, UINT64_C(564439971254894642)}, 9);

	update_hints_for_extents(pah_p, NULL, 0, (uint64_t[]){0,1,9,12,65010,94999,95002,400040,400055, UINT64_C(564439971254894642), UINT64_C(9223372036854775805), UINT64_C(9223372036854775815), UINT64_C(11564439971254894642), UINT64_C(11564439971254894643)}, 14);

	printf("\n\n\n");

	uint64_t result[50];
	uint64_t result_size = 0;

	result_size = 20;
	find_free_extents(pah_p, 0, result, &result_size);
	for(uint64_t i = 0; i < result_size; i++)
		printf("%"PRIu64"\n", result[i]);
	printf("\n");

	result_size = 20;
	find_free_extents(pah_p, 65000, result, &result_size);
	for(uint64_t i = 0; i < result_size; i++)
		printf("%"PRIu64"\n", result[i]);
	printf("\n");

	result_size = 5;
	find_free_extents(pah_p, 95000, result, &result_size);
	for(uint64_t i = 0; i < result_size; i++)
		printf("%"PRIu64"\n", result[i]);
	printf("\n");

	result_size = 50;
	find_free_extents(pah_p, 400000, result, &result_size);
	for(uint64_t i = 0; i < result_size; i++)
		printf("%"PRIu64"\n", result[i]);
	printf("\n");

	result_size = 20;
	find_free_extents(pah_p, UINT64_C(11564439971254894640), result, &result_size);
	for(uint64_t i = 0; i < result_size; i++)
		printf("%"PRIu64"\n", result[i]);
	printf("\n");

	result_size = 20;
	find_free_extents(pah_p, UINT64_C(9223372036854775800), result, &result_size);
	for(uint64_t i = 0; i < result_size; i++)
		printf("%"PRIu64"\n", result[i]);
	printf("\n");

	flush_and_delete_page_allocation_hints(pah_p);

	return 0;
}