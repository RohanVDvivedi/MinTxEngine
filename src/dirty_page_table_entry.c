#include<dirty_page_table_entry.h>

int compare_dirty_page_table_entries(const void* dpte1, const void* dpte2)
{
	return compare_numbers(((const dirty_page_table_entry*)dpte1)->page_id, ((const dirty_page_table_entry*)dpte2)->page_id);
}

cy_uint hash_dirty_page_table_entry(const void* dpte)
{
	return ((const dirty_page_table_entry*)dpte)->page_id;
}

#include<stdlib.h>

dirty_page_table_entry* get_new_dirty_page_table_entry()
{
	dirty_page_table_entry* dpte = malloc(sizeof(dirty_page_table_entry));
	if(dpte == NULL)
	{
		printf("ISSUE :: unable to allocate memory for dirty page table entry\n");
		exit(-1);
	}
	initialize_llnode(&(dpte->enode));
	return dpte;
}

void delete_dirty_page_table_entry(dirty_page_table_entry* dpte)
{
	free(dpte);
}

#include<wale.h>

uint256 get_minimum_recLSN_for_dirty_page_table(const hashmap* dirty_page_table)
{
	uint256 min_recLSN = INVALID_LOG_SEQUENCE_NUMBER;

	for(const dirty_page_table_entry* dpte = get_first_of_in_hashmap(dirty_page_table, FIRST_OF_HASHMAP); dpte != NULL; dpte = get_next_of_in_hashmap(dirty_page_table, dpte, ANY_IN_HASHMAP))
	{
		if(are_equal_uint256(min_recLSN, INVALID_LOG_SEQUENCE_NUMBER))
			min_recLSN = dpte->recLSN;
		else
			min_recLSN = min_uint256(min_recLSN, dpte->recLSN);
	}

	return min_recLSN;
}

void delete_dirty_page_table_entry_notify(void* resource_p, const void* data_p)
{
	delete_dirty_page_table_entry((dirty_page_table_entry*) data_p);
}