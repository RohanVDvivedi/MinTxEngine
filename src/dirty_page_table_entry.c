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