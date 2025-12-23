#include<mintxengine/page_allocation_hints.h>

int main()
{
	page_allocation_hints* pah_p = get_new_page_allocation_hints("./test.db_hints");

	update_hints_for_extents(pah_p, (uint64_t[]){0,2,3,65000,95000,95001,400000,400003, 11564439971254894642}, 9, (uint64_t[]){1,9,12,65002,94999,95002,400040,400055, 21564439971254894642}, 9);

	flush_and_delete_page_allocation_hints(pah_p);
	return 0;
}