#ifndef DIRTY_PAGE_TABLE_ENTRY_H
#define DIRTY_PAGE_TABLE_ENTRY_H

typedef struct dirty_page_table_entry dirty_page_table_entry;
struct dirty_page_table_entry
{
	uint64_t page_id; // page_id of the page that is dirty
	uint256 recLSN; // the oldest LSN that made this page dirty, also called recoveryLSN -> you need to start redoing from this LSN to reach latest state of this page

	// embedded node to manage the entry
	llnode enode;
};

#endif