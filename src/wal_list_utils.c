#include<wal_list_utils.h>

int create_new_wal_list(mini_transaction_engine* mte);

int initialize_wal_list(mini_transaction_engine* mte);

cy_uint find_relevant_from_wal_list(arraylist* wa_list, uint256 LSN);

int drop_oldest_from_wal_list(arraylist* wa_list);

void close_all_from_wal_list(arraylist* wa_list);