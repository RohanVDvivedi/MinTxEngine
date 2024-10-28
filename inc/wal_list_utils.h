#ifndef WAL_LIST_UTILS_H
#define WAL_LIST_UTILS_H

#include<mini_transaction_engine.h>

// first LSN of any file is 7
#define FIRST_LOG_SEQUENCE_NUMBER get_uint256(7)

// create a new wal directory at (mte->database_file_name + "_logs/") and a new first log.7 (almost empty) wal file
// it will fail if this directory already exists
// it will also initialize flushedLSN and checkpointLSN attributes of mte, on success
int create_new_wal_list(mini_transaction_engine* mte);

// fails if the database_file_name + "/logs/" directory does not exists OR is empty
// it will also fail if any of the wal file is not named according to their first LSNs
// of if their log_sequence_number_widths do not match with the one in the stats of mte
// it will also initialize flushedLSN and checkpointLSN attributes of mte, on success
int initialize_wal_list(mini_transaction_engine* mte);

// returns index of the wal_accessor from the wal_list that may include LSN
cy_uint find_relevant_from_wal_list_UNSAFE(arraylist* wa_list, uint256 LSN);

// deletes the oldest wale file present in the wa_list
int drop_oldest_from_wal_list_UNSAFE(mini_transaction_engine* mte);

// close all from the wal_list
void close_all_in_wal_list(arraylist* wa_list);

#endif