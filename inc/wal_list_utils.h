#ifndef WAL_LIST_UTILS_H
#define WAL_LIST_UTILS_H

#include<mini_transaction_engine.h>

// first LSN of any file is 7
#define FIRST_LOG_SEQUENCE_NUMBER get_uint256(7)

// create a new wal directory at (mte->database_file_name + "_logs/") and a new first log.7 (almost empty) wal file
// it will fail if this directory already exists
void create_new_wal_list(arraylist* wa_list, const mini_transaction_engine* mte);

// fails if the database_file_name + "/logs/" directory does not exists OR is empty
void initialize_wal_list(arraylist* wa_list, const mini_transaction_engine* mte);

// returns index of the wal_accessor from the wal_list that may include LSN
cy_uint find_relevant_from_wal_list(arraylist* wa_list, uint256 LSN);

// deletes the oldest wale file present in the wa_list
int drop_oldest_from_wal_list(arraylist* wa_list);

// close all from the wal_list
void close_all_from_wal_list(arraylist* wa_list);

#endif