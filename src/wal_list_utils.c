#include<wal_list_utils.h>

#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/stat.h>
#include<sys/types.h>

#include<callbacks_wale.h>

int create_new_wal_list(mini_transaction_engine* mte)
{
	initialize_arraylist(&(mte->wa_list), 1);

	char* directory_name = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(directory_name == NULL)
		return 0;

	strcpy(directory_name, mte->database_file_name);
	strcat(directory_name, "_logs/");
	int directory_name_length = strlen(directory_name);

	struct stat s;
	int err = stat(directory_name, &s);
	if(err != -1 || errno != ENOENT)
		goto FAILURE;

	err = mkdir(directory_name, 0700);
	if(err == -1)
		goto FAILURE;

	wal_accessor* wa = malloc(sizeof(wal_accessor));
	wa->wale_LSNs_from = FIRST_LOG_SEQUENCE_NUMBER;
	char* filename = directory_name;
	sprint_uint256(filename + directory_name_length, FIRST_LOG_SEQUENCE_NUMBER);
	if(!create_and_open_block_file(&(wa->wale_block_file), filename, 0))
	{
		free(wa);
		goto FAILURE;
	}
	if(!initialize_wale(&(wa->wale_handle), mte->stats.log_sequence_number_width, FIRST_LOG_SEQUENCE_NUMBER, &(mte->global_lock), get_block_io_ops_for_wale(&(wa->wale_block_file)), mte->append_only_buffer_block_count, &err))
	{
		close_block_file(&(wa->wale_block_file));
		free(wa);
		goto FAILURE;
	}

	push_back_to_arraylist(&(mte->wa_list), wa);

	return 1;

	FAILURE :;
	close_all_in_wal_list(&(mte->wa_list));
	return 0;
}

int initialize_wal_list(mini_transaction_engine* mte);

cy_uint find_relevant_from_wal_list(arraylist* wa_list, uint256 LSN);

int drop_oldest_from_wal_list(mini_transaction_engine* mte);

void close_all_in_wal_list(arraylist* wa_list)
{
	// iterate over all and close block files
	while(!is_empty_arraylist(wa_list))
	{
		wal_accessor* wa = (wal_accessor*) get_front_of_arraylist(wa_list);
		pop_front_from_arraylist(wa_list);
		deinitialize_wale(&(wa->wale_handle));
		close_block_file(&(wa->wale_block_file));
		free(wa);
	}

	deinitialize_arraylist(wa_list);
}