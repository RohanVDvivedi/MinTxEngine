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
	{
		deinitialize_arraylist(&(mte->wa_list));
		return 0;
	}
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
	if(wa == NULL)
		goto FAILURE;
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

	free(directory_name);
	return 1;

	FAILURE :;
	close_all_in_wal_list(&(mte->wa_list));
	free(directory_name);
	return 0;
}

static int compare_wal_accessor(const void* a, const void* b)
{
	return compare_uint256(((const wal_accessor*)a)->wale_LSNs_from, ((const wal_accessor*)b)->wale_LSNs_from);
}

static int if_valid_read_file_name_into_LSN(const char* base_filename, uint256* wale_LSNs_from)
{
	if(strlen(base_filename) != 64)
		return 0;

	(*wale_LSNs_from) = get_0_uint256();
	for(int i = 0; i < 64; i++)
	{
		unsigned int digit = get_digit_from_char(base_filename[i], 16);
		if(digit == -1)
			return 0;
		(*wale_LSNs_from) = left_shift_uint256((*wale_LSNs_from), 4);
		(*wale_LSNs_from) = bitwise_or_uint256((*wale_LSNs_from), get_uint256(digit));
	}

	return 1;
}

#include<dirent.h>

int initialize_wal_list(mini_transaction_engine* mte)
{
	initialize_arraylist(&(mte->wa_list), 32);

	char* directory_name = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(directory_name == NULL)
	{
		deinitialize_arraylist(&(mte->wa_list));
		return 0;
	}
	strcpy(directory_name, mte->database_file_name);
	strcat(directory_name, "_logs/");
	int directory_name_length = strlen(directory_name);

	DIR* dr = opendir(directory_name);
	if(dr == NULL)
	{
		deinitialize_arraylist(&(mte->wa_list));
		free(directory_name);
		return 0;
	}

	struct dirent *en;
	while ((en = readdir(dr)) != NULL) {
		
		uint256 wale_LSNs_from;
		if(!if_valid_read_file_name_into_LSN(en->d_name, &wale_LSNs_from))
			goto FAILURE;

		char* filename = directory_name;
		strcpy(filename + directory_name_length, en->d_name);

		wal_accessor* wa = malloc(sizeof(wal_accessor));
		if(wa == NULL)
			goto FAILURE;
		wa->wale_LSNs_from = wale_LSNs_from;
		if(!open_block_file(&(wa->wale_block_file), filename, 0))
		{
			free(wa);
			goto FAILURE;
		}
		if(get_total_size_for_block_file(&(wa->wale_block_file)) == 0) // if this block file is empty, delete it, a valid wale file must have mster record written
		{
			close_block_file(&(wa->wale_block_file));
			free(wa);
			remove(filename);
			continue;
		}
		int err;
		if(!initialize_wale(&(wa->wale_handle), mte->stats.log_sequence_number_width, INVALID_LOG_SEQUENCE_NUMBER, &(mte->global_lock), get_block_io_ops_for_wale(&(wa->wale_block_file)), 0, &err))
		{
			close_block_file(&(wa->wale_block_file));
			free(wa);
			goto FAILURE;
		}

		// fetch firstLSN and nextLSN with the globa lock held
		pthread_mutex_lock(&(mte->global_lock));
		uint32_t LSNwidth = get_log_sequence_number_width(&(wa->wale_handle));
		uint256 firstLSN = get_first_log_sequence_number(&(wa->wale_handle));
		uint256 nextLSN = get_next_log_sequence_number(&(wa->wale_handle));
		pthread_mutex_unlock(&(mte->global_lock));

		// check that the first log sequence numbers of wale matches the file id and the log sequence number width matches, else we skip it
		if((LSNwidth != mte->stats.log_sequence_number_width)
		|| (!are_equal_uint256(firstLSN, INVALID_LOG_SEQUENCE_NUMBER) && !are_equal_uint256(firstLSN, wa->wale_LSNs_from))
		|| (are_equal_uint256(firstLSN, INVALID_LOG_SEQUENCE_NUMBER) && !are_equal_uint256(nextLSN, wa->wale_LSNs_from))
		)
		{
			deinitialize_wale(&(wa->wale_handle));
			close_block_file(&(wa->wale_block_file));
			free(wa);
			goto FAILURE;
		}

		if(is_full_arraylist(&(mte->wa_list)))
			expand_arraylist(&(mte->wa_list));
		if(!push_back_to_arraylist(&(mte->wa_list), wa))
		{
			deinitialize_wale(&(wa->wale_handle));
			close_block_file(&(wa->wale_block_file));
			free(wa);
			goto FAILURE;
		}
	}

	if(!is_empty_arraylist(&(mte->wa_list)))
		goto FAILURE;

	// sort wal_list by their wale_LSNs_from
	index_accessed_interface iai = get_index_accessed_interface_for_front_of_arraylist(&(mte->wa_list));
	quick_sort_iai(&iai, 0, get_element_count_arraylist(&(mte->wa_list)) - 1, &simple_comparator(compare_wal_accessor));

	// add append_only_buffer_count buffers to the last wale in it
	wal_accessor* wa = (wal_accessor*) get_back_of_arraylist(&(mte->wa_list));
	pthread_mutex_lock(&(mte->global_lock));
	int err = 0;
	int res = modify_append_only_buffer_block_count(&(wa->wale_handle), mte->append_only_buffer_block_count, &err);
	pthread_mutex_unlock(&(mte->global_lock));
	if(!res)
		goto FAILURE;

	closedir(dr);
	free(directory_name);
	return 1;

	FAILURE :;
	closedir(dr);
	close_all_in_wal_list(&(mte->wa_list));
	free(directory_name);
	return 0;
}

cy_uint find_relevant_from_wal_list(arraylist* wa_list, uint256 LSN)
{
	if(is_empty_arraylist(wa_list))
		return INVALID_INDEX;
	index_accessed_interface iai = get_index_accessed_interface_for_front_of_arraylist(wa_list);
	return find_preceding_or_equals_in_sorted_iai(&iai, 0, get_element_count_arraylist(wa_list) - 1, &(wal_accessor){.wale_LSNs_from = LSN}, &simple_comparator(compare_wal_accessor));
}

int drop_oldest_from_wal_list(mini_transaction_engine* mte)
{
	if(!is_empty_arraylist(&(mte->wa_list)))
		return 0;

	char* filename = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(filename == NULL)
		return 0;

	wal_accessor* wa = (wal_accessor*) get_front_of_arraylist(&(mte->wa_list));
	pop_front_from_arraylist(&(mte->wa_list));
	uint256 file_id = wa->wale_LSNs_from;
	deinitialize_wale(&(wa->wale_handle));
	close_block_file(&(wa->wale_block_file));
	free(wa);

	strcpy(filename, mte->database_file_name);
	strcat(filename, "_logs/");
	int directory_name_length = strlen(filename);
	sprint_uint256(filename + directory_name_length, file_id);
	remove(filename);
	free(filename);
	return 1;
}

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