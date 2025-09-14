#include<mintxengine/wal_list_utils.h>

#include<stdlib.h>
#include<string.h>
#include<errno.h>
#include<sys/stat.h>
#include<sys/types.h>

#include<mintxengine/callbacks_wale.h>

int create_new_wal_list(mini_transaction_engine* mte)
{
	if(!initialize_arraylist(&(mte->wa_list), 32))
		return 0;

	char* dirname = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(dirname == NULL)
	{
		deinitialize_arraylist(&(mte->wa_list));
		return 0;
	}
	strcpy(dirname, mte->database_file_name);
	strcat(dirname, "_logs/");
	int dirname_length = strlen(dirname);

	struct stat64 s;
	int err = stat64(dirname, &s);
	if(err != -1 || errno != ENOENT)
		goto FAILURE;

	err = mkdir(dirname, 0700);
	if(err == -1)
		goto FAILURE;

	wal_accessor* wa = malloc(sizeof(wal_accessor));
	if(wa == NULL)
		goto FAILURE;
	wa->wale_LSNs_from = FIRST_LOG_SEQUENCE_NUMBER;
	char* filename = dirname;
	sprint_uint256(filename + dirname_length, FIRST_LOG_SEQUENCE_NUMBER);
	if(!create_and_open_block_file(&(wa->wale_block_file), filename, 0))
	{
		free(wa);
		goto FAILURE;
	}
	if(!initialize_wale(&(wa->wale_handle), mte->stats.log_sequence_number_width, FIRST_LOG_SEQUENCE_NUMBER, &(mte->global_lock), get_block_io_ops_for_wale(&(wa->wale_block_file)), mte->wale_append_only_buffer_block_count, &err))
	{
		close_block_file(&(wa->wale_block_file));
		free(wa);
		goto FAILURE;
	}

	push_back_to_arraylist(&(mte->wa_list), wa);

	// since it is a new databse set both flushedLSN and checkpointLSN to INVALID_LOG_SEQUENCE_NUMBER
	mte->flushedLSN = INVALID_LOG_SEQUENCE_NUMBER;
	mte->checkpointLSN = INVALID_LOG_SEQUENCE_NUMBER;

	free(dirname);
	return 1;

	FAILURE :;
	close_all_in_wal_list(&(mte->wa_list));
	free(dirname);
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
	if(!initialize_arraylist(&(mte->wa_list), 32))
		return 0;

	char* dirname = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(dirname == NULL)
	{
		deinitialize_arraylist(&(mte->wa_list));
		return 0;
	}
	strcpy(dirname, mte->database_file_name);
	strcat(dirname, "_logs/");
	int dirname_length = strlen(dirname);

	DIR* dr = opendir(dirname);
	if(dr == NULL)
		goto FAILURE;

	struct dirent64 *en;
	while ((en = readdir64(dr)) != NULL)
	{
		// skip parent and self directories
		if(0 == strcmp(en->d_name, "..") || 0 == strcmp(en->d_name, "."))
			continue;
		
		uint256 wale_LSNs_from;
		if(!if_valid_read_file_name_into_LSN(en->d_name, &wale_LSNs_from)) // there is some random file in logs directory so fail
			goto FAILURE;

		char* filename = dirname;
		strcpy(filename + dirname_length, en->d_name);

		wal_accessor* wa = malloc(sizeof(wal_accessor));
		if(wa == NULL)
			goto FAILURE;
		wa->wale_LSNs_from = wale_LSNs_from;
		if(!open_block_file(&(wa->wale_block_file), filename, 0))
		{
			free(wa);
			goto FAILURE;
		}
		if(get_total_size_for_block_file(&(wa->wale_block_file)) == 0) // if this block file is empty, delete it, a valid wale file must have master record written
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

		// fetch LSNwidth,firstLSN and nextLSN with the global lock held
		pthread_mutex_lock(&(mte->global_lock));
		uint32_t LSNwidth = get_log_sequence_number_width(&(wa->wale_handle));
		uint256 firstLSN = get_first_log_sequence_number(&(wa->wale_handle));
		uint256 nextLSN = get_next_log_sequence_number(&(wa->wale_handle));
		pthread_mutex_unlock(&(mte->global_lock));

		// check that the first log sequence numbers of wale matches the file id and the log sequence number width matches, else we skip it
		// valid condition is
		//		LSNwidth matches AND ((firstLSN is valid AND firstLSN matches filename) OR (firstLSN is invalid AND nextLSN matches filename)
		// if this condition fails, the logs directory or the files or the filenames were tampered so we quit with failure
		if(!(
			(LSNwidth == mte->stats.log_sequence_number_width)
		&& (
			(!are_equal_uint256(firstLSN, INVALID_LOG_SEQUENCE_NUMBER) && are_equal_uint256(firstLSN, wa->wale_LSNs_from))
			|| (are_equal_uint256(firstLSN, INVALID_LOG_SEQUENCE_NUMBER) && are_equal_uint256(nextLSN, wa->wale_LSNs_from)))
		))
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

	closedir(dr);
	dr = NULL;

	if(is_empty_arraylist(&(mte->wa_list)))
		goto FAILURE;

	// sort wal_list by their wale_LSNs_from
	index_accessed_interface iai = get_index_accessed_interface_for_front_of_arraylist(&(mte->wa_list));
	quick_sort_iai(&iai, 0, get_element_count_arraylist(&(mte->wa_list)) - 1, &simple_comparator(compare_wal_accessor));

	// make sure that after sorting the wale-s cover the complete range and do not have gaps
	pthread_mutex_lock(&(mte->global_lock));
	for(cy_uint i = 1; i < get_element_count_arraylist(&(mte->wa_list)); i++)
	{
		wale* prev = &(((wal_accessor*)get_from_front_of_arraylist(&(mte->wa_list), i-1))->wale_handle);
		wale* curr = &(((wal_accessor*)get_from_front_of_arraylist(&(mte->wa_list), i))->wale_handle);
		uint256 prev_next_LSN = get_next_log_sequence_number(prev);
		uint256 curr_first_LSN = get_first_log_sequence_number(curr);
		if(are_equal_uint256(curr_first_LSN, INVALID_LOG_SEQUENCE_NUMBER)) // if the curr file has no logs (for some unknown reason), then its first is equal to its next LSN
			curr_first_LSN = get_next_log_sequence_number(curr);
		if(compare_uint256(prev_next_LSN, curr_first_LSN)) // if they are different then we fail
		{
			pthread_mutex_unlock(&(mte->global_lock));
			goto FAILURE;
		}
	}
	pthread_mutex_unlock(&(mte->global_lock));

	// add append_only_buffer_count buffers to the last wale in it
	wal_accessor* wa = (wal_accessor*) get_back_of_arraylist(&(mte->wa_list));
	pthread_mutex_lock(&(mte->global_lock));
	int err = 0;
	int res = modify_append_only_buffer_block_count(&(wa->wale_handle), mte->wale_append_only_buffer_block_count, &err);
	pthread_mutex_unlock(&(mte->global_lock));
	if(!res)
		goto FAILURE;

	pthread_mutex_lock(&(mte->global_lock));

	// iterate in reverse and set the flushedLSN attribute
	mte->flushedLSN = INVALID_LOG_SEQUENCE_NUMBER;
	for(cy_uint i = 0; i < get_element_count_arraylist(&(mte->wa_list)) && are_equal_uint256(mte->flushedLSN, INVALID_LOG_SEQUENCE_NUMBER); i++)
		mte->flushedLSN = get_last_flushed_log_sequence_number(&(((wal_accessor*)get_from_back_of_arraylist(&(mte->wa_list), i))->wale_handle));

	// iterate in reverse and set the checkpointLSN attribute
	mte->checkpointLSN = INVALID_LOG_SEQUENCE_NUMBER;
	for(cy_uint i = 0; i < get_element_count_arraylist(&(mte->wa_list)) && are_equal_uint256(mte->checkpointLSN, INVALID_LOG_SEQUENCE_NUMBER); i++)
		mte->checkpointLSN = get_check_point_log_sequence_number(&(((wal_accessor*)get_from_back_of_arraylist(&(mte->wa_list), i))->wale_handle));

	pthread_mutex_unlock(&(mte->global_lock));

	free(dirname);
	return 1;

	FAILURE :;
	if(dr != NULL)
		closedir(dr);
	close_all_in_wal_list(&(mte->wa_list));
	free(dirname);
	return 0;
}

cy_uint find_relevant_from_wal_list_UNSAFE(arraylist* wa_list, uint256 LSN)
{
	if(is_empty_arraylist(wa_list))
		return INVALID_INDEX;
	index_accessed_interface iai = get_index_accessed_interface_for_front_of_arraylist(wa_list);
	return find_preceding_or_equals_in_sorted_iai(&iai, 0, get_element_count_arraylist(wa_list) - 1, &(wal_accessor){.wale_LSNs_from = LSN}, &simple_comparator(compare_wal_accessor));
}

int drop_oldest_from_wal_list_UNSAFE(mini_transaction_engine* mte)
{
	if(is_empty_arraylist(&(mte->wa_list))) // can not drop wal file from empty wa_list
		return 0;

	char* filename = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(filename == NULL)
	{
		printf("ISSUE :: failure to allocate memory for filename\n");
		exit(-1);
	}

	wal_accessor* wa = (wal_accessor*) get_front_of_arraylist(&(mte->wa_list));
	pop_front_from_arraylist(&(mte->wa_list));
	uint256 file_id = wa->wale_LSNs_from;
	deinitialize_wale(&(wa->wale_handle));
	close_block_file(&(wa->wale_block_file));
	free(wa);

	strcpy(filename, mte->database_file_name);
	strcat(filename, "_logs/");
	int dirname_length = strlen(filename);
	sprint_uint256(filename + dirname_length, file_id);
	remove(filename);
	free(filename);
	return 1;
}

int create_newest_in_wal_list_UNSAFE(mini_transaction_engine* mte)
{
	if(is_empty_arraylist(&(mte->wa_list))) // can not create wal file from empty wa_list
		return 0;

	int err = 0;
	uint256 file_id = INVALID_LOG_SEQUENCE_NUMBER;

	{
		wal_accessor* curr_wa = (wal_accessor*) get_back_of_arraylist(&(mte->wa_list));

		file_id = get_next_log_sequence_number(&(curr_wa->wale_handle));

		// turn off writing to curr_wa
		if(!modify_append_only_buffer_block_count(&(curr_wa->wale_handle), 0, &err))
		{
			printf("ISSUE :: failure to turn off reading in the current writable wale, (to be dine in attemot to create a new one)\n");
			exit(-1);
		}
	}

	char* filename = malloc(strlen(mte->database_file_name) + 20 + 64);
	if(filename == NULL)
	{
		printf("ISSUE :: failure to allocate memory for filename\n");
		exit(-1);
	}
	strcpy(filename, mte->database_file_name);
	strcat(filename, "_logs/");
	int dirname_length = strlen(filename);
	sprint_uint256(filename + dirname_length, file_id);

	wal_accessor* wa = malloc(sizeof(wal_accessor));
	if(wa == NULL)
	{
		printf("ISSUE :: unable to allocate memory for a new wal file\n");
		exit(-1);
	}
	wa->wale_LSNs_from = file_id;
	if(!create_and_open_block_file(&(wa->wale_block_file), filename, 0))
	{
		printf("ISSUE :: unable to create a new block file for new wal_accessor\n");
		exit(-1);
	}
	if(!initialize_wale(&(wa->wale_handle), mte->stats.log_sequence_number_width, wa->wale_LSNs_from, &(mte->global_lock), get_block_io_ops_for_wale(&(wa->wale_block_file)), mte->wale_append_only_buffer_block_count, &err))
	{
		printf("ISSUE :: unable to create a new wale for new wal_accessor\n");
		exit(-1);
	}

	if(is_full_arraylist(&(mte->wa_list)))
		expand_arraylist(&(mte->wa_list));
	if(!push_back_to_arraylist(&(mte->wa_list), wa))
	{
		printf("ISSUE :: unable to push new wal_accessor to wa_list in mini transaction engine\n");
		exit(-1);
	}

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