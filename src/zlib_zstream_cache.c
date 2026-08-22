#include<mintxengine/zlib_zstream_cache.h>

#include<posixutils/timespec_utils.h>

#include<stdlib.h>
#include<stdio.h>

void init_zstream_cache(zstream_cache* zstrm_cash)
{
	pthread_mutex_init(&(zstrm_cash->zstream_cache_lock), NULL);
	zstrm_cash->inflate_zstreams.zstreams_count = 0;
	initialize_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list), offsetof(zstream_wrapper, in_use_cache_node));
	zstrm_cash->deflate_zstreams.zstreams_count = 0;
	initialize_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list), offsetof(zstream_wrapper, in_use_cache_node));
}

zstream_wrapper* get_inflate_zstream_object(zstream_cache* zstrm_cash)
{
	// take lock and check in cache
	pthread_mutex_lock(&(zstrm_cash->zstream_cache_lock));

	zstream_wrapper* zstrm_wrap = (zstream_wrapper*) get_head_of_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list));
	if(zstrm_wrap != NULL)
	{
		zstrm_cash->inflate_zstreams.zstreams_count--;
		remove_head_from_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list));
	}

	pthread_mutex_unlock(&(zstrm_cash->zstream_cache_lock));

	// if cache had one return it
	if(zstrm_wrap != NULL)
		return zstrm_wrap;

	// else create one and return it instead
	zstrm_wrap = malloc(sizeof(zstream_wrapper));
	zstrm_wrap->zstrm.zalloc = Z_NULL;
	zstrm_wrap->zstrm.zfree = Z_NULL;
	zstrm_wrap->zstrm.opaque = Z_NULL;
	initialize_llnode(&(zstrm_wrap->in_use_cache_node));
	zstrm_wrap->last_used_in_microseconds = 0; // invalid on first creation

	// init the state
	if(Z_OK != inflateInit(&(zstrm_wrap->zstrm)))
	{
		printf("ISSUE :: failure to initialize zlib uncompression stream for uncompressing log record\n");
		exit(-1);
	}

	return zstrm_wrap;
}

zstream_wrapper* get_deflate_zstream_object(zstream_cache* zstrm_cash)
{
	// take lock and check in cache
	pthread_mutex_lock(&(zstrm_cash->zstream_cache_lock));

	zstream_wrapper* zstrm_wrap = (zstream_wrapper*) get_head_of_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list));
	if(zstrm_wrap != NULL)
	{
		zstrm_cash->deflate_zstreams.zstreams_count--;
		remove_head_from_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list));
	}

	pthread_mutex_unlock(&(zstrm_cash->zstream_cache_lock));

	// if cache had one return it
	if(zstrm_wrap != NULL)
		return zstrm_wrap;

	// else create one and return it instead
	zstrm_wrap = malloc(sizeof(zstream_wrapper));
	zstrm_wrap->zstrm.zalloc = Z_NULL;
	zstrm_wrap->zstrm.zfree = Z_NULL;
	zstrm_wrap->zstrm.opaque = Z_NULL;
	initialize_llnode(&(zstrm_wrap->in_use_cache_node));
	zstrm_wrap->last_used_in_microseconds = 0; // invalid on first creation

	if(Z_OK != deflateInit(&(zstrm_wrap->zstrm), MINTXENGINE_ZLIB_COMPRESSION_LEVEL))
	{
		printf("ISSUE :: failure to initialize zlib compression stream for compressing log record\n");
		exit(-1);
	}

	return zstrm_wrap;
}

void give_back_inflate_zstream_object(zstream_cache* zstrm_cash, zstream_wrapper* zstrm_wrap)
{
	uint64_t curr_time_in_microseconds;
	{
		struct timespec curr_time;
		clock_gettime(CLOCK_MONOTONIC, &curr_time);
		curr_time_in_microseconds = timespec_to_microseconds(curr_time);
	}

	zstrm_wrap->last_used_in_microseconds = curr_time_in_microseconds;

	// take lock and insert it in cache
	pthread_mutex_lock(&(zstrm_cash->zstream_cache_lock));

	insert_head_in_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list), zstrm_wrap);

	while(!is_empty_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list)))
	{
		zstream_wrapper* zstrm_wrap = (zstream_wrapper*) get_tail_of_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list));
		if(curr_time_in_microseconds - zstrm_wrap->last_used_in_microseconds < ZSTREAM_LIFE_IN_MICROSECONDS)
			break;

		remove_tail_from_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list));

		inflateEnd(&(zstrm_wrap->zstrm));
		free(zstrm_wrap);
	}

	pthread_mutex_unlock(&(zstrm_cash->zstream_cache_lock));
}

void give_back_deflate_zstream_object(zstream_cache* zstrm_cash, zstream_wrapper* zstrm_wrap)
{
	uint64_t curr_time_in_microseconds;
	{
		struct timespec curr_time;
		clock_gettime(CLOCK_MONOTONIC, &curr_time);
		curr_time_in_microseconds = timespec_to_microseconds(curr_time);
	}

	zstrm_wrap->last_used_in_microseconds = curr_time_in_microseconds;

	// take lock and insert it in cache
	pthread_mutex_lock(&(zstrm_cash->zstream_cache_lock));

	insert_head_in_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list), zstrm_wrap);

	while(!is_empty_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list)))
	{
		zstream_wrapper* zstrm_wrap = (zstream_wrapper*) get_tail_of_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list));
		if(curr_time_in_microseconds - zstrm_wrap->last_used_in_microseconds < ZSTREAM_LIFE_IN_MICROSECONDS)
			break;

		remove_tail_from_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list));

		deflateEnd(&(zstrm_wrap->zstrm));
		free(zstrm_wrap);
	}

	pthread_mutex_unlock(&(zstrm_cash->zstream_cache_lock));
}

void deinit_zstream_cache(zstream_cache* zstrm_cash)
{
	pthread_mutex_destroy(&(zstrm_cash->zstream_cache_lock));

	while(!is_empty_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list)))
	{
		zstream_wrapper* zstrm_wrap = (zstream_wrapper*) get_head_of_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list));

		remove_head_from_linkedlist(&(zstrm_cash->inflate_zstreams.zstreams_list));

		inflateEnd(&(zstrm_wrap->zstrm));
		free(zstrm_wrap);
	}

	while(!is_empty_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list)))
	{
		zstream_wrapper* zstrm_wrap = (zstream_wrapper*) get_head_of_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list));

		remove_head_from_linkedlist(&(zstrm_cash->deflate_zstreams.zstreams_list));

		deflateEnd(&(zstrm_wrap->zstrm));
		free(zstrm_wrap);
	}
}