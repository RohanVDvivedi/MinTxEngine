#ifndef ZLIB_ZSTREAM_CACHE_H
#define ZLIB_ZSTREAM_CACHE_H

#include<zlib.h>

#include<cutlery/linkedlist.h>

#include<stdint.h>

#include<pthread.h>

#define ZSTREAM_CACHE_COUNT                80ULL
#define ZSTREAM_LIFE_IN_MICROSECONDS   600000ULL

#define MINTXENGINE_ZLIB_COMPRESSION_LEVEL  3  // Z_DEFAULT_COMPRESSION can also be used gives good compression but very slow, so we choose level 3

// if any object of zstream is alive for more than ZSTREAM_LIFE_IN_MICROSECONDS and there are more than ZSTREAM_CACHE_COUNT object then start killing them

typedef struct zstream_wrapper zstream_wrapper;
struct zstream_wrapper
{
	z_stream zstrm;

	llnode in_use_cache_node;

	uint64_t last_used_in_microseconds;
};

typedef struct zstream_cache_list zstream_cache_list;
struct zstream_cache_list
{
	uint64_t zstreams_count;

	linkedlist zstreams_list;
};

typedef struct zstream_cache zstream_cache;
struct zstream_cache
{
	pthread_mutex_t zstream_cache_lock;

	zstream_cache_list inflate_zstreams;

	zstream_cache_list deflate_zstreams;
};

void init_zstream_cache(zstream_cache* zstrm_cash);

zstream_wrapper* get_inflate_zstream_object(zstream_cache* zstrm_cash);
zstream_wrapper* get_deflate_zstream_object(zstream_cache* zstrm_cash);

void give_back_inflate_zstream_object(zstream_cache* zstrm_cash, zstream_wrapper* zstrm_wrap);
void give_back_deflate_zstream_object(zstream_cache* zstrm_cash, zstream_wrapper* zstrm_wrap);

void deinit_zstream_cache(zstream_cache* zstrm_cash);

#endif