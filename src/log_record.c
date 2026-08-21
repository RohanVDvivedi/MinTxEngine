#include<mintxengine/log_record.h>

#include<mintxengine/system_page_header_util.h>

#include<zlib.h>

#include<stdlib.h>
#include<string.h>

char const * const log_record_type_strings[] = {
	[UNIDENTIFIED]                            = "UNIDENTIFIED",
	[PAGE_ALLOCATION]                         = "PAGE_ALLOCATION",
	[PAGE_DEALLOCATION]                       = "PAGE_DEALLOCATION",
	[PAGE_INIT]                               = "PAGE_INIT",
	[PAGE_SET_HEADER]                         = "PAGE_SET_HEADER",
	[TUPLE_APPEND]                            = "TUPLE_APPEND",
	[TUPLE_INSERT]                            = "TUPLE_INSERT",
	[TUPLE_UPDATE]                            = "TUPLE_UPDATE",
	[TUPLE_DISCARD]                           = "TUPLE_DISCARD",
	[TUPLE_DISCARD_ALL]                       = "TUPLE_DISCARD_ALL",
	[TUPLE_DISCARD_TRAILING_TOMB_STONES]      = "TUPLE_DISCARD_TRAILING_TOMB_STONES",
	[TUPLE_SWAP]                              = "TUPLE_SWAP",
	[TUPLE_UPDATE_ELEMENT_IN_PLACE]           = "TUPLE_UPDATE_ELEMENT_IN_PLACE",
	[PAGE_CLONE]                              = "PAGE_CLONE",
	[PAGE_COMPACTION]                         = "PAGE_COMPACTION",
	[FULL_PAGE_WRITE]                         = "FULL_PAGE_WRITE",
	[PAGE_INIT_CREATION]                      = "PAGE_INIT_CREATION",
	[COMPENSATION_LOG]                        = "COMPENSATION_LOG",
	[ABORT_MINI_TX]                           = "ABORT_MINI_TX",
	[COMPLETE_MINI_TX]                        = "COMPLETE_MINI_TX",
	[CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY] = "CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY",
	[CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY]       = "CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY",
	[CHECKPOINT_END]                          = "CHECKPOINT_END",
	[USER_INFO]                               = "USER_INFO",
};

// input is always consumed and freed
static void* uncompress_serialized_log_record_idempotently(void* input, uint32_t input_size, uint32_t* output_size)
{
	if(input_size == 0)
	{
		printf("ISSUE :: invalid serialized log record of size 0, requested to be uncompressed\n");
		exit(-1);
	}

	// if the log record is already uncompressed , return input as is
	if(!(((char*)input)[0] & (1<<7)))
	{
		(*output_size) = input_size;
		return input;
	}

	{
		// initialize output
		uint32_t output_capacity = 50;
		void* output = malloc(50);
		if(output == NULL)
		{
			printf("ISSUE :: failure to allocate memory for uncompression of log record\n");
			exit(-1);
		}

		// consume first byte
		((char*)output)[0] = ((char*)input)[0] & (~(1<<7));

		z_stream zstrm;
		zstrm.zalloc = Z_NULL;
		zstrm.zfree = Z_NULL;
		zstrm.opaque = Z_NULL;

		zstrm.next_in = input + 1;
		zstrm.avail_in = input_size - 1;

		zstrm.next_out = output + 1;
		zstrm.avail_out = output_capacity - 1;

		if(Z_OK != inflateInit(&zstrm))
		{
			printf("ISSUE :: failure to initialize zlib uncompression stream for uncompressing log record\n");
			exit(-1);
		}

		while(1)
		{
			int res = inflate(&zstrm, Z_FINISH);

			if(res == Z_OK || res == Z_BUF_ERROR)
			{
				uint32_t new_output_capacity = output_capacity * 2;
				output = realloc(output, new_output_capacity);
				if(output == NULL)
				{
					printf("ISSUE :: failure to allocate memory for uncompression of log record\n");
					exit(-1);
				}

				zstrm.next_out = output + output_capacity;
				zstrm.avail_out = new_output_capacity - output_capacity;

				output_capacity = new_output_capacity;
			}
			else if(res == Z_STREAM_END)
				break;
			else
			{
				printf("ISSUE :: %d error encountered while uncompressing log record inside zlib\n", res);
				exit(-1);
			}
		}

		(*output_size) = zstrm.total_out + 1;

		inflateEnd(&zstrm);

		free(input);
		return output;
	}
}

// every NULLABLE attribute is preceded by a single byte : 0 => NULL, else a value follows

// serialize_tuple_size_def never writes more than this many bytes
#define SIZE_DEF_MAX_BYTES 16

log_record uncompress_and_parse_log_record(const mini_transaction_engine_stats* stats, const void* serialized_log_record, uint32_t serialized_log_record_size)
{
	if(serialized_log_record_size <= 1)
		return (log_record){};

	// uncompress it before parsing
	serialized_log_record = uncompress_serialized_log_record_idempotently((void*)serialized_log_record, serialized_log_record_size, &serialized_log_record_size);

	unsigned char log_record_type = ((const unsigned char*)serialized_log_record)[0];
	const void* c = serialized_log_record + 1;	// cursor into the contents, advanced as we read
	const uint32_t LW = stats->log_sequence_number_width;
	const uint32_t PW = stats->page_id_width;

	switch(log_record_type)
	{
		default : return (log_record){.type = UNIDENTIFIED, .parsed_from = serialized_log_record, .parsed_from_size = serialized_log_record_size};
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			log_record lr;
			lr.type = log_record_type;

			lr.palr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.palr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.palr.page_id = deserialize_uint64(c, PW);				c += PW;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_INIT :
		{
			log_record lr;
			lr.type = PAGE_INIT;

			lr.pilr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.pilr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.pilr.page_id = deserialize_uint64(c, PW);				c += PW;

			// page contents are exactly this many bytes, implied by the page_id, so no size prefix
			uint32_t page_content_size = get_page_content_size_for_page(lr.pilr.page_id, stats);
			lr.pilr.old_page_contents = c;								c += page_content_size;

			lr.pilr.new_page_header_size = deserialize_uint32(c, 4);	c += 4;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.pilr.new_size_def), c, size_def_size);	c += size_def_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_SET_HEADER :
		{
			log_record lr;
			lr.type = PAGE_SET_HEADER;

			lr.pshlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.pshlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.pshlr.page_id = deserialize_uint64(c, PW);				c += PW;

			// the size sits in front of BOTH headers, so neither needs its own prefix
			lr.pshlr.page_header_size = deserialize_uint32(c, 4);		c += 4;
			lr.pshlr.old_page_header_contents = c;						c += lr.pshlr.page_header_size;
			lr.pshlr.new_page_header_contents = c;						c += lr.pshlr.page_header_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_APPEND :
		{
			log_record lr;
			lr.type = TUPLE_APPEND;

			lr.talr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.talr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.talr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.talr.size_def), c, size_def_size);	c += size_def_size;

			int new_tuple_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(new_tuple_is_NOT_NULL)
			{
				uint32_t new_tuple_size = get_tuple_size_using_tuple_size_def(&(lr.talr.size_def), c);
				lr.talr.new_tuple = c;									c += new_tuple_size;
			}
			else
				lr.talr.new_tuple = NULL;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_INSERT :
		{
			log_record lr;
			lr.type = TUPLE_INSERT;

			lr.tilr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tilr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tilr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.tilr.size_def), c, size_def_size);	c += size_def_size;

			lr.tilr.insert_index = deserialize_uint32(c, 4);			c += 4;

			int new_tuple_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(new_tuple_is_NOT_NULL)
			{
				uint32_t new_tuple_size = get_tuple_size_using_tuple_size_def(&(lr.tilr.size_def), c);
				lr.tilr.new_tuple = c;									c += new_tuple_size;
			}
			else
				lr.tilr.new_tuple = NULL;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_UPDATE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE;

			lr.tulr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tulr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tulr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.tulr.size_def), c, size_def_size);	c += size_def_size;

			lr.tulr.update_index = deserialize_uint32(c, 4);			c += 4;

			int old_tuple_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(old_tuple_is_NOT_NULL)
			{
				uint32_t old_tuple_size = get_tuple_size_using_tuple_size_def(&(lr.tulr.size_def), c);
				lr.tulr.old_tuple = c;									c += old_tuple_size;
			}
			else
				lr.tulr.old_tuple = NULL;

			int new_tuple_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(new_tuple_is_NOT_NULL)
			{
				uint32_t new_tuple_size = get_tuple_size_using_tuple_size_def(&(lr.tulr.size_def), c);
				lr.tulr.new_tuple = c;									c += new_tuple_size;
			}
			else
				lr.tulr.new_tuple = NULL;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_DISCARD :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD;

			lr.tdlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tdlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tdlr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.tdlr.size_def), c, size_def_size);	c += size_def_size;

			lr.tdlr.discard_index = deserialize_uint32(c, 4);			c += 4;

			int old_tuple_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(old_tuple_is_NOT_NULL)
			{
				uint32_t old_tuple_size = get_tuple_size_using_tuple_size_def(&(lr.tdlr.size_def), c);
				lr.tdlr.old_tuple = c;									c += old_tuple_size;
			}
			else
				lr.tdlr.old_tuple = NULL;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_DISCARD_ALL :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_ALL;

			lr.tdalr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tdalr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tdalr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.tdalr.size_def), c, size_def_size);	c += size_def_size;

			uint32_t page_content_size = get_page_content_size_for_page(lr.tdalr.page_id, stats);
			lr.tdalr.old_page_contents = c;								c += page_content_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			log_record lr;
			lr.type = TUPLE_DISCARD_TRAILING_TOMB_STONES;

			lr.tdttlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tdttlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tdttlr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.tdttlr.size_def), c, size_def_size);	c += size_def_size;

			lr.tdttlr.discarded_trailing_tomb_stones_count = deserialize_uint32(c, 4);	c += 4;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_SWAP :
		{
			log_record lr;
			lr.type = TUPLE_SWAP;

			lr.tslr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tslr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tslr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.tslr.size_def), c, size_def_size);	c += size_def_size;

			lr.tslr.swap_index1 = deserialize_uint32(c, 4);			c += 4;
			lr.tslr.swap_index2 = deserialize_uint32(c, 4);			c += 4;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			log_record lr;
			lr.type = TUPLE_UPDATE_ELEMENT_IN_PLACE;

			lr.tueiplr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.tueiplr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.tueiplr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t type_info_size = deserialize_uint32(c, 4);			c += 4;
			int type_info_allocation_error = 0;
			data_type_info* dti = deserialize_type_info(c, type_info_size, &type_info_allocation_error);
			if(type_info_allocation_error)
				exit(-1);
			c += type_info_size;
			if(dti != NULL)
				finalize_type_info(dti);
			initialize_tuple_def(&(lr.tueiplr.tpl_def), dti);

			lr.tueiplr.tuple_index = deserialize_uint32(c, 4);			c += 4;

			uint32_t positions_length = deserialize_uint32(c, 4);		c += 4;
			lr.tueiplr.element_index.positions_length = positions_length;
			lr.tueiplr.element_index.positions = (positions_length == 0) ? NULL : malloc(sizeof(uint32_t) * positions_length);
			for(uint32_t k = 0; k < positions_length; k++)
			{
				lr.tueiplr.element_index.positions[k] = deserialize_uint32(c, 4);	c += 4;
			}

			const data_type_info* element_def = (dti == NULL) ? NULL : get_type_info_for_element_from_tuple_def(&(lr.tueiplr.tpl_def), lr.tueiplr.element_index);

			int old_element_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(!old_element_is_NOT_NULL)
				lr.tueiplr.old_element = (*NULL_DATUM);
			else if(element_def->type == BIT_FIELD)
			{
				get_datum_for_type_info(&(lr.tueiplr.old_element), UINT_NULLABLE[8], c); c += 8;
				lr.tueiplr.old_element.bit_field_value = lr.tueiplr.old_element.uint_value;
			}
			else
			{
				get_datum_for_type_info(&(lr.tueiplr.old_element), element_def, c); c += get_size_for_type_info(element_def, c);
			}

			int new_element_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(!new_element_is_NOT_NULL)
				lr.tueiplr.new_element = (*NULL_DATUM);
			else if(element_def->type == BIT_FIELD)
			{
				get_datum_for_type_info(&(lr.tueiplr.new_element), UINT_NULLABLE[8], c); c += 8;
				lr.tueiplr.new_element.bit_field_value = lr.tueiplr.new_element.uint_value;
			}
			else
			{
				get_datum_for_type_info(&(lr.tueiplr.new_element), element_def, c);  c += get_size_for_type_info(element_def, c);
			}

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_CLONE :
		{
			log_record lr;
			lr.type = PAGE_CLONE;

			lr.pclr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.pclr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.pclr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.pclr.size_def), c, size_def_size);	c += size_def_size;

			uint32_t page_content_size = get_page_content_size_for_page(lr.pclr.page_id, stats);
			lr.pclr.old_page_contents = c;								c += page_content_size;
			lr.pclr.new_page_contents = c;								c += page_content_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_COMPACTION :
		{
			log_record lr;
			lr.type = PAGE_COMPACTION;

			lr.pcptlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.pcptlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.pcptlr.page_id = deserialize_uint64(c, PW);				c += PW;

			uint32_t size_def_size = deserialize_uint32(c, 4);			c += 4;
			deserialize_tuple_size_def(&(lr.pcptlr.size_def), c, size_def_size);	c += size_def_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case FULL_PAGE_WRITE :
		{
			log_record lr;
			lr.type = FULL_PAGE_WRITE;

			lr.fpwlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.fpwlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.fpwlr.page_id = deserialize_uint64(c, PW);				c += PW;
			lr.fpwlr.writerLSN = deserialize_uint256(c, LW);			c += LW;

			uint32_t page_content_size = get_page_content_size_for_page(lr.fpwlr.page_id, stats);
			lr.fpwlr.page_contents = c;									c += page_content_size;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case PAGE_INIT_CREATION :
		{
			log_record lr;
			lr.type = PAGE_INIT_CREATION;

			lr.piclr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.piclr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.piclr.page_id = deserialize_uint64(c, PW);				c += PW;

			lr.piclr.init_type = ((unsigned char*)c)[0];				c += 1;

			if(lr.piclr.init_type == PAGE_INIT_CONTENT_DATA)
			{
				uint32_t page_content_size = get_page_content_size_for_page(lr.piclr.page_id, stats);
				lr.piclr.page_contents = c;									c += page_content_size;
			}
			else
				lr.piclr.page_contents = NULL;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case COMPENSATION_LOG :
		{
			log_record lr;
			lr.type = COMPENSATION_LOG;

			lr.clr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.clr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.clr.undo_of_LSN = deserialize_uint256(c, LW);			c += LW;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case ABORT_MINI_TX :
		{
			log_record lr;
			lr.type = ABORT_MINI_TX;

			lr.amtlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.amtlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.amtlr.abort_error = deserialize_int32(c, 4);				c += 4;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case COMPLETE_MINI_TX :
		{
			log_record lr;
			lr.type = COMPLETE_MINI_TX;

			lr.cmtlr.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.cmtlr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.cmtlr.is_aborted = ((const unsigned char*)c)[0];			c += 1;

			int cmtlr_info_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(cmtlr_info_is_NOT_NULL)
			{
				lr.cmtlr.info_size = deserialize_uint32(c, 4);			c += 4;
				lr.cmtlr.info = c;										c += lr.cmtlr.info_size;
			}
			else
			{
				lr.cmtlr.info = NULL;
				lr.cmtlr.info_size = 0;
			}

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			log_record lr;
			lr.type = CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY;

			lr.ckptmttelr.prev_log_record_LSN = deserialize_uint256(c, LW);		c += LW;
			lr.ckptmttelr.mt.mini_transaction_id = deserialize_uint256(c, LW);	c += LW;
			lr.ckptmttelr.mt.lastLSN = deserialize_uint256(c, LW);				c += LW;
			lr.ckptmttelr.mt.state = deserialize_uint32(c, 4);					c += 4;
			lr.ckptmttelr.mt.abort_error = deserialize_int32(c, 4);				c += 4;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			log_record lr;
			lr.type = CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY;

			lr.ckptdptelr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.ckptdptelr.dpte.page_id = deserialize_uint64(c, PW);			c += PW;
			lr.ckptdptelr.dpte.recLSN = deserialize_uint256(c, LW);			c += LW;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case CHECKPOINT_END :
		{
			log_record lr;
			lr.type = CHECKPOINT_END;

			lr.ckptelr.prev_log_record_LSN = deserialize_uint256(c, LW);	c += LW;
			lr.ckptelr.begin_LSN = deserialize_uint256(c, LW);				c += LW;

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
		case USER_INFO :
		{
			log_record lr;
			lr.type = USER_INFO;

			int uilr_info_is_NOT_NULL = (((const unsigned char*)c)[0] != 0);	c += 1;
			if(uilr_info_is_NOT_NULL)
			{
				lr.uilr.info_size = deserialize_uint32(c, 4);			c += 4;
				lr.uilr.info = c;										c += lr.uilr.info_size;
			}
			else
			{
				lr.uilr.info = NULL;
				lr.uilr.info_size = 0;
			}

			lr.parsed_from = serialized_log_record;
			lr.parsed_from_size = serialized_log_record_size;
			return lr;
		}
	}
}

void destroy_and_free_parsed_log_record(log_record* lr)
{
	switch(lr->type)
	{
		default :
			break;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			destroy_type_info_recursively(lr->tueiplr.tpl_def.type_info, NULL);
			free(lr->tueiplr.element_index.positions);
			break;
		}
	}

	free((void*)(lr->parsed_from));

	(*lr) = (log_record){};
}

// compression limit should be set in some 100s of bytes
// set it to UINT32_MAX to completely disable compression, this would save a lot on the compute, but will blow up your log sizes, you can try and see if you want it
#define COMPRESSION_LIMIT 400 // all log records with size greater than COMPRESSION_LIMIT will be compressed

// input is always consumed and freed
static void* compress_serialized_log_record_idempotently(void* input, uint32_t input_size, uint32_t* output_size)
{
	if(input_size == 0)
	{
		printf("ISSUE :: invalid serialized log record of size 0, requested to be compressed\n");
		exit(-1);
	}

	// if the log record is already compressed OR the input_size is smaller than 100 , return input as is
	if(((char*)input)[0] & (1<<7))
	{
		(*output_size) = input_size;
		return input;
	}

	if(input_size < COMPRESSION_LIMIT)
	{
		(*output_size) = input_size;
		return input;
	}

	{
		// initialize output
		uint32_t output_capacity = 50;
		void* output = malloc(50);
		if(output == NULL)
		{
			printf("ISSUE :: failure to allocate memory for compression of log record\n");
			exit(-1);
		}

		// consume first byte
		((char*)output)[0] = ((char*)input)[0] | (1<<7);

		z_stream zstrm;
		zstrm.zalloc = Z_NULL;
		zstrm.zfree = Z_NULL;
		zstrm.opaque = Z_NULL;

		zstrm.next_in = input + 1;
		zstrm.avail_in = input_size - 1;

		zstrm.next_out = output + 1;
		zstrm.avail_out = output_capacity - 1;

		if(Z_OK != deflateInit(&zstrm, 3)) // Z_DEFAULT_COMPRESSION can also be used gives good compression but very slow, so we choose level 3
		{
			printf("ISSUE :: failure to initialize zlib compression stream for compressing log record\n");
			exit(-1);
		}

		while(1)
		{
			int res = deflate(&zstrm, Z_FINISH);

			if(res == Z_OK || res == Z_BUF_ERROR)
			{
				uint32_t new_output_capacity = output_capacity * 2;
				output = realloc(output, new_output_capacity);
				if(output == NULL)
				{
					printf("ISSUE :: failure to allocate memory for compression of log record\n");
					exit(-1);
				}

				zstrm.next_out = output + output_capacity;
				zstrm.avail_out = new_output_capacity - output_capacity;

				output_capacity = new_output_capacity;
			}
			else if(res == Z_STREAM_END)
				break;
			else
			{
				printf("ISSUE :: %d error encountered while compressing log record inside zlib\n", res);
				exit(-1);
			}
		}

		(*output_size) = zstrm.total_out + 1;

		deflateEnd(&zstrm);

		if((*output_size) >= input_size) // if for some reason data expanded instead then return input as is, freeing the allocated output buffer
		{
			free(output);
			(*output_size) = input_size;
			return input;
		}

		free(input);
		return output;
	}
}

const void* serialize_and_compress_log_record(const mini_transaction_engine_stats* stats, const log_record* lr, uint32_t* result_size)
{
	const uint32_t LW = stats->log_sequence_number_width;
	const uint32_t PW = stats->page_id_width;

	// ---- PASS 1 : compute the exact size, so the buffer is allocated once and never grown ----
	uint32_t capacity = 1;	// the type byte
	switch(lr->type)
	{
		default :
			(*result_size) = 0;
			return NULL;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			capacity += 2 * LW + PW;
			break;
		case PAGE_INIT :
			capacity += 2 * LW + PW + get_page_content_size_for_page(lr->pilr.page_id, stats) + 4 + 4 + SIZE_DEF_MAX_BYTES;
			break;
		case PAGE_SET_HEADER :
			capacity += 2 * LW + PW + 4 + 2 * lr->pshlr.page_header_size;
			break;
		case TUPLE_APPEND :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 1
				+ ((lr->talr.new_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple));
			break;
		case TUPLE_INSERT :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 4 + 1
				+ ((lr->tilr.new_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple));
			break;
		case TUPLE_UPDATE :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 4 + 2 * (1)
				+ ((lr->tulr.old_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple))
				+ ((lr->tulr.new_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple));
			break;
		case TUPLE_DISCARD :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 4 + 1
				+ ((lr->tdlr.old_tuple == NULL) ? 0 : get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple));
			break;
		case TUPLE_DISCARD_ALL :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + get_page_content_size_for_page(lr->tdalr.page_id, stats);
			break;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 4;
			break;
		case TUPLE_SWAP :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 4 + 4;
			break;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			const data_type_info* element_def = get_type_info_for_element_from_tuple_def(&(lr->tueiplr.tpl_def), lr->tueiplr.element_index);
			capacity += 2 * LW + PW
				+ 4 + get_byte_count_for_serialized_type_info(lr->tueiplr.tpl_def.type_info)
				+ 4
				+ 4 + 4 * lr->tueiplr.element_index.positions_length
				+ 1 + (is_datum_NULL(&(lr->tueiplr.old_element)) ? 0 : (is_variable_sized_type_info(element_def) ? stats->page_size : max(element_def->size, 8)))
				+ 1 + (is_datum_NULL(&(lr->tueiplr.new_element)) ? 0 : (is_variable_sized_type_info(element_def) ? stats->page_size : max(element_def->size, 8)));
			break;
		}
		case PAGE_CLONE :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES + 2 * get_page_content_size_for_page(lr->pclr.page_id, stats);
			break;
		case PAGE_COMPACTION :
			capacity += 2 * LW + PW + 4 + SIZE_DEF_MAX_BYTES;
			break;
		case FULL_PAGE_WRITE :
			capacity += 2 * LW + PW + LW + get_page_content_size_for_page(lr->fpwlr.page_id, stats);
			break;
		case PAGE_INIT_CREATION :
			capacity += 2 * LW + PW + 1 + get_page_content_size_for_page(lr->piclr.page_id, stats);
			break;
		case COMPENSATION_LOG :
			capacity += 3 * LW;
			break;
		case ABORT_MINI_TX :
			capacity += 2 * LW + 4;
			break;
		case COMPLETE_MINI_TX :
			capacity += 2 * LW + 1 + 1 + 4 + lr->cmtlr.info_size;
			break;
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
			capacity += 3 * LW + 4 + 4;
			break;
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
			capacity += 2 * LW + PW;
			break;
		case CHECKPOINT_END :
			capacity += 2 * LW;
			break;
		case USER_INFO :
			capacity += 1 + ((lr->uilr.info != NULL) ?  (4 + lr->uilr.info_size) : 0);
			break;
	}

	void* result = malloc(capacity);
	if(result == NULL)
	{
		(*result_size) = 0;
		return NULL;
	}

	// ---- PASS 2 : write ----
	((unsigned char*)result)[0] = lr->type;
	void* c = result + 1;	// cursor into the contents, advanced as we write

	switch(lr->type)
	{
		default :
			break;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			serialize_uint256(c, LW, lr->palr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->palr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->palr.page_id);				c += PW;
			break;
		}
		case PAGE_INIT :
		{
			serialize_uint256(c, LW, lr->pilr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->pilr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->pilr.page_id);				c += PW;

			// the page content size is implied by the page_id, so it is never stored
			uint32_t page_content_size = get_page_content_size_for_page(lr->pilr.page_id, stats);
			memory_move(c, lr->pilr.old_page_contents, page_content_size);	c += page_content_size;

			serialize_uint32(c, 4, lr->pilr.new_page_header_size);	c += 4;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->pilr.new_size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;
			break;
		}
		case PAGE_SET_HEADER :
		{
			serialize_uint256(c, LW, lr->pshlr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->pshlr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->pshlr.page_id);					c += PW;

			// one size in front covers BOTH headers
			serialize_uint32(c, 4, lr->pshlr.page_header_size);			c += 4;
			memory_move(c, lr->pshlr.old_page_header_contents, lr->pshlr.page_header_size);	c += lr->pshlr.page_header_size;
			memory_move(c, lr->pshlr.new_page_header_contents, lr->pshlr.page_header_size);	c += lr->pshlr.page_header_size;
			break;
		}
		case TUPLE_APPEND :
		{
			serialize_uint256(c, LW, lr->talr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->talr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->talr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->talr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;

			((unsigned char*)c)[0] = ((lr->talr.new_tuple == NULL) ? 0 : 1);	c += 1;
			if(lr->talr.new_tuple != NULL)
			{
				uint32_t new_tuple_size = get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple);
				memory_move(c, lr->talr.new_tuple, new_tuple_size);	c += new_tuple_size;
			}
			break;
		}
		case TUPLE_INSERT :
		{
			serialize_uint256(c, LW, lr->tilr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tilr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tilr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->tilr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;

			serialize_uint32(c, 4, lr->tilr.insert_index);			c += 4;

			((unsigned char*)c)[0] = ((lr->tilr.new_tuple == NULL) ? 0 : 1);	c += 1;
			if(lr->tilr.new_tuple != NULL)
			{
				uint32_t new_tuple_size = get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple);
				memory_move(c, lr->tilr.new_tuple, new_tuple_size);	c += new_tuple_size;
			}
			break;
		}
		case TUPLE_UPDATE :
		{
			serialize_uint256(c, LW, lr->tulr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tulr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tulr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->tulr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;

			serialize_uint32(c, 4, lr->tulr.update_index);			c += 4;

			((unsigned char*)c)[0] = ((lr->tulr.old_tuple == NULL) ? 0 : 1);	c += 1;
			if(lr->tulr.old_tuple != NULL)
			{
				uint32_t old_tuple_size = get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple);
				memory_move(c, lr->tulr.old_tuple, old_tuple_size);	c += old_tuple_size;
			}

			((unsigned char*)c)[0] = ((lr->tulr.new_tuple == NULL) ? 0 : 1);	c += 1;
			if(lr->tulr.new_tuple != NULL)
			{
				uint32_t new_tuple_size = get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple);
				memory_move(c, lr->tulr.new_tuple, new_tuple_size);	c += new_tuple_size;
			}
			break;
		}
		case TUPLE_DISCARD :
		{
			serialize_uint256(c, LW, lr->tdlr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tdlr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tdlr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->tdlr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;

			serialize_uint32(c, 4, lr->tdlr.discard_index);			c += 4;

			((unsigned char*)c)[0] = ((lr->tdlr.old_tuple == NULL) ? 0 : 1);	c += 1;
			if(lr->tdlr.old_tuple != NULL)
			{
				uint32_t old_tuple_size = get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple);
				memory_move(c, lr->tdlr.old_tuple, old_tuple_size);	c += old_tuple_size;
			}
			break;
		}
		case TUPLE_DISCARD_ALL :
		{
			serialize_uint256(c, LW, lr->tdalr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tdalr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tdalr.page_id);					c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->tdalr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);						c += 4 + size_def_size;

			uint32_t page_content_size = get_page_content_size_for_page(lr->tdalr.page_id, stats);
			memory_move(c, lr->tdalr.old_page_contents, page_content_size);	c += page_content_size;
			break;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			serialize_uint256(c, LW, lr->tdttlr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tdttlr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tdttlr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->tdttlr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);						c += 4 + size_def_size;

			serialize_uint32(c, 4, lr->tdttlr.discarded_trailing_tomb_stones_count);	c += 4;
			break;
		}
		case TUPLE_SWAP :
		{
			serialize_uint256(c, LW, lr->tslr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tslr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tslr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->tslr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;

			serialize_uint32(c, 4, lr->tslr.swap_index1);			c += 4;
			serialize_uint32(c, 4, lr->tslr.swap_index2);			c += 4;
			break;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			serialize_uint256(c, LW, lr->tueiplr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->tueiplr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->tueiplr.page_id);				c += PW;

			uint32_t type_info_size = serialize_type_info(lr->tueiplr.tpl_def.type_info, c + 4);
			serialize_uint32(c, 4, type_info_size);						c += 4 + type_info_size;

			serialize_uint32(c, 4, lr->tueiplr.tuple_index);			c += 4;

			serialize_uint32(c, 4, lr->tueiplr.element_index.positions_length);	c += 4;
			for(uint32_t k = 0; k < lr->tueiplr.element_index.positions_length; k++)
			{
				serialize_uint32(c, 4, lr->tueiplr.element_index.positions[k]);	c += 4;
			}

			const data_type_info* element_def = get_type_info_for_element_from_tuple_def(&(lr->tueiplr.tpl_def), lr->tueiplr.element_index);

			((unsigned char*)c)[0] = (is_datum_NULL(&(lr->tueiplr.old_element)) ? 0 : 1);	c += 1;
			if(!is_datum_NULL(&(lr->tueiplr.old_element)))
			{
				if(element_def->type == BIT_FIELD)
				{
					if(!set_datum_for_type_info(UINT_NULLABLE[8], c, 0, UINT32_MAX, &(datum){.uint_value = lr->tueiplr.old_element.bit_field_value}))
						exit(-1);
					c += 8;
				}
				else
				{
					if(!set_datum_for_type_info(element_def, c, 0, UINT32_MAX, &(lr->tueiplr.old_element)))
						exit(-1);
					c += get_size_for_type_info(element_def, c);
				}
			}

			((unsigned char*)c)[0] = (is_datum_NULL(&(lr->tueiplr.new_element)) ? 0 : 1);	c += 1;
			if(!is_datum_NULL(&(lr->tueiplr.new_element)))
			{
				if(element_def->type == BIT_FIELD)
				{
					if(!set_datum_for_type_info(UINT_NULLABLE[8], c, 0, UINT32_MAX, &(datum){.uint_value = lr->tueiplr.new_element.bit_field_value}))
						exit(-1);
					c += 8;
				}
				else
				{
					if(!set_datum_for_type_info(element_def, c, 0, UINT32_MAX, &(lr->tueiplr.new_element)))
						exit(-1);
					c += get_size_for_type_info(element_def, c);
				}
			}
			break;
		}
		case PAGE_CLONE :
		{
			serialize_uint256(c, LW, lr->pclr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->pclr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->pclr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->pclr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);					c += 4 + size_def_size;

			uint32_t page_content_size = get_page_content_size_for_page(lr->pclr.page_id, stats);
			memory_move(c, lr->pclr.old_page_contents, page_content_size);	c += page_content_size;
			memory_move(c, lr->pclr.new_page_contents, page_content_size);	c += page_content_size;
			break;
		}
		case PAGE_COMPACTION :
		{
			serialize_uint256(c, LW, lr->pcptlr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->pcptlr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->pcptlr.page_id);				c += PW;

			uint32_t size_def_size = serialize_tuple_size_def(&(lr->pcptlr.size_def), c + 4);
			serialize_uint32(c, 4, size_def_size);						c += 4 + size_def_size;
			break;
		}
		case FULL_PAGE_WRITE :
		{
			serialize_uint256(c, LW, lr->fpwlr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->fpwlr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->fpwlr.page_id);					c += PW;
			serialize_uint256(c, LW, lr->fpwlr.writerLSN);				c += LW;

			uint32_t page_content_size = get_page_content_size_for_page(lr->fpwlr.page_id, stats);
			memory_move(c, lr->fpwlr.page_contents, page_content_size);	c += page_content_size;
			break;
		}
		case PAGE_INIT_CREATION :
		{
			serialize_uint256(c, LW, lr->piclr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->piclr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->piclr.page_id);					c += PW;

			((char*)c)[0] = lr->piclr.init_type; c += 1;

			if(lr->piclr.init_type == PAGE_INIT_CONTENT_DATA)
			{
				uint32_t page_content_size = get_page_content_size_for_page(lr->piclr.page_id, stats);
				memory_move(c, lr->piclr.page_contents, page_content_size);	c += page_content_size;
			}
			break;
		}
		case COMPENSATION_LOG :
		{
			serialize_uint256(c, LW, lr->clr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->clr.prev_log_record_LSN);	c += LW;
			serialize_uint256(c, LW, lr->clr.undo_of_LSN);			c += LW;
			break;
		}
		case ABORT_MINI_TX :
		{
			serialize_uint256(c, LW, lr->amtlr.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->amtlr.prev_log_record_LSN);	c += LW;
			serialize_int32(c, 4, lr->amtlr.abort_error);				c += 4;
			break;
		}
		case COMPLETE_MINI_TX :
		{
			serialize_uint256(c, LW, lr->cmtlr.mini_transaction_id);		c += LW;
			serialize_uint256(c, LW, lr->cmtlr.prev_log_record_LSN);		c += LW;
			((unsigned char*)c)[0] = lr->cmtlr.is_aborted;					c += 1;

			((unsigned char*)c)[0] = ((lr->cmtlr.info == NULL) ? 0 : 1);	c += 1;
			if(lr->cmtlr.info != NULL)
			{
				serialize_uint32(c, 4, lr->cmtlr.info_size);				c += 4;
				memory_move(c, lr->cmtlr.info, lr->cmtlr.info_size);		c += lr->cmtlr.info_size;
			}
			break;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			serialize_uint256(c, LW, lr->ckptmttelr.prev_log_record_LSN);		c += LW;
			serialize_uint256(c, LW, lr->ckptmttelr.mt.mini_transaction_id);	c += LW;
			serialize_uint256(c, LW, lr->ckptmttelr.mt.lastLSN);				c += LW;
			serialize_uint32(c, 4, lr->ckptmttelr.mt.state);					c += 4;
			serialize_int32(c, 4, lr->ckptmttelr.mt.abort_error);				c += 4;
			break;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			serialize_uint256(c, LW, lr->ckptdptelr.prev_log_record_LSN);	c += LW;
			serialize_uint64(c, PW, lr->ckptdptelr.dpte.page_id);			c += PW;
			serialize_uint256(c, LW, lr->ckptdptelr.dpte.recLSN);			c += LW;
			break;
		}
		case CHECKPOINT_END :
		{
			serialize_uint256(c, LW, lr->ckptelr.prev_log_record_LSN);	c += LW;
			serialize_uint256(c, LW, lr->ckptelr.begin_LSN);			c += LW;
			break;
		}
		case USER_INFO :
		{
			((unsigned char*)c)[0] = ((lr->uilr.info == NULL) ? 0 : 1);	c += 1;
			if(lr->uilr.info != NULL)
			{
				serialize_uint32(c, 4, lr->uilr.info_size);			c += 4;
				memory_move(c, lr->uilr.info, lr->uilr.info_size);	c += lr->uilr.info_size;
			}
			break;
		}
	}

	(*result_size) = c - result;

	// compress it, if it is big enough to be worth it
	return compress_serialized_log_record_idempotently(result, (*result_size), result_size);
}

static void print_binary(const void* data, uint32_t data_size)
{
	if(data == NULL)
	{
		printf("NULL");
		return;
	}
	for(uint32_t i = 0; i < data_size; i++)
		printf("%02x, ", ((unsigned char*)data)[i]);
}

void print_log_record(const log_record* lr, const mini_transaction_engine_stats* stats)
{
	printf("type : %s\n", log_record_type_strings[lr->type]);

	switch(lr->type)
	{
		default :
			return;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			printf("mini_transaction_id : "); print_uint256(lr->palr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->palr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->palr.page_id);
			return;
		}
		case PAGE_INIT :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pilr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pilr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pilr.page_id);
			printf("old_page_contents : "); print_binary(lr->pilr.old_page_contents, get_page_content_size_for_page(lr->pilr.page_id, stats)); printf("\n");
			printf("new_page_header_size : %"PRIu32"\n", lr->pilr.new_page_header_size);
			printf("new_size_def : \n"); print_tuple_size_def(&(lr->pilr.new_size_def)); printf("\n");
			return;
		}
		case PAGE_SET_HEADER :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pshlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pshlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pshlr.page_id);
			printf("old_page_header_contents : "); print_binary(lr->pshlr.old_page_header_contents, lr->pshlr.page_header_size); printf("\n");
			printf("new_page_header_contents : "); print_binary(lr->pshlr.new_page_header_contents, lr->pshlr.page_header_size); printf("\n");
			return;
		}
		case TUPLE_APPEND :
		{
			printf("mini_transaction_id : "); print_uint256(lr->talr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->talr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->talr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->talr.size_def)); printf("\n");
			printf("new_tuple : ");
			if(lr->talr.new_tuple != NULL)
				print_binary(lr->talr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->talr.size_def), lr->talr.new_tuple));
			else
				print_binary(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_INSERT :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tilr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tilr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tilr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tilr.size_def)); printf("\n");
			printf("insert_index : %"PRIu32"\n", lr->tilr.insert_index);
			printf("new_tuple : ");
			if(lr->tilr.new_tuple != NULL)
				print_binary(lr->tilr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->tilr.size_def), lr->tilr.new_tuple));
			else
				print_binary(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_UPDATE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tulr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tulr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tulr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tulr.size_def)); printf("\n");
			printf("update_index : %"PRIu32"\n", lr->tulr.update_index);
			printf("old_tuple : ");
			if(lr->tulr.old_tuple != NULL)
				print_binary(lr->tulr.old_tuple, get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.old_tuple));
			else
				print_binary(NULL, 0);
			printf("\n");
			printf("new_tuple : ");
			if(lr->tulr.new_tuple != NULL)
				print_binary(lr->tulr.new_tuple, get_tuple_size_using_tuple_size_def(&(lr->tulr.size_def), lr->tulr.new_tuple));
			else
				print_binary(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_DISCARD :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tdlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdlr.size_def)); printf("\n");
			printf("discard_index : %"PRIu32"\n", lr->tdlr.discard_index);
			printf("old_tuple : ");
			if(lr->tdlr.old_tuple != NULL)
				print_binary(lr->tdlr.old_tuple, get_tuple_size_using_tuple_size_def(&(lr->tdlr.size_def), lr->tdlr.old_tuple));
			else
				print_binary(NULL, 0);
			printf("\n");
			return;
		}
		case TUPLE_DISCARD_ALL :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdalr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tdalr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdalr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdalr.size_def)); printf("\n");
			printf("old_page_contents : "); print_binary(lr->tdalr.old_page_contents, get_page_content_size_for_page(lr->tdalr.page_id, stats)); printf("\n");
			return;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tdttlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tdttlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tdttlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tdttlr.size_def)); printf("\n");
			printf("discarded_trailing_tomb_stones_count : %"PRIu32"\n", lr->tdttlr.discarded_trailing_tomb_stones_count);
			return;
		}
		case TUPLE_SWAP :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tslr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tslr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tslr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->tslr.size_def)); printf("\n");
			printf("swap_index1 : %"PRIu32"\n", lr->tslr.swap_index1);
			printf("swap_index2 : %"PRIu32"\n", lr->tslr.swap_index2);
			return;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->tueiplr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->tueiplr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->tueiplr.page_id);
			printf("tpl_def : "); print_tuple_def(&(lr->tueiplr.tpl_def)); printf("\n");
			printf("tuple_index : %"PRIu32"\n", lr->tueiplr.tuple_index);
			printf("element_index : {"); for(uint32_t i = 0; i < lr->tueiplr.element_index.positions_length; i++) printf("%"PRIu32", ", lr->tueiplr.element_index.positions[i]); printf("}\n");
			const data_type_info* ele_def = get_type_info_for_element_from_tuple_def(&(lr->tueiplr.tpl_def), lr->tueiplr.element_index);
			printf("old_element : "); print_datum(&(lr->tueiplr.old_element), ele_def); printf("\n");
			printf("new_element : "); print_datum(&(lr->tueiplr.new_element), ele_def); printf("\n");
			return;
		}
		case PAGE_CLONE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pclr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pclr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pclr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->pclr.size_def)); printf("\n");
			printf("old_page_contents : "); print_binary(lr->pclr.old_page_contents, get_page_content_size_for_page(lr->pclr.page_id, stats)); printf("\n");
			printf("new_page_contents : "); print_binary(lr->pclr.new_page_contents, get_page_content_size_for_page(lr->pclr.page_id, stats)); printf("\n");
			return;
		}
		case PAGE_COMPACTION :
		{
			printf("mini_transaction_id : "); print_uint256(lr->pcptlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->pcptlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->pcptlr.page_id);
			printf("size_def : \n"); print_tuple_size_def(&(lr->pcptlr.size_def)); printf("\n");
			return;
		}
		case FULL_PAGE_WRITE :
		{
			printf("mini_transaction_id : "); print_uint256(lr->fpwlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->fpwlr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->fpwlr.page_id);
			printf("writerLSN : "); print_uint256(lr->fpwlr.writerLSN); printf("\n");
			printf("page_contents : "); print_binary(lr->fpwlr.page_contents, get_page_content_size_for_page(lr->fpwlr.page_id, stats)); printf("\n");
			return;
		}
		case PAGE_INIT_CREATION :
		{
			printf("mini_transaction_id : "); print_uint256(lr->piclr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->piclr.prev_log_record_LSN); printf("\n");
			printf("page_id : %"PRIu64"\n", lr->piclr.page_id);
			printf("init_type : ");
				if(lr->piclr.init_type == PAGE_INIT_GARBAGE_DATA) printf("GARBAGE");
				else if(lr->piclr.init_type == PAGE_INIT_ZERO_DATA) printf("ZERO");
				else if(lr->piclr.init_type == PAGE_INIT_CONTENT_DATA) printf("CONTENT");
				else printf("%d", lr->piclr.init_type);
			printf("\n");
			if(lr->piclr.init_type == PAGE_INIT_CONTENT_DATA)
			{
				printf("page_contents : "); print_binary(lr->piclr.page_contents, get_page_content_size_for_page(lr->piclr.page_id, stats)); printf("\n");
			}
			return;
		}
		case COMPENSATION_LOG :
		{
			printf("mini_transaction_id : "); print_uint256(lr->clr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->clr.prev_log_record_LSN); printf("\n");
			printf("undo_of_LSN : "); print_uint256(lr->clr.undo_of_LSN); printf("\n");
			return;
		}
		case ABORT_MINI_TX :
		{
			printf("mini_transaction_id : "); print_uint256(lr->amtlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->amtlr.prev_log_record_LSN); printf("\n");
			printf("abort_error : %d\n", lr->amtlr.abort_error);
			return;
		}
		case COMPLETE_MINI_TX :
		{
			printf("mini_transaction_id : "); print_uint256(lr->cmtlr.mini_transaction_id); printf("\n");
			printf("prev_log_record_LSN : "); print_uint256(lr->cmtlr.prev_log_record_LSN); printf("\n");
			printf("is_aborted : %d\n", !!(lr->cmtlr.is_aborted));
			printf("info : "); print_binary(lr->cmtlr.info, lr->cmtlr.info_size); printf("\n");
			return;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			printf("prev_log_record_LSN : "); print_uint256(lr->ckptmttelr.prev_log_record_LSN); printf("\n");
			printf("mt :\n");
			printf("mini_transaction_id : "); print_uint256(lr->ckptmttelr.mt.mini_transaction_id); printf("\n");
			printf("lastLSN : "); print_uint256(lr->ckptmttelr.mt.lastLSN); printf("\n");
			printf("state : %d\n", lr->ckptmttelr.mt.state);
			printf("abort_error : %d\n", lr->ckptmttelr.mt.abort_error);
			return;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			printf("prev_log_record_LSN : "); print_uint256(lr->ckptdptelr.prev_log_record_LSN); printf("\n");
			printf("dpte :\n");
			printf("page_id : %"PRIu64"\n", lr->ckptdptelr.dpte.page_id);
			printf("recLSN : "); print_uint256(lr->ckptdptelr.dpte.recLSN); printf("\n");
			return;
		}
		case CHECKPOINT_END :
		{
			printf("prev_log_record_LSN : "); print_uint256(lr->ckptelr.prev_log_record_LSN); printf("\n");
			printf("begin_LSN : "); print_uint256(lr->ckptelr.begin_LSN); printf("\n");
			return;
		}
		case USER_INFO :
		{
			printf("info : "); print_binary(lr->uilr.info, lr->uilr.info_size); printf("\n");
			return;
		}
	}
}

#include<wale/wale.h>

uint256 get_mini_transaction_id_for_log_record(const log_record* lr)
{
	switch(lr->type)
	{
		default :
			return INVALID_LOG_SEQUENCE_NUMBER;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			return lr->palr.mini_transaction_id;
		case PAGE_INIT :
			return lr->pilr.mini_transaction_id;
		case PAGE_SET_HEADER :
			return lr->pshlr.mini_transaction_id;
		case TUPLE_APPEND :
			return lr->talr.mini_transaction_id;
		case TUPLE_INSERT :
			return lr->tilr.mini_transaction_id;
		case TUPLE_UPDATE :
			return lr->tulr.mini_transaction_id;
		case TUPLE_DISCARD :
			return lr->tdlr.mini_transaction_id;
		case TUPLE_DISCARD_ALL :
			return lr->tdalr.mini_transaction_id;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			return lr->tdttlr.mini_transaction_id;
		case TUPLE_SWAP :
			return lr->tslr.mini_transaction_id;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			return lr->tueiplr.mini_transaction_id;
		case PAGE_CLONE :
			return lr->pclr.mini_transaction_id;
		case PAGE_COMPACTION :
			return lr->pcptlr.mini_transaction_id;
		case FULL_PAGE_WRITE :
			return lr->fpwlr.mini_transaction_id;
		case PAGE_INIT_CREATION :
			return lr->piclr.mini_transaction_id;
		case COMPENSATION_LOG :
			return lr->clr.mini_transaction_id;
		case ABORT_MINI_TX :
			return lr->amtlr.mini_transaction_id;
		case COMPLETE_MINI_TX :
			return lr->cmtlr.mini_transaction_id;
	}
}

int set_mini_transaction_id_for_log_record(log_record* lr, uint256 mini_transaction_id)
{
	switch(lr->type)
	{
		default :
			return 0;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			lr->palr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_INIT :
		{
			lr->pilr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_SET_HEADER :
		{
			lr->pshlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_APPEND :
		{
			lr->talr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_INSERT :
		{
			lr->tilr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_UPDATE :
		{
			lr->tulr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_DISCARD :
		{
			lr->tdlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_DISCARD_ALL :
		{
			lr->tdalr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			lr->tdttlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_SWAP :
		{
			lr->tslr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			lr->tueiplr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_CLONE :
		{
			lr->pclr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_COMPACTION :
		{
			lr->pcptlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case FULL_PAGE_WRITE :
		{
			lr->fpwlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case PAGE_INIT_CREATION :
		{
			lr->piclr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case COMPENSATION_LOG :
		{
			lr->clr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case ABORT_MINI_TX :
		{
			lr->amtlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
		case COMPLETE_MINI_TX :
		{
			lr->cmtlr.mini_transaction_id = mini_transaction_id;
			return 1;
		}
	}
}

uint256 get_prev_log_record_LSN_for_log_record(const log_record* lr)
{
	switch(lr->type)
	{
		default :
			return INVALID_LOG_SEQUENCE_NUMBER;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			return lr->palr.prev_log_record_LSN;
		case PAGE_INIT :
			return lr->pilr.prev_log_record_LSN;
		case PAGE_SET_HEADER :
			return lr->pshlr.prev_log_record_LSN;
		case TUPLE_APPEND :
			return lr->talr.prev_log_record_LSN;
		case TUPLE_INSERT :
			return lr->tilr.prev_log_record_LSN;
		case TUPLE_UPDATE :
			return lr->tulr.prev_log_record_LSN;
		case TUPLE_DISCARD :
			return lr->tdlr.prev_log_record_LSN;
		case TUPLE_DISCARD_ALL :
			return lr->tdalr.prev_log_record_LSN;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			return lr->tdttlr.prev_log_record_LSN;
		case TUPLE_SWAP :
			return lr->tslr.prev_log_record_LSN;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			return lr->tueiplr.prev_log_record_LSN;
		case PAGE_CLONE :
			return lr->pclr.prev_log_record_LSN;
		case PAGE_COMPACTION :
			return lr->pcptlr.prev_log_record_LSN;
		case FULL_PAGE_WRITE :
			return lr->fpwlr.prev_log_record_LSN;
		case PAGE_INIT_CREATION :
			return lr->piclr.prev_log_record_LSN;
		case COMPENSATION_LOG :
			return lr->clr.prev_log_record_LSN;
		case ABORT_MINI_TX :
			return lr->amtlr.prev_log_record_LSN;
		case COMPLETE_MINI_TX :
			return lr->cmtlr.prev_log_record_LSN;
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
			return lr->ckptmttelr.prev_log_record_LSN;
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
			return lr->ckptdptelr.prev_log_record_LSN;
		case CHECKPOINT_END :
			return lr->ckptelr.prev_log_record_LSN;
	}
}

int set_prev_log_record_LSN_for_log_record(log_record* lr, uint256 prev_log_record_LSN)
{
	switch(lr->type)
	{
		default :
			return 0;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
		{
			lr->palr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_INIT :
		{
			lr->pilr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_SET_HEADER :
		{
			lr->pshlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_APPEND :
		{
			lr->talr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_INSERT :
		{
			lr->tilr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_UPDATE :
		{
			lr->tulr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_DISCARD :
		{
			lr->tdlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_DISCARD_ALL :
		{
			lr->tdalr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
		{
			lr->tdttlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_SWAP :
		{
			lr->tslr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
		{
			lr->tueiplr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_CLONE :
		{
			lr->pclr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_COMPACTION :
		{
			lr->pcptlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case FULL_PAGE_WRITE :
		{
			lr->fpwlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case PAGE_INIT_CREATION :
		{
			lr->piclr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case COMPENSATION_LOG :
		{
			lr->clr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case ABORT_MINI_TX :
		{
			lr->amtlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case COMPLETE_MINI_TX :
		{
			lr->cmtlr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case CHECKPOINT_MINI_TRANSACTION_TABLE_ENTRY :
		{
			lr->ckptmttelr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case CHECKPOINT_DIRTY_PAGE_TABLE_ENTRY :
		{
			lr->ckptdptelr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
		case CHECKPOINT_END :
		{
			lr->ckptelr.prev_log_record_LSN = prev_log_record_LSN;
			return 1;
		}
	}
}

uint64_t get_page_id_for_log_record(const log_record* lr)
{
	switch(lr->type)
	{
		default :
			return 0;
		case PAGE_ALLOCATION :
		case PAGE_DEALLOCATION :
			return lr->palr.page_id;
		case PAGE_INIT :
			return lr->pilr.page_id;
		case PAGE_SET_HEADER :
			return lr->pshlr.page_id;
		case TUPLE_APPEND :
			return lr->talr.page_id;
		case TUPLE_INSERT :
			return lr->tilr.page_id;
		case TUPLE_UPDATE :
			return lr->tulr.page_id;
		case TUPLE_DISCARD :
			return lr->tdlr.page_id;
		case TUPLE_DISCARD_ALL :
			return lr->tdalr.page_id;
		case TUPLE_DISCARD_TRAILING_TOMB_STONES :
			return lr->tdttlr.page_id;
		case TUPLE_SWAP :
			return lr->tslr.page_id;
		case TUPLE_UPDATE_ELEMENT_IN_PLACE :
			return lr->tueiplr.page_id;
		case PAGE_CLONE :
			return lr->pclr.page_id;
		case PAGE_COMPACTION :
			return lr->pcptlr.page_id;
		case FULL_PAGE_WRITE :
			return lr->fpwlr.page_id;
		case PAGE_INIT_CREATION :
			return lr->piclr.page_id;
	}
}