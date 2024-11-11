#include<tuple.h>

data_type_info digits_type_info;
data_type_info num_in_words_type_info;
data_type_info* record_type_info;
tuple_def record_def;

data_type_info* key_type_info;
tuple_def key_def;

#define RECORD_S_KEY_ELEMENT_COUNT 2
positional_accessor KEY_POS[2] = {STATIC_POSITION(0), STATIC_POSITION(2)};
compare_direction CMP_DIR[2] = {ASC, ASC};

void initialize_tuple_defs()
{
	record_type_info = malloc(sizeof_tuple_data_type_info(4));
	initialize_tuple_data_type_info(record_type_info, "record", 0, 900, 4);

	strcpy(record_type_info->containees[0].field_name, "num");
	record_type_info->containees[0].type_info = UINT_NON_NULLABLE[8];

	strcpy(record_type_info->containees[1].field_name, "order");
	record_type_info->containees[1].type_info = INT_NON_NULLABLE[1];

	num_in_words_type_info = get_variable_length_string_type("num_in_words", 70);
	strcpy(record_type_info->containees[2].field_name, "num_in_words");
	record_type_info->containees[2].type_info = &num_in_words_type_info;

	digits_type_info = get_variable_element_count_array_type("digits", 16, UINT_NON_NULLABLE[1]);
	strcpy(record_type_info->containees[3].field_name, "digits");
	record_type_info->containees[3].type_info = &digits_type_info;

	initialize_tuple_def(&record_def, record_type_info);

	key_type_info = malloc(sizeof_tuple_data_type_info(2));
	initialize_tuple_data_type_info(key_type_info, "key", 0, 900, 2);

	strcpy(key_type_info->containees[0].field_name, "num");
	key_type_info->containees[0].type_info = record_type_info->containees[0].type_info;

	strcpy(key_type_info->containees[1].field_name, "num_in_words");
	key_type_info->containees[1].type_info = record_type_info->containees[2].type_info;

	initialize_tuple_def(&key_def, key_type_info);

	print_tuple_def(&record_def);
	printf("\n\n");

	print_tuple_def(&key_def);
	printf("\n\n");
}

#define BUFFER_SIZE 300

const char *ones[] = {
  "zero", "one", "two", "three", "four", "five", "six", "seven",
  "eight", "nine", "ten", "eleven", "twelve", "thirteen", "fourteen",
  "fifteen", "sixteen", "seventeen", "eighteen", "nineteen"
};

const char *tens[] = {
  "", "", "twenty", "thirty", "forty", "fifty", "sixty", "seventy", "eighty", "ninety"
};

void num_in_words(char* output, uint16_t n) {
  if (n < 20) {
    strcpy(output, ones[n]);
  } else if (n < 100) {
    sprintf(output, "%s-%s", tens[(n / 10) % 10], ones[n % 10]);
  } else if (n < 1000) {
  	{
  		char temp[100];
  		num_in_words(temp, n % 100);
    	sprintf(output, "%s hundred %s", ones[(n / 100) % 10], temp);
	}
  } else {
  	strcpy(output, "TOO00-BIG");
  }
}

uint16_t find_order(uint64_t num, int order)
{
	switch(order)
	{
		case 0:
			return (num / 1ULL) % 1000;
		case 1:
			return (num / 1000ULL) % 1000;
		case 2:
			return (num / 1000000ULL) % 1000;
		case 3:
			return (num / 1000000000ULL) % 1000;
		case 4:
		{
			printf("ORDER TOO BIG\n");
			exit(-1);
		}
	}
	return 0;
}

void construct_record(void* buffer, uint64_t num, int order)
{
	init_tuple(&record_def, buffer);

	set_element_in_tuple(&record_def, STATIC_POSITION(0), buffer, &(user_value){.uint_value = num}, UINT32_MAX);

	uint16_t o = find_order(num, order);
	set_element_in_tuple(&record_def, STATIC_POSITION(1), buffer, &(user_value){.int_value = order}, UINT32_MAX);

	char temp[100];
	num_in_words(temp, o);
	set_element_in_tuple(&record_def, STATIC_POSITION(2), buffer, &(user_value){.string_value = temp, .string_size = strlen(temp)}, UINT32_MAX);

	set_element_in_tuple(&record_def, STATIC_POSITION(3), buffer, EMPTY_USER_VALUE, UINT32_MAX);
	uint32_t size = 0;
	while(num > 0)
	{
		uint8_t d = num % 10;
		if(!expand_element_count_for_element_in_tuple(&record_def, STATIC_POSITION(3), buffer, size, 1, UINT32_MAX))
			break;
		size++;
		set_element_in_tuple(&record_def, STATIC_POSITION(3,(size-1)), buffer, &(user_value){.uint_value = d}, UINT32_MAX);
		num = num / 10;
	}
}

int validate_record(const void* buffer)
{
	uint64_t num = get_value_from_element_from_tuple(&record_def, STATIC_POSITION(0), buffer).uint_value;

	int order = get_value_from_element_from_tuple(&record_def, STATIC_POSITION(1), buffer).int_value;

	uint16_t o = find_order(num, order);
	{
		char t1[1000];
		num_in_words(t1, o);
		const char* t2 = get_value_from_element_from_tuple(&record_def, STATIC_POSITION(2), buffer).string_value;
		if(strlen(t1) != get_value_from_element_from_tuple(&record_def, STATIC_POSITION(2), buffer).string_size)
			return 0;
		if(strncmp(t1, t2, strlen(t1)))
			return 0;
	}

	uint32_t index = 0;
	uint32_t size = get_element_count_for_element_from_tuple(&record_def, STATIC_POSITION(3), buffer);
	while(num)
	{
		uint8_t d = num % 10;
		if(index >= size)
			return 0;
		if(d != get_value_from_element_from_tuple(&record_def, STATIC_POSITION(3,index), buffer).uint_value)
			return 0;
		index++;
		num /= 10;
	}

	if(index < get_element_count_for_element_from_tuple(&record_def, STATIC_POSITION(3), buffer))
		return 0;

	return 1;
}

void construct_key(void* buffer, uint64_t num, int order)
{
	init_tuple(&key_def, buffer);

	set_element_in_tuple(&key_def, STATIC_POSITION(0), buffer, &(user_value){.uint_value = num}, UINT32_MAX);

	uint16_t o = find_order(num, order);
	char temp[100];
	num_in_words(temp, o);
	set_element_in_tuple(&key_def, STATIC_POSITION(1), buffer, &(user_value){.string_value = temp, .string_size = strlen(temp)}, UINT32_MAX);
}

void deinitialize_tuple_defs()
{
	free(key_type_info);
	free(record_type_info);
}