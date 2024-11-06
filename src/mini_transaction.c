#include<mini_transaction.h>

int compare_mini_transactions(const void* mt1, const void* mt2)
{
	return compare_uint256(((const mini_transaction*)mt1)->mini_transaction_id, ((const mini_transaction*)mt2)->mini_transaction_id);
}

cy_uint hash_mini_transaction(const void* mt)
{
	return ((const mini_transaction*)mt)->mini_transaction_id.limbs[0];
}

#include<stdlib.h>

mini_transaction* get_new_mini_transaction()
{
	mini_transaction* mt = malloc(sizeof(mini_transaction));
	if(mt == NULL)
	{
		printf("ISSUE :: unable to allocate memory for mini transaction\n");
		exit(-1);
	}
	pthread_cond_init(&(mt->write_lock_wait), NULL);
	mt->reference_counter = 0;
	initialize_llnode(&(mt->enode));
	return mt;
}

void delete_mini_transaction(mini_transaction* mt)
{
	pthread_cond_destroy(&(mt->write_lock_wait));
	free(mt);
}

#include<wale.h>

uint256 get_minimum_mini_transaction_id_for_mini_transaction_table(const hashmap* mini_transaction_table)
{
	uint256 min_mini_tx_id = INVALID_LOG_SEQUENCE_NUMBER;

	for(const mini_transaction* mt = get_first_of_in_hashmap(mini_transaction_table, FIRST_OF_HASHMAP); mt != NULL; mt = get_next_of_in_hashmap(mini_transaction_table, mt, ANY_IN_HASHMAP))
	{
		if(are_equal_uint256(min_mini_tx_id, INVALID_LOG_SEQUENCE_NUMBER))
			min_mini_tx_id = mt->mini_transaction_id;
		else
			min_mini_tx_id = min_uint256(min_mini_tx_id, mt->mini_transaction_id);
	}

	return min_mini_tx_id;
}