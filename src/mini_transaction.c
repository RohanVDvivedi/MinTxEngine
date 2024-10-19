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
		exit(-1);
	pthread_cond_init(&(mt->write_lock_wait), NULL);
	mt->waiters_count = 0;
	initialize_llnode(&(mt->enode));
	return mt;
}

void delete_mini_transaction(mini_transaction* mt)
{
	pthread_cond_destroy(&(mt->write_lock_wait));
	free(mt);
}