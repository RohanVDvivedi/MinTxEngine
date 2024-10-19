#include<mini_transaction.h>

int compare_mini_transactions(const void* mt1, const void* mt2)
{
	return compare_uint256(((const mini_transaction*)mt1)->mini_transaction_id, ((const mini_transaction*)mt2)->mini_transaction_id);
}

cy_uint hash_mini_transaction(const void* mt)
{
	return ((const mini_transaction*)mt)->mini_transaction_id.limbs[0];
}