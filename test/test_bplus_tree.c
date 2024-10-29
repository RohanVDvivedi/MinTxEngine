#include<stdio.h>
#include<stdlib.h>

#include<mini_transaction_engine.h>
#include<callbacks_tupleindexer.h>

int main()
{
	init_pam_for_mini_tx_engine(mte);
	init_pmm_for_mini_tx_engine(mte);
}