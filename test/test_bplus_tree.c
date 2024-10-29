#include<stdio.h>
#include<stdlib.h>

#include<mini_transaction_engine.h>
#include<callbacks_tupleindexer.h>

mini_transaction_engine mte;

#define SYSTEM_PAGE_SIZE 1024
#define PAGE_ID_WIDTH 4
#define LSN_WIDTH 4

#define BUFFERPOOL_BUFFERS 100
#define WALE_BUFFERS 10

#define LATCH_WAIT_TIMEOUT_US 3000
#define LOCK_WAIT_TIMEOUT_US 30000
#define CHECKPOINT_PERIOD_US (5 * 60 * 1000000) // 5 minutes

int main()
{
	if(!initialize_mini_transaction_engine(&mte, "testbpt.db", SYSTEM_PAGE_SIZE, PAGE_ID_WIDTH, LSN_WIDTH, BUFFERPOOL_BUFFERS, WALE_BUFFERS, LATCH_WAIT_TIMEOUT_US, LOCK_WAIT_TIMEOUT_US, CHECKPOINT_PERIOD_US))
	{
		printf("failed to initialize mini transaction engine\n");
		exit(-1);
	}
	init_pam_for_mini_tx_engine(&mte);
	//init_pmm_for_mini_tx_engine(&mte);
}