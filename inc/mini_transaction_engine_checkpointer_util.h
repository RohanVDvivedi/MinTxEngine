#ifndef MINI_TRANSACTION_ENGINE_CHECKPOINTER_UTIL_H
#define MINI_TRANSACTION_ENGINE_CHECKPOINTER_UTIL_H

#include<hashtable.h>

typedef struct checkpoint checkpoint;
struct checkpoint
{
	hashtable mini_transaction_table; // mini transactions by mini transaction ids

	hashtable dirty_page_table; // dirty pages by page ids
};

// returns begin_LSN of the checkpoint
// it will also initialize ckpt
uint256 read_checkpoint_from_wal_UNSAFE(mini_transaction_engine* mte, uint256 checkpointLSN, checkpoint* ckpt);

// returns checkpoint end LSN and begin_LSN
uint256 append_checkpoint_to_wal_UNSAFE(mini_transaction_engine* mte, const checkpoint* ckpt, uint256* begin_LSN);

#endif