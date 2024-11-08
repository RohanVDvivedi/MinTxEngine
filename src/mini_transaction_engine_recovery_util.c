#include<mini_transaction_engine_recovery_util.h>

#include<mini_transaction_engine_checkpointer_util.h>

checkpoint analyze(mini_transaction_engine* mte)
{
	// TODO
}

void redo(mini_transaction_engine* mte, checkpoint* ckpt)
{
	// TODO
}

void undo(mini_transaction_engine* mte)
{
	// TODO
}

void recover(mini_transaction_engine* mte)
{
	checkpoint ckpt = analyze(mte); // runs to generate the checkpoint for the last state of the mini_transaction_engine's mini transaction table and dirty page table at the time of crash
	redo(mte, &ckpt);				// consumes checkpoint and deinitizlizes it, and redos all log records from the minimum recLSN in the checkpoint
	undo(mte); 						// undos uncommitted log records
}