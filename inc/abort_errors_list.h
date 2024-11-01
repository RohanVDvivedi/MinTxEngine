#ifndef ABORT_ERRORS_LIST_H
#define ABORT_ERRORS_LIST_H

/*
	all abort errors must be negative
*/

#define ILLEGAL_PAGE_ID -1
#define OUT_OF_BUFFERPOOL_MEMORY -2
#define PLAUSIBLE_DEADLOCK -3
#define UNABLE_TO_TRANSITION_LOCK -4
#define PAGE_TO_BE_FREED_IS_LOCKED -5
#define OUT_OF_AVAILABLE_PAGE_IDS -6
#define ABORTED_AFTER_CRASH -7

#endif