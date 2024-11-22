# MinTxEngine
A Mini Transaction Engine supported by WALe and Bufferpool libraries.
It works on the tuples with layout as specified by the TupleStore library, fully compatible with the data structures like BplusTree, HashTable, LinkedPageList, ArrayTable, PageTable, and Sorter of TupleIndexer.

The usage is simple, for any operaton that you want to be ACID compliant on the database but involves a set of page writes to be atomic to become consistent, then you wrap them into a mini transaction.
This mini transacion will be atomic keeping the databse consistent, and will be durable (if there was a flush anytime after it's completion), and upon an abort or a crash it will be rolled back.
These mini transactions are also crash recoverable with a modified version of ARIES algorithm, that runs at start up.

Usage : For instance, an insert into a bplus tree can be wrapped in a mini transacion to either make it go all the way through or fail and undo completely, This will need a mini transaction because a bplus tree insert may cause multiple splits and you would want all of these changes to persist or none of them to persist. This is what MinTxEngine solves. Another good example would be to perform a MVCC version switch atomically, which could involve inserting a new row in the table and pointing the old version's next pointer to this new row.

Concurrency level : You get latches on pages that you request for read or write, and the mini transaction engine will write lock them only upon an update. (You control latches on pages, the engine controls write locks on the pages). latches are released as soon as you release the latch on the page, but any acquired write lock on the page is released only at the end of the mini transaction, this avoids dirty reads and cascading aborts.

It is built on top of WALe (as write ahead logging library) and Bufferpool (as page cache with latches), acting on top of strict-typed-tuples and page layouts dictated by TupleStore, fully compatible with TupleIndexer's persistent data-structures, to provide ACID compliant, ARIES-like-fully-recoverable mini-transactions to build you next database storage engine.

If you plan to use it to build a transactional database storage engine, you will encompass each of a higher-level-transaction's small operations into concurrent individual mini transactions, but you will still need tuple, table and database level locks that span entire transactions, preferably with a MVCC read conditionals. NOTE :: mini transaction latches are released after the change/read is done and write locks on pages are released after mini transaction ends. So there is no way that a mini transaction can guarantee Isolation (I in ACID) for your higher level transactions (that possibly includes multiple mini transactions). A mini transaction provides only READ-COMMITTED isolation to individual mini transactions (, justifiable, as per the general isolation level jargon).

Also, There can be multiple mini transactions in flight performing their operations on several different threads, but a mini transaction itself is never thread safe, i.e. do not attempt to run a single mini transaction on more than 1 thread at a time. Additionally, mini transactions prevent write locks on pages from deadlocking using timeouts (this is crude, I know, but it is not a transaction engine, it is a mini transaction engine), but there is no provision of preventing latches from deadlocking (i.e. 2 write iterators on the leaf levels, with leaf-only traversals, traversing in opposite direction, may deadlock, so avoid doing this).

Contact me, you are intrigued by my work and want to collaborate on the next biggest database storage engine, built using this mini transaction engine.

Limitations:
 User level log records can not be more than 6 times the page size of the database.
 data_type_info's of the Tuples being used with this engine may not serialize to more than 4 times the page size of the database.

## Setup instructions
**Install dependencies :**
 * [TupleStore](https://github.com/RohanVDvivedi/TupleStore)
 * [WALe](https://github.com/RohanVDvivedi/WALe)
 * [Bufferpool](https://github.com/RohanVDvivedi/Bufferpool)
 * [BlockIO](https://github.com/RohanVDvivedi/BlockIO)
 * [SerializableInteger](https://github.com/RohanVDvivedi/SerializableInteger)
 * [ReaderWriterLock](https://github.com/RohanVDvivedi/ReaderWriterLock)
 * [Cutlery](https://github.com/RohanVDvivedi/Cutlery)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/MinTxEngine.git`

**Build from source :**
 * `cd MinTxEngine`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lmintxengine -ltuplestore -lwale -lz -lbufferpool -lblockio -lboompar -lrwlock -lserint -lcutlery -lpthread` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<mini_transaction_engine.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd c_template_application`
 * `sudo make uninstall`
