# MinTxEngine
A Mini Transaction Engine similar to the one found in the internals of MySQL. Supported by WALe and Bufferpool libraries.
It works on the tuples with layout as specified by the TupleStore library, fully compatible with the data structures like BplusTree, HashTable, LinkedPageList, ArrayTable, PageTable, and Sorter of TupleIndexer.

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
