# MinTxEngine
A Mini Transaction Engine similar to the one found in the internals of MySQL. Supported by WALe and Bufferpool libraries.

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
 * add `-lmintxengine -ltuplestore -lwale -lbufferpool -lrwlock -lserint -lcutlery -lpthread` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<mini_transaction_engine.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd c_template_application`
 * `sudo make uninstall`
