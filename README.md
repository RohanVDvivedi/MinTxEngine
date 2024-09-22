# MinTxEngine
A Mini Transaction Engine similar to the one found in the internals of MySQL. Supported by WALe and Bufferpool libraries.

## Setup instructions
**Install dependencies :**
 * [WALe](https://github.com/RohanVDvivedi/WALe)
 * [Bufferpool](https://github.com/RohanVDvivedi/Bufferpool)

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/MinTxEngine.git`

**Build from source :**
 * `cd MinTxEngine`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lmintxengine` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<.h>`
   * `#include<.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd c_template_application`
 * `sudo make uninstall`
