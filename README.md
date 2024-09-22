# c_template_application
A template for all of my c projects.

## Setup instructions

**Download source code :**
 * `git clone https://github.com/RohanVDvivedi/c_template_application.git`

**Build from source :**
 * `cd c_template_application`
 * `make clean all`

**Install from the build :**
 * `sudo make install`
 * ***Once you have installed from source, you may discard the build by*** `make clean`

## Using The library
 * add `-lcapp` linker flag, while compiling your application
 * do not forget to include appropriate public api headers as and when needed. this includes
   * `#include<capp.h>`
   * `#include<print_temp.h>`

## Instructions for uninstalling library

**Uninstall :**
 * `cd c_template_application`
 * `sudo make uninstall`
