#ifndef LIOLIB_COPY_H
#define LIOLIB_COPY_H

#include "lua.h"
#include "stdio.h"

FILE *liolib_copy_tofile(lua_State *L, int index);
FILE **liolib_copy_newfile(lua_State *L);

#endif
