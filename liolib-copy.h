#ifndef LIOLIB_COPY_H
#define LIOLIB_COPY_H

#include "lua.h"
#include "stdio.h"

FILE *liolib_copy_tofile(lua_State *L, int index);
FILE **liolib_copy_newfile(lua_State *L);

#if LUA_VERSION_NUM >= 502
/* lua_equal deprecated in favour of lua_compare */
#define lua_equal(L,a,b) (lua_compare((L), (a), (b), LUA_OPEQ))
/* lua_setfenv/lua_getfenv are gone from Lua 5.2 onwards.
   We can use lua_setuservalue/lua_getuservalue for userdata only
   (which is the only thing we use it for here). */
#define lua_getfenv(L,i) (lua_getuservalue((L), (i)))
#define lua_setfenv(L,i) (lua_setuservalue((L), (i)))
/* lua_objlen renamed to lua_rawlen */
#define lua_objlen(L,i) (lua_rawlen((L), (i)))

#define LUA_NUMBER_SCAN 
#endif

#endif
