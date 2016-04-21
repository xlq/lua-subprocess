#include "liolib-copy.h"
#include "errno.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"

#define LUA_LIB
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"

static int pushresult(lua_State *L, int i, const char *filename)
{
    int en = errno;  /* calls to Lua API may change this value */
    if (i) {
        lua_pushboolean(L, 1);
        return 1;
    } else {
        lua_pushnil(L);
        if (filename)
            lua_pushfstring(L, "%s: %s", filename, strerror(en));
        else
            lua_pushfstring(L, "%s", strerror(en));
        lua_pushinteger(L, en);
        return 3;
    }
}

/* If SHARE_LIOLIB is defined, Lua's own FILE* metatable can be used. This is
   fine when the C runtime library used by Lua and by this module is the same,
   but FILE* objects are not compatible between different runtime libraries.
   If Lua was compiled with a different runtime library, DO NOT set SHARE_LIOLIB.
   In this case, copies of Lua's IO functions will be compiled in.
   If SHARE_LIOLIB is set and crashes ensue, turn it off! */
#ifndef SHARE_LIOLIB

#undef LUA_FILEHANDLE
#define LUA_FILEHANDLE "lio2_FILE*"

#define tofilep(L) ((FILE **)luaL_checkudata(L, 1, LUA_FILEHANDLE))

static FILE *tofile(lua_State *L)
{
    FILE **f = tofilep(L);
    if (*f == NULL)
        luaL_error(L, "attempt to use a closed file");
    return *f;
}

static int io_close(lua_State *L)
{
    FILE **p = tofilep(L);
    if (*p != NULL){
        int ok = (fclose(*p) == 0);
        *p = NULL;
        return pushresult(L, ok, NULL);
    } else {
        return 0;
    }
}

static int io_tostring(lua_State *L)
{
    FILE *f = *tofilep(L);
    if (f == NULL)
        lua_pushliteral(L, "file (closed)");
    else
        lua_pushfstring(L, "file (%p)", f);
    return 1;
}

static int io_readline(lua_State *L);

static void aux_lines(lua_State *L, int idx, int toclose)
{
    lua_pushvalue(L, idx);
    lua_pushboolean(L, toclose);  /* close/not close file when finished */
    lua_pushcclosure(L, io_readline, 2);
}

static int f_lines(lua_State *L)
{
    tofile(L);  /* check that it's a valid file handle */
    aux_lines(L, 1, 0);
    return 1;
}

/*
** {======================================================
** READ
** =======================================================
*/

static int read_number(lua_State *L, FILE *f)
{
    double d;
    if (fscanf(f, "%lf", &d) == 1) {
        lua_pushnumber(L, d);
        return 1;
    } else return 0;  /* read fails */
}

static int test_eof(lua_State *L, FILE *f)
{
    int c = getc(f);
    ungetc(c, f);
    lua_pushlstring(L, NULL, 0);
    return (c != EOF);
}

static int read_line(lua_State *L, FILE *f)
{
    luaL_Buffer b;
    luaL_buffinit(L, &b);
    for (;;) {
        size_t l;
        char *p = luaL_prepbuffer(&b);
        if (fgets(p, LUAL_BUFFERSIZE, f) == NULL) {  /* eof? */
            luaL_pushresult(&b);  /* close buffer */
            return (lua_objlen(L, -1) > 0);  /* check whether read something */
        }
        l = strlen(p);
        if (l == 0 || p[l-1] != '\n')
            luaL_addsize(&b, l);
        else {
            luaL_addsize(&b, l - 1);  /* do not include `eol' */
            luaL_pushresult(&b);  /* close buffer */
            return 1;  /* read at least an `eol' */
        }
    }
}

static int read_chars(lua_State *L, FILE *f, size_t n)
{
    size_t rlen;  /* how much to read */
    size_t nr;  /* number of chars actually read */
    luaL_Buffer b;
    luaL_buffinit(L, &b);
    rlen = LUAL_BUFFERSIZE;  /* try to read that much each time */
    do {
        char *p = luaL_prepbuffer(&b);
        if (rlen > n) rlen = n;  /* cannot read more than asked */
        nr = fread(p, sizeof(char), rlen, f);
        luaL_addsize(&b, nr);
        n -= nr;  /* still have to read `n' chars */
    } while (n > 0 && nr == rlen);  /* until end of count or eof */
    luaL_pushresult(&b);  /* close buffer */
    return (n == 0 || lua_objlen(L, -1) > 0);
}

static int g_read(lua_State *L, FILE *f, int first)
{
    int nargs = lua_gettop(L) - 1;
    int success;
    int n;
    clearerr(f);
    if (nargs == 0) {  /* no arguments? */
        success = read_line(L, f);
        n = first+1;  /* to return 1 result */
    } else {  /* ensure stack space for all results and for auxlib's buffer */
        luaL_checkstack(L, nargs+LUA_MINSTACK, "too many arguments");
        success = 1;
        for (n = first; nargs-- && success; n++) {
            if (lua_type(L, n) == LUA_TNUMBER) {
                size_t l = (size_t)lua_tointeger(L, n);
                success = (l == 0) ? test_eof(L, f) : read_chars(L, f, l);
            }
            else {
                const char *p = lua_tostring(L, n);
                luaL_argcheck(L, p && p[0] == '*', n, "invalid option");
                switch (p[1]) {
                case 'n':  /* number */
                    success = read_number(L, f);
                    break;
                case 'l':  /* line */
                    success = read_line(L, f);
                    break;
                case 'a':  /* file */
                    read_chars(L, f, ~((size_t)0));  /* read MAX_SIZE_T chars */
                    success = 1; /* always success */
                    break;
                default:
                    return luaL_argerror(L, n, "invalid format");
                }
            }
        }
    }
    if (ferror(f))
        return pushresult(L, 0, NULL);
    if (!success) {
        lua_pop(L, 1);  /* remove last result */
        lua_pushnil(L);  /* push nil instead */
    }
    return n - first;
}

static int f_read(lua_State *L) {
  return g_read(L, tofile(L), 2);
}

static int io_readline (lua_State *L)
{
    FILE *f = *(FILE **)lua_touserdata(L, lua_upvalueindex(1));
    int sucess;
    if (f == NULL)  /* file is already closed? */
        luaL_error(L, "file is already closed");
    sucess = read_line(L, f);
    if (ferror(f))
        return luaL_error(L, "%s", strerror(errno));
    if (sucess) return 1;
    else {  /* EOF */
        if (lua_toboolean(L, lua_upvalueindex(2))) {  /* generator created file? */
            lua_settop(L, 0);
            lua_pushvalue(L, lua_upvalueindex(1));
            io_close(L);  /* close it */
        }
        return 0;
    }
}

/* }====================================================== */


static int g_write(lua_State *L, FILE *f, int arg)
{
    int nargs = lua_gettop(L) - 1;
    int status = 1;
    for (; nargs--; arg++) {
        if (lua_type(L, arg) == LUA_TNUMBER) {
          /* optimization: could be done exactly as for strings */
            status = status &&
              fprintf(f, LUA_NUMBER_FMT, lua_tonumber(L, arg)) > 0;
        } else {
            size_t l;
            const char *s = luaL_checklstring(L, arg, &l);
            status = status && (fwrite(s, sizeof(char), l, f) == l);
        }
    }
    return pushresult(L, status, NULL);
}

static int f_write(lua_State *L)
{
    return g_write(L, tofile(L), 2);
}

static int f_seek(lua_State *L)
{
    static const int mode[] = {SEEK_SET, SEEK_CUR, SEEK_END};
    static const char *const modenames[] = {"set", "cur", "end", NULL};
    FILE *f = tofile(L);
    int op = luaL_checkoption(L, 2, "cur", modenames);
    long offset = luaL_optinteger(L, 3, 0);
    op = fseek(f, offset, mode[op]);
    if (op)
        return pushresult(L, 0, NULL);  /* error */
    else {
        lua_pushinteger(L, ftell(f));
        return 1;
    }
}

static int f_setvbuf(lua_State *L)
{
    static const int mode[] = {_IONBF, _IOFBF, _IOLBF};
    static const char *const modenames[] = {"no", "full", "line", NULL};
    FILE *f = tofile(L);
    int op = luaL_checkoption(L, 2, NULL, modenames);
    lua_Integer sz = luaL_optinteger(L, 3, LUAL_BUFFERSIZE);
    int res = setvbuf(f, NULL, mode[op], sz);
    return pushresult(L, res == 0, NULL);
}

static int f_flush(lua_State *L)
{
    return pushresult(L, fflush(tofile(L)) == 0, NULL);
}

static const luaL_Reg flib[] = {
    {"close", io_close},
    {"flush", f_flush},
    {"lines", f_lines},
    {"read", f_read},
    {"seek", f_seek},
    {"setvbuf", f_setvbuf},
    {"write", f_write},
    {"__gc", io_close},
    {"__tostring", io_tostring},
    {NULL, NULL}
};

static void createmeta(lua_State *L)
{
    luaL_newmetatable(L, LUA_FILEHANDLE);  /* create metatable for file handles */
    lua_pushvalue(L, -1);  /* push metatable */
    lua_setfield(L, -2, "__index");  /* metatable.__index = metatable */
#if LUA_VERSION_NUM >= 502
    luaL_setfuncs(L, flib, 0);
#else
    luaL_register(L, NULL, flib);  /* file methods */
#endif
}

#else /* #ifndef SHARE_LIOLIB */

static int io_fclose(lua_State *L)
{
    FILE **p = luaL_checkudata(L, 1, LUA_FILEHANDLE);
    int ok = (fclose(*p) == 0);
    *p = NULL;
    return pushresult(L, ok, NULL);
}

#endif /* #ifndef SHARE_LIOLIB */

FILE *liolib_copy_tofile(lua_State *L, int index)
{
    int eq;
    if (lua_type(L, index) != LUA_TTABLE) return NULL;
    lua_getmetatable(L, index);
    luaL_getmetatable(L, LUA_FILEHANDLE);
    eq = lua_equal(L, -2, -1);
    lua_pop(L, 2);
    if (!eq) return NULL;
    return *(FILE **) lua_touserdata(L, index);
}

/*
** When creating file handles, always creates a `closed' file handle
** before opening the actual file; so, if there is a memory error, the
** file is not left opened.
*/
FILE **liolib_copy_newfile(lua_State *L)
{
    FILE **pf = (FILE **)lua_newuserdata(L, sizeof(FILE *));
    *pf = NULL;  /* file handle is currently `closed' */
    luaL_getmetatable(L, LUA_FILEHANDLE);
#ifdef SHARE_LIOLIB
    /* create environment for the file */
    lua_createtable(L, 0, 1);
    lua_pushcfunction(L, io_fclose);
    lua_setfield(L, -2, "__close");
    lua_setfenv(L, -3);
#else
    /* create metatable as needed */
    if (lua_isnil(L, -1)){
        lua_pop(L, 1);
        createmeta(L);
    }
#endif
    lua_setmetatable(L, -2);
    /* leave file object on stack */
    return pf;
}
