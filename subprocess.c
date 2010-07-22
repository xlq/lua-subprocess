/* Copyright (c) 2010 Joshua Phillips
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#define LUA_LIB
#include "lua.h"
#include "lualib.h"
#include "lauxlib.h"
#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "unistd.h"
#include "fcntl.h"
#include "errno.h"
#include "sys/wait.h"
#include "sys/stat.h"

#define PIPE_ENV "subprocess_pipe_env"
#define SUBPROCESS_UDATA_META "subprocess_udata_metatable"
#define SUBPROCESS_META "subprocess_table_metatable"
#define tofilep(L)	((FILE **) luaL_checkudata(L, 1, LUA_FILEHANDLE))

/* special constants for popen arguments */
static char PIPE, STDOUT;

/* Creates new Lua file object.
   Leaves object on top of stack.
   Returns FILE** that can be set. */
static FILE **newfile(lua_State *L)
{
    FILE **fp = lua_newuserdata(L, sizeof(FILE *));
    *fp = NULL;
    luaL_getmetatable(L, LUA_FILEHANDLE);
    lua_setmetatable(L, -2);
    return fp;
}

/* Turn a file descriptor into a lovely Lua file object.
   Return value:
     1:  success, file object is on top of stack
     0:  failure, error message is on top of stack */
static int superfdopen(lua_State *L, int fd, const char *mode)
{
    FILE **fp;
    int en;

    fp = newfile(L);
    lua_getfield(L, LUA_REGISTRYINDEX, PIPE_ENV);
    lua_setfenv(L, -2);
    *fp = fdopen(fd, mode);
    if (!*fp){
        en = errno;
        lua_pop(L, 1);
        lua_pushfstring(L, "fdopen: %s", strerror(en));
        return 0;
    } else {
        return 1;
    }
}

static int close_pipe(lua_State *L)
{
    int ok, err;
    FILE **p;
    
    p = tofilep(L);
    ok = fclose(*p);
    *p = NULL;
    if (ok){
        lua_pushboolean(L, 1);
        return 1;
    } else {
        err = errno;
        lua_pushnil(L);
        lua_pushfstring(L, "(pipe): %s", strerror(err));
        lua_pushinteger(L, err);
        return 3;
    }
}

/* Structure for subprocess userdata */
struct spudata {
    pid_t pid;
    int exitcode;
    unsigned char done;
};

static const char *fd_names[3] = {"stdin", "stdout", "stderr"};

/* popen {arg0, arg1, arg2, ..., [executable=...]} */
static int superpopen(lua_State *L)
{
    /* List of arguments (malloc'd NULL-terminated array of malloc'd C strings) */
    size_t nargs = 0;
    char **args = NULL;
    /* Command to run (malloc'd) */
    char *command = NULL;
    /* Directory to run it in */
    char *cwd = NULL;
    /* pipe to pass error info from child to parent */
    int errpipe[2];
    /* files for stdin, stdout, stderr of child process */
    int fds[3];
    /* other ends of pipes */
    int pipes[3];
    
    /* Each of these corresponds to a variable above.
       If set to one, the relevant
       file descriptor is closed on error. */
    unsigned char errpipe_set[2] = {0, 0};
    unsigned char fds_set[3] = {0, 0, 0},
                  pipes_set[3] = {0, 0, 0};

    /* Close fd in parent after fork? */
    unsigned char fds_close[3] = {1, 1, 1};

    unsigned char close_fds;
    const char *s;
    pid_t pid;
    int flags, en, count;
    size_t i;
    struct spudata *spup;
    int p[2];
    struct stat statbuf;

    luaL_checktype(L, 1, LUA_TTABLE);
    /* get arguments */
    nargs = lua_objlen(L, 1);
    args = malloc((nargs + 1) * sizeof(char *));
    if (!args){
        lua_pushstring(L, "memory full");
        goto fail;
    }
    for (i=0; i<=nargs; ++i) args[i] = NULL;
    for (i=1; i<=nargs; ++i){
        lua_rawgeti(L, 1, i);
        s = lua_tolstring(L, -1, NULL);
        if (!s){
            lua_pushfstring(L, "popen argument %d not a string", (int) i);
            goto fail;
        }
        args[i-1] = strdup(s);
        lua_pop(L, 1);
    }
    
    /* get command string */
    lua_getfield(L, 1, "executable");
    if (!lua_isnil(L, -1)){
        /* use given string as command */
        s = lua_tolstring(L, -1, NULL);
        if (!s) goto use_args0;
        command = strdup(s);
        if (!command){
            lua_pushstring(L, "memory full");
            goto fail;
        }
    } else {
use_args0:
        /* use args[0] as command */
        if (nargs > 0){
            command = strdup(args[0]);
            if (!command){
                lua_pushstring(L, "memory full");
                goto fail;
            }
        } else {
            lua_pushstring(L, "no command or arguments specified");
            goto fail;
        }
    }
    lua_pop(L, 1); /* to match lua_getfield */

    /* get directory name */
    lua_getfield(L, 1, "cwd");
    if (lua_isstring(L, -1)){
        cwd = strdup(lua_tostring(L, -1));
        /* Does the dir exist? */
        if (stat(cwd, &statbuf)){
            lua_pushfstring(L, "Cannot chdir to %s: %s", cwd, strerror(errno));
            goto fail;
        }
    } else if (!lua_isnil(L, -1)){
        lua_pushfstring(L, "string expected for cwd (got %s)", lua_typename(L, lua_type(L, -1)));
        goto fail;
    }
    lua_pop(L, 1);

    /* close_fds */
    lua_getfield(L, 1, "close_fds");
    close_fds = lua_toboolean(L, -1);
    lua_pop(L, 1);

    /* handle stdin/stdout/stderr */
    for (i=0; i<3; ++i){
        lua_getfield(L, 1, fd_names[i]);
        if (lua_isnil(L, -1)){
            /* not specified; boring */
        } else if (lua_touserdata(L, -1) == &PIPE){
            /* create a new pipe */
            if (pipe(p)) goto errno_fail;
            if (i == 0){
                /* stdin */
                fds[0] = p[0];
                pipes[0] = p[1];
                fds_set[0] = 1;
                pipes_set[0] = 1;
            } else {
                /* stdout/stderr */
                fds[i] = p[1];
                pipes[i] = p[0];
                fds_set[i] = 1;
                pipes_set[i] = 1;
            }
        } else if (lua_touserdata(L, -1) == &STDOUT){
            if (i == 2 && pipes_set[1]){
                /* send stderr to stdout */
                fds[2] = fds[1];
                fds_set[2] = 1;
            } else {
                lua_pushstring(L, "only set stderr to STDOUT, and only when stdout=PIPE");
                goto fail;
            }
        } else if (lua_isstring(L, -1)){
            /* open a file */
            s = lua_tostring(L, -1);
            if (s){
                if (i == 0){
                    p[0] = open(s, O_RDONLY);
                } else {
                    p[0] = open(s, O_WRONLY);
                }
                if (!p[0]){
                    /* XXX: can I give lua_pushfstring a string from lua_tostring? */
                    lua_pushfstring(L, "cannot open %s: %s", s, strerror(errno));
                    goto fail;
                }
                fds[i] = p[0];
                fds_set[i] = 1;
            } else {
                /* shrug */
            }
        } else if (lua_isnumber(L, -1)){
            /* use this fd */
            fds[i] = lua_tointeger(L, -1);
            fds_set[i] = 1;
            fds_close[i] = 0; /* do not close it */
        } else {
            /* huh? */
            lua_pushfstring(L, "unexpected value for %s", fd_names[i]);
            goto fail;
        }
        lua_pop(L, 1);
    }


    /* create a pipe for returning error status */
    if (pipe(errpipe)){
errno_fail:
        lua_pushstring(L, strerror(errno));
        goto fail;
    }
    errpipe_set[0] = 1;
    errpipe_set[1] = 1;
    flags = fcntl(errpipe[1], F_GETFD);
    if (flags == -1) goto errno_fail;
    if (fcntl(errpipe[1], F_SETFD, flags | FD_CLOEXEC)) goto errno_fail;

    /* do the fork/exec (TODO: use vfork?) */
    pid = fork();
    if (pid == -1) goto errno_fail;
    else if (pid == 0){
        /* child */
        close(errpipe[0]);
        /* dup file descriptors */
        for (i=0; i<3; ++i){
            if (fds_set[i])
                dup2(fds[i], i);
        }
        /* close other fds */
        if (close_fds){
            for (i=3; i<(unsigned)sysconf(_SC_OPEN_MAX); ++i)
                close(i);
        }
        /* change directory */
        if (cwd && chdir(cwd)){
            en = errno;
            write(errpipe[1], &en, sizeof(int));
            _exit(1);
        }
        /* exec! Farewell, process state. */
        execvp(command, args);
        /* Oh dear, we're still here. */
        en = errno;
        write(errpipe[1], &en, sizeof(int));
        _exit(1);
    }
    /* parent */
    /* close unneeded fds */
    close(errpipe[1]);
    errpipe_set[1] = 0;
    if (fds_set[1] && fds_set[2] && fds[1] == fds[2])
        fds_set[2] = 0;
    for (i=0; i<3; ++i){
        if (fds_set[i]){
            if (fds_close[i])
                close(fds[i]);
            fds_set[i] = 0;
        }
    }
    /* free unneeded info */
    free(cwd);
    free(command);
    command = NULL;
    for (i=0; i<nargs; ++i) free(args[i]);
    free(args);
    args = NULL;

    /* read errno from child */
    while ((count = read(errpipe[0], &en, sizeof(int))) == -1)
        if (errno != EAGAIN && errno != EINTR) break;
    if (count > 0){
        /* exec failed */
        close(errpipe[0]);
        lua_pushnil(L);
        lua_pushstring(L, strerror(en));
        lua_pushinteger(L, en);
        return 3;
    }
    close(errpipe[0]);
    errpipe_set[0] = 0;

    /* Child is now running. */
    /* Create table for subprocess info */
    lua_createtable(L, 0, 2);
    luaL_getmetatable(L, SUBPROCESS_META);
    lua_setmetatable(L, -2);
    /* Create userdata */
    spup = lua_newuserdata(L, sizeof(struct spudata));
    luaL_getmetatable(L, SUBPROCESS_UDATA_META);
    lua_setmetatable(L, -2);
    spup->pid = pid;
    spup->done = 0;
    /* Put userdata in table */
    lua_setfield(L, -2, "_subprocess");
    lua_pushinteger(L, pid);
    lua_setfield(L, -2, "pid");
    for (i=0; i<3; ++i){
        if (pipes_set[i]){
            if (i==2 && pipes_set[1] && pipes[1] == pipes[2]){
                lua_getfield(L, -1, "stdout");
                lua_setfield(L, -2, "stderr");
            } else {
                if (!superfdopen(L, pipes[i], (i==0)?"w":"r")){
                    /* not much we can do; just close it */
                    close(pipes[i]);
                    fprintf(stderr, "fdopen failed after fork and exec: %s\n", lua_tostring(L, -1));
                    lua_pop(L, 1); /* pop error message */
                } else {
                    lua_setfield(L, -2, fd_names[i]);
                }
            }
        }
    }

    /* Return the table */
    return 1;
fail:
    if (fds_set[1] && fds_set[2] && fds[1] == fds[2])
        fds_set[2] = 0;
    if (pipes_set[1] && pipes_set[2] && pipes[1] == pipes[2])
        pipes_set[2] = 0;
    for (i=0; i<3; ++i){
        if (fds_set[i]) close(fds[i]);
        if (pipes_set[i]) close(pipes[i]);
    }
    for (i=0; i<2; ++i)
        if (errpipe_set[i])
            close(errpipe[i]);
    free(command);
    free(cwd);
    for (i=0; i<nargs; ++i) free(args[i]);
    free(args);
    return lua_error(L);
}

static inline struct spudata *checksp(lua_State *L)
{
    struct spudata *spup;
    luaL_checktype(L, 1, LUA_TTABLE);
    lua_getfield(L, 1, "_subprocess");
    spup = luaL_checkudata(L, -1, SUBPROCESS_UDATA_META);
    lua_pop(L, 1);
    return spup;
}

static int sp_tostring(lua_State *L)
{
    struct spudata *spup = checksp(L);
    lua_pushfstring(L, "subprocess (%d)", (int) spup->pid);
    return 1;
}

/* Do a waitpid (poll or wait) */
static int sp_waitpid(lua_State *L, struct spudata *spup, int wait)
{
    int stat, options;
    if (spup->done){
        lua_pushinteger(L, spup->exitcode);
        return 1;
    }
    if (wait) options = 0;
    else options = WNOHANG;
    switch (waitpid(spup->pid, &stat, options)){
        case -1:
            return luaL_error(L, strerror(errno));
        case 0:
            /* child still running */
            lua_pushnil(L);
            return 1;
        default:
            if (WIFEXITED(stat)){
                /* child terminated */
                spup->exitcode = WEXITSTATUS(stat);
doret:
                spup->done = 1;
                lua_pushinteger(L, spup->exitcode);
                lua_pushvalue(L, -1);
                lua_setfield(L, -3, "exitcode");
                return 1;
            } else if (WIFSIGNALED(stat)){
                /* child died on a signal */
                spup->exitcode = -WTERMSIG(stat);
                goto doret;
            } else if (WIFSTOPPED(stat)){
                spup->exitcode = -WSTOPSIG(stat);
                goto doret;
            } else {
                /* ??? */
                spup->exitcode = 1;
                spup->done = 1;
                lua_pushstring(L, "disappeared into black hole");
                lua_pushvalue(L, -1);
                lua_setfield(L, -3, "exitcode");
                return 1;
            }
    }
}

static int sp_poll(lua_State *L)
{
    return sp_waitpid(L, checksp(L), 0);
}

static int sp_wait(lua_State *L)
{
    return sp_waitpid(L, checksp(L), 1);
}

static int sp_send_signal(lua_State *L)
{
    struct spudata *spup = checksp(L);
    int sig = luaL_checkint(L, 2);
    if (!spup->done){
        if (kill(spup->pid, sig)){
            return luaL_error(L, "kill: %s", strerror(errno));
        }
        spup->done = 1;
        spup->exitcode = -sig;
        lua_pushvalue(L, 1);
        lua_pushinteger(L, -sig);
        lua_setfield(L, -2, "exitcode");
        lua_pop(L, 1);
    }
    return 0;
}

static int sp_terminate(lua_State *L)
{
    lua_settop(L, 1);
    lua_pushinteger(L, SIGTERM);
    return sp_send_signal(L);
}

static int sp_kill(lua_State *L)
{
    lua_settop(L, 1);
    lua_pushinteger(L, SIGKILL);
    return sp_send_signal(L);
}

static const luaL_Reg subprocess_meta[] = {
    {"__tostring", sp_tostring},
    {"poll", sp_poll},
    {"wait", sp_wait},
    {"send_signal", sp_send_signal},
    {"terminate", sp_terminate},
    {"kill", sp_kill},
    {NULL, NULL}
};

/* convenience functions */
static int call(lua_State *L)
{
    int r = superpopen(L);
    if (r != 1){
        return r;
    }
    return sp_wait(L);
}

static int call_capture(lua_State *L)
{
    int r;
    lua_settop(L, 1);
    luaL_checktype(L, 1, LUA_TTABLE);
    lua_getfield(L, 1, "stdout");
    lua_pushlightuserdata(L, &PIPE);
    lua_setfield(L, 1, "stdout");
    r = superpopen(L);
    if (r != 1) return r;
    /* stack: args oldstdout sp */
    /* restore old stdout value in table */
    lua_pushvalue(L, 2);
    lua_setfield(L, 1, "stdout");
    lua_replace(L, 1);
    lua_settop(L, 1);
    /* stack: sp */
    lua_getfield(L, 1, "stdout");
    lua_getfield(L, 2, "read");
    lua_pushvalue(L, 2);
    lua_pushstring(L, "*a");
    lua_call(L, 2, 2);
    /* stack: sp stdout a b */
    /* close stdout, rather than relying on GC */
    lua_getfield(L, 2, "close");
    lua_pushvalue(L, 2);
    lua_call(L, 1, 0);
    /* wait for child (to avoid leaving a zombie) */
    lua_getfield(L, 1, "wait");
    lua_pushvalue(L, 1);
    lua_call(L, 1, 1);
    /* return exitcode, content */
    lua_pushvalue(L, 3);
    return 2;
}

/* Miscellaneous */

static int superwait(lua_State *L)
{
    int stat;
    pid_t pid = wait(&stat);
    if (pid == -1) return luaL_error(L, strerror(errno));
    lua_pushinteger(L, pid);
    if (WIFEXITED(stat)){
        /* child terminated */
        lua_pushinteger(L, WEXITSTATUS(stat));
    } else if (WIFSIGNALED(stat)){
        /* child died on a signal */
        lua_pushinteger(L, -WTERMSIG(stat));
    } else if (WIFSTOPPED(stat)){
        lua_pushinteger(L, -WSTOPSIG(stat));
    } else {
        /* ??? */
        lua_pushstring(L, "disappeared into black hole");
    }
    return 2;
}

static const luaL_Reg subprocess[] = {
    /* {"pipe", superpipe}, */
    {"popen", superpopen},
    {"call", call},
    {"call_capture", call_capture},
    {"wait", superwait},
    {NULL, NULL}
};

LUALIB_API int luaopen_subprocess(lua_State *L)
{
    luaL_register(L, "subprocess", subprocess);
    lua_pushlightuserdata(L, &PIPE);
    lua_setfield(L, -2, "PIPE");
    lua_pushlightuserdata(L, &STDOUT);
    lua_setfield(L, -2, "STDOUT");
    
    /* create environment for pipes */
    lua_createtable(L, 0, 1);
    lua_pushcfunction(L, close_pipe);
    lua_setfield(L, -2, "__close");
    lua_setfield(L, LUA_REGISTRYINDEX, PIPE_ENV);

    /* create metatable for subprocesses' userdata */
    luaL_newmetatable(L, SUBPROCESS_UDATA_META);
    lua_pushnil(L);
    lua_setfield(L, -2, "__metatable");
    /* TODO: set __gc functiotn? */
    lua_pop(L, 1);

    /* create metatable for subprocesses */
    luaL_newmetatable(L, SUBPROCESS_META);
    lua_pushvalue(L, -1);
    lua_setfield(L, -2, "__index");
    lua_pushnil(L);
    lua_setfield(L, -2, "__metatable");
    luaL_register(L, NULL, subprocess_meta);
    lua_pop(L, 1);

    return 1;
}
