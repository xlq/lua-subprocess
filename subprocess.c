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
#include "assert.h"

#define PIPE_ENV "subprocess_pipe_env"
#define SUBPROCESS_UDATA_META "subprocess_udata_metatable"
#define SUBPROCESS_META "subprocess_table_metatable"
#define tofilep(L)	((FILE **) luaL_checkudata(L, 1, LUA_FILEHANDLE))

/* special constants for popen arguments */
static char PIPE, STDOUT;

/* Checks to see if object at acceptable index is a file object.
   Returns file object if it is, or NULL if it is not. */
static FILE *isfilep(lua_State *L, int index)
{
    int iseq;
    FILE **fp;
    if (!lua_isuserdata(L, index)) return NULL;
    lua_getmetatable(L, index);
    luaL_getmetatable(L, LUA_FILEHANDLE);
    iseq = lua_rawequal(L, -1, -2);
    lua_pop(L, 2);
    if (!iseq) return NULL;
    fp = lua_touserdata(L, index);
    if (!fp) return NULL;
    return *fp;
}

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

/* Turn a FILE* object into a Lua pipe and push it on the stack */
static int ftopipe(lua_State *L, FILE *f)
{
    FILE **fp;

    fp = newfile(L);
    lua_getfield(L, LUA_REGISTRYINDEX, PIPE_ENV);
    lua_setfenv(L, -2);
    *fp = f;
    return 1;
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

struct fdinfo {
    enum {
        FDMODE_INHERIT = 0,  /* fd is inherited from parent */
        FDMODE_FILENAME,     /* open named file */
        FDMODE_FILEDES,      /* use a file descriptor */
        FDMODE_FILEOBJ,      /* use FILE* */
        FDMODE_PIPE,         /* create and use pipe */
        FDMODE_STDOUT        /* redirect to stdout (only for stderr) */
    } mode;
    union {
        char *filename;
        int filedes;
        FILE *fileobj;
    } info;
};

static void closefds(int *fds, int n)
{
    int i;
    for (i=0; i<n; ++i)
        if (fds[i] != -1)
            close(fds[i]);
}

static void closefiles(FILE **files, int n)
{
    int i;
    for (i=0; i<n; ++i)
        if (files[i] != NULL)
            fclose(files[i]);
}

static void freestrings(char **strs, int n)
{
    int i;
    for (i=0; i<n; ++i)
        if (strs[i] != NULL)
            free(strs[i]);
}

/* Function for opening subprocesses. Returns 0 on success and -1 on failure. */
static int dopopen(char *const *args,       /* program arguments with NULL sentinel */
                   const char *executable,  /* actual executable */
                   struct fdinfo fdinfo[3], /* info for stdin/stdout/stderr */
                   int close_fds,           /* 1 to close all fds */
                   const char *cwd,         /* working directory for program */
                   int *errno_out,          /* set on failure */
                   pid_t *pid_out,          /* set on success! */
                   FILE *pipe_ends_out[3]   /* pipe ends are put here */
                  )
{
    int fds[3];
    int i;
    struct fdinfo *fdi;
    int piperw[2];
    int errpipe[2]; /* pipe for returning error status */
    int flags;
    int en; /* saved errno */
    int count;
    pid_t pid;

    for (i=0; i<3; ++i)
        pipe_ends_out[i] = NULL;

    /* Manage stdin/stdout/stderr */
    for (i=0; i<3; ++i){
        fdi = &fdinfo[i];
        switch (fdi->mode){
            case FDMODE_INHERIT:
inherit:
                if ((fds[i] = dup(i)) == -1){
fd_failure:
                    *errno_out = errno;
                    closefds(fds, i);
                    closefiles(pipe_ends_out, i);
                    return -1;
                }
                break;
            case FDMODE_FILENAME:
                if (i == STDIN_FILENO){
                    if ((fds[i] = creat(fdi->info.filename, 0666)) == -1) goto fd_failure;
                } else {
                    if ((fds[i] = open(fdi->info.filename, O_RDONLY)) == -1) goto fd_failure;
                }
                break;
            case FDMODE_FILEDES:
                if ((fds[i] = dup(fdi->info.filedes)) == -1) goto fd_failure;
                break;
            case FDMODE_FILEOBJ:
                if ((fds[i] = dup(fileno(fdi->info.fileobj))) == -1) goto fd_failure;
                break;
            case FDMODE_PIPE:
                if (pipe(piperw) == -1) goto fd_failure;
                if (i == STDIN_FILENO){
                    fds[i] = piperw[0]; /* give read end to process */
                    if ((pipe_ends_out[i] = fdopen(piperw[1], "w")) == NULL) goto fd_failure;
                } else {
                    fds[i] = piperw[1]; /* give write end to process */
                    if ((pipe_ends_out[i] = fdopen(piperw[0], "r")) == NULL) goto fd_failure;
                }
                break;
            case FDMODE_STDOUT:
                if (i == STDERR_FILENO){
                    if ((fds[STDERR_FILENO] = dup(fds[STDOUT_FILENO])) == -1) goto fd_failure;
                } else goto inherit;
                break;
        }
    }
    
    /* Find executable name */
    if (!executable){
        /* use first arg */
        executable = args[0];
    }
    assert(executable != NULL);

    /* Create a pipe for returning error status */
    if (pipe(errpipe) == -1){
        *errno_out = errno;
        closefds(fds, 3);
        closefiles(pipe_ends_out, 3);
        return -1;
    }
    /* Make write end close on exec */
    flags = fcntl(errpipe[1], F_GETFD);
    if (flags == -1){
pipe_failure:
        *errno_out = errno;
        closefds(errpipe, 2);
        closefds(fds, 3);
        closefiles(pipe_ends_out, 3);
        return -1;
    }
    if (fcntl(errpipe[1], F_SETFD, flags | FD_CLOEXEC) == -1) goto pipe_failure;

    /* Do the fork/exec (TODO: use vfork somehow?) */
    pid = fork();
    if (pid == -1) goto pipe_failure;
    else if (pid == 0){
        /* child */
        close(errpipe[0]);
        
        /* dup file descriptors */
        for (i=0; i<3; ++i){
            if (dup2(fds[i], i) == -1) goto child_failure;
        }

        /* close other fds */
        if (close_fds){
            for (i=3; i<sysconf(_SC_OPEN_MAX); ++i){
                if (i != errpipe[1])
                    close(i);
            }
        }

        /* change directory */
        if (cwd && chdir(cwd)) goto child_failure;

        /* exec! Farewell, subprocess.c! */
        execvp(executable, args);

        /* Oh dear, we're still here. */
child_failure:
        en = errno;
        write(errpipe[1], &en, sizeof en);
        _exit(1);
    }

    /* parent */
    /* close unneeded fds */
    closefds(fds, 3);
    close(errpipe[1]);
    
    /* read errno from child */
    while ((count = read(errpipe[0], &en, sizeof en)) == -1)
        if (errno != EAGAIN && errno != EINTR) break;
    if (count > 0){
        /* exec failed */
        close(errpipe[0]);
        *errno_out = en;
        return -1;
    }
    close(errpipe[0]);

    /* Child is now running */
    *pid_out = pid;
    return 0;
}

/* popen {arg0, arg1, arg2, ..., [executable=...]} */
static int superpopen(lua_State *L)
{
    struct spudata *spup;

    /* List of arguments (malloc'd NULL-terminated array of malloc'd C strings) */
    int nargs = 0;
    char **args = NULL;
    /* Command to run (malloc'd) */
    char *executable = NULL;
    /* Directory to run it in */
    char *cwd = NULL;
    /* File options */
    struct fdinfo fdinfo[3];
    /* Close fds? */
    int close_fds = 0;

    int en = 0; /* errno copy */
    pid_t pid = -1; /* child's pid */
    FILE *pipe_ends[3] = {NULL, NULL, NULL};
    int i, j, result;
    struct stat statbuf;
    FILE *f;
    const char *s;
    
    /* get arguments */
    nargs = lua_objlen(L, 1);
    if (nargs == 0) return luaL_error(L, "no arguments specified");
    args = malloc((nargs + 1) * sizeof *args);
    if (!args) return luaL_error(L, "memory full");
    for (i=0; i<=nargs; ++i) args[i] = NULL;
    for (i=1; i<=nargs; ++i){
        lua_rawgeti(L, 1, i);
        s = lua_tostring(L, -1);
        if (!s){
            freestrings(args, nargs);
            free(args);
            return luaL_error(L, "popen argument %d not a string", (int) i);

        }
        args[i-1] = strdup(s);
        if (args[i-1] == NULL){
strings_failure:
            freestrings(args, nargs);
            free(args);
            return luaL_error(L, "memory full");
        }
        lua_pop(L, 1);
    }
    
    /* get executable string */
    lua_getfield(L, 1, "executable");
    s = lua_tostring(L, -1);
    if (s){
        executable = strdup(s);
        if (executable == NULL) goto strings_failure;
    }
    lua_pop(L, 1); /* to match lua_getfield */

    /* get directory name */
    lua_getfield(L, 1, "cwd");
    if (lua_isstring(L, -1)){
        cwd = strdup(lua_tostring(L, -1));
        if (!cwd){
            free(executable);
            freestrings(args, nargs);
            free(args);
            return luaL_error(L, "memory full");
        }
        /* make sure the cwd exists */
        if (stat(cwd, &statbuf)){
            free(executable);
            freestrings(args, nargs);
            free(args);
            return luaL_error(L, "cannot chdir to `%s': %s", cwd, strerror(errno));
        }
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
            fdinfo[i].mode = FDMODE_INHERIT;
        } else if (lua_touserdata(L, -1) == &PIPE){
            fdinfo[i].mode = FDMODE_PIPE;
        } else if (lua_touserdata(L, -1) == &STDOUT){
            if (i == STDERR_FILENO && fdinfo[STDOUT_FILENO].mode == FDMODE_PIPE){
                fdinfo[i].mode = FDMODE_STDOUT;
            } else {
                lua_pushstring(L, "STDOUT must be used only for stderr when stdout is set to PIPE");
files_failure:
                for (j=0; j<i; ++j){
                    if (fdinfo[j].mode == FDMODE_FILENAME)
                        free(fdinfo[j].info.filename);
                }
                free(executable);
                freestrings(args, nargs);
                free(args);
                return lua_error(L);
            }
        } else if (lua_isstring(L, -1)){
            /* open a file */
            fdinfo[i].mode = FDMODE_FILENAME;
            if ((fdinfo[i].info.filename = strdup(lua_tostring(L, -1))) == NULL){
                lua_pushstring(L, "out of memory");
                goto files_failure;
            }
        } else if (lua_isnumber(L, -1)){
            /* use this fd */
            fdinfo[i].mode = FDMODE_FILEDES;
            fdinfo[i].info.filedes = lua_tointeger(L, -1);
        } else {
            f = isfilep(L, -1);
            if (f){
                fdinfo[i].mode = FDMODE_FILEOBJ;
                fdinfo[i].info.fileobj = f;
            } else {
                /* huh? */
                lua_pushfstring(L, "unexpected value for %s", fd_names[i]);
                goto files_failure;
            }
        }
        lua_pop(L, 1);
    }

    result = dopopen(args, executable, fdinfo, close_fds, cwd, &en, &pid, pipe_ends);
    for (i=0; i<3; ++i)
        if (fdinfo[i].mode == FDMODE_FILENAME)
            free(fdinfo[i].info.filename);
    free(executable);
    freestrings(args, nargs);
    free(args);
    if (result == -1){
        /* failed */
        return luaL_error(L, "popen failed: %s", strerror(en));
    }

    /* Create popen object: it has two parts, the table part and the userdata part. */
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
    /* Set pid */
    lua_pushinteger(L, pid);
    lua_setfield(L, -2, "pid");
    /* Set pipe objects */
    for (i=0; i<3; ++i){
        if (pipe_ends[i]){
            ftopipe(L, pipe_ends[i]);
            lua_setfield(L, -2, fd_names[i]);
        }
    }
    /* Return the table */
    return 1;
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
