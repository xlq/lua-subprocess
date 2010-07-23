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

#if !defined(OS_WINDOWS) && !defined(OS_POSIX)
#error None of these are defined: OS_WINDOWS, OS_POSIX
#endif

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
#include "assert.h"
#if defined(OS_POSIX)
#include "sys/wait.h"
#include "sys/stat.h"
typedef int filedes_t;
struct child_info {
    pid_t pid;
    int exitcode;
    unsigned char done; /* set to 1 when child has finished and closed. */
};
static int direxists(const char *fname)
{
    struct stat statbuf;
    if (stat(fname, &statbuf)){
        return 0;
    }
    return !!S_ISDIR(statbuf.st_mode);
}

#elif defined(OS_WINDOWS)
#include "windows.h"
typedef HANDLE filedes_t;
struct child_info {
    HANDLE hProcess;
    DWORD pid;
    int exitcode;
    unsigned char done; /* set to 1 when child has finished and closed. */
};
static int direxists(const char *fname)
{
    DWORD result;
    result = GetFileAttributes(fname);
    if (result == INVALID_FILE_ATTRIBUTES) return 0;
    return !!(result & FILE_ATTRIBUTE_DIRECTORY);
}

#endif

#define PIPE_ENV "subprocess_pipe_env"
#define SUBPROCESS_CI_META "subprocess_child_info_metatable"
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
/*static int superfdopen(lua_State *L, int fd, const char *mode)
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
}*/

/* __close method for a pipe */
static int close_pipe(lua_State *L)
{
    int ok, err;
    FILE **p;

    puts("Here I am, in close_pipe");
    
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
        filedes_t filedes;
        FILE *fileobj;
    } info;
};

static void closefds(filedes_t *fds, int n)
{
    int i;
    for (i=0; i<n; ++i){
#if defined(OS_POSIX)
        if (fds[i] != -1)
            close(fds[i]);
#elif defined(OS_WINDOWS)
        if (fds[i] != INVALID_HANDLE_VALUE)
            CloseHandle(fds[i]);
#endif
    }
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

#ifdef OS_WINDOWS
/* Copy a Windows error into a buffer */
static void copy_w32error(char errmsg_out[], size_t errmsg_len, DWORD error)
{
    if (FormatMessage(
        FORMAT_MESSAGE_FROM_SYSTEM, NULL, error, 0,
        (void *) errmsg_out, errmsg_len, NULL) == 0)
    {
        strncpy(errmsg_out, "failed to get error message", errmsg_len + 1);
    }
}

/* Push a Windows error onto a Lua stack */
static void push_w32error(lua_State *L, DWORD error)
{
    LPTSTR buf;
    if (FormatMessage(
        FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM,
        NULL, error, 0, (void *) &buf, 1, NULL) == 0)
    {
        lua_pushliteral(L, "failed to get error message");
    } else {
        lua_pushstring(L, buf);
        LocalFree(buf);
    }
}

/* n is 0, 1 or 2
   return handle for standard input/output/error */
static HANDLE getstdhandle(int n)
{
    DWORD n2;
    switch (n){
        case 0: n2 = STD_INPUT_HANDLE; break;
        case 1: n2 = STD_OUTPUT_HANDLE; break;
        case 2: n2 = STD_ERROR_HANDLE; break;
        default: return INVALID_HANDLE_VALUE;
    }
    return GetStdHandle(n2);
}

struct str {
    char *data;
    size_t len;
    size_t size; /* size allocated */
};

static void str_init(struct str *s)
{
    s->data = NULL;
    s->len = 0;
    s->size = 0;
}

/* Append n chars from s2 */
static int str_appendlstr(struct str *s, char *s2, size_t n)
{
    void *newp;
    if (s->size < s->len + n){
        if (s->size < 16) s->size = 16;
        while (s->size < s->len + n)
            s->size = (s->size * 3) / 2;
        newp = realloc(s->data, s->size + 1);
        if (newp == NULL){
            free(s->data);
            return 0;
        }
        s->data = newp;
    }
    memcpy(s->data + s->len, s2, n);
    s->len += n;
    s->data[s->len] = '\0';
    return 1;
}

static int str_appendc(struct str *s, char ch)
{
    return str_appendlstr(s, &ch, 1);
}

/* Compiles command line for CreateProcess. Returns malloc'd string. */
static char *compile_cmdline(char *const *args)
{
    /*  "      --> \"
        \"     --> \\\"
        \<NUL> --> \\    */
    struct str str;
    char *arg;
    str_init(&str);
    while (*args != NULL){
        arg = *args++;
        if (!str_appendc(&str, '"')) return NULL;
        while (arg[0]){
            if (arg[0] == '"'){
                if (!str_appendlstr(&str, "\\\"", 2)) return NULL;
            } else if (arg[0] == '\\'){
                if (arg[1] == '"' || arg[1] == '\0'){
                    if (!str_appendlstr(&str, "\\\\", 2)) return NULL;
                } else {
                    if (!str_appendc(&str, '\\')) return NULL;
                }
            } else {
                if (!str_appendc(&str, arg[0])) return NULL;
            }
            arg++;
        }
        if (!str_appendlstr(&str, "\" ", 2)) return NULL;
    }
    str.data[str.len - 1] = '\0';
    return str.data;
}
#endif

/* Function for opening subprocesses. Returns 0 on success and -1 on failure.
   On failure, errmsg_out shall contain a '\0'-terminated error message. */
static int dopopen(char *const *args,        /* program arguments with NULL sentinel */
                   const char *executable,   /* actual executable */
                   struct fdinfo fdinfo[3],  /* info for stdin/stdout/stderr */
                   int close_fds,            /* 1 to close all fds */
                   const char *cwd,          /* working directory for program */
                   struct child_info *ci_out, /* set on success! */
                   FILE *pipe_ends_out[3],   /* pipe ends are put here */
                   char errmsg_out[],        /* written to on failure */
                   size_t errmsg_len         /* length of errmsg_out (EXCLUDING sentinel) */
                  )
#if defined(OS_POSIX)
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

    errmsg_out[errmsg_len] = '\0';

    for (i=0; i<3; ++i)
        pipe_ends_out[i] = NULL;

    /* Manage stdin/stdout/stderr */
    for (i=0; i<3; ++i){
        fdi = &fdinfo[i];
        switch (fdi->mode){
            case FDMODE_INHERIT:
inherit:
                fds[i] = dup(i);
                if (fds[i] == -1){
fd_failure:
                    strncpy(errmsg_out, strerror(errno), errmsg_len + 1);
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
        strncpy(errmsg_out, strerror(errno), errmsg_len + 1);
        closefds(fds, 3);
        closefiles(pipe_ends_out, 3);
        return -1;
    }
    /* Make write end close on exec */
    flags = fcntl(errpipe[1], F_GETFD);
    if (flags == -1){
pipe_failure:
        strncpy(errmsg_out, strerror(errno), errmsg_len + 1);
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
        strncpy(errmsg_out, strerror(en), errmsg_len + 1);
        return -1;
    }
    close(errpipe[0]);

    /* Child is now running */
    ci_out->pid = pid;
    return 0;
}
#elif defined(OS_WINDOWS)
{
    HANDLE hfiles[3], piper, pipew, hfile;
    int i, fd;
    struct fdinfo *fdi;
    SECURITY_ATTRIBUTES secattr;
    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    char *cmdline;

    errmsg_out[errmsg_len] = '\0';

    /* Create a SECURITY_ATTRIBUTES for inheritable handles */
    secattr.nLength = sizeof secattr;
    secattr.lpSecurityDescriptor = NULL;
    secattr.bInheritHandle = TRUE;

    for (i=0; i<3; ++i)
        pipe_ends_out[i] = NULL;

    /* Manage stdin/stdout/stderr */
    for (i=0; i<3; ++i){
        fdi = &fdinfo[i];
        switch (fdi->mode){
            case FDMODE_INHERIT:
inherit:
                /* XXX: duplicated file handles share the
                   same object (and thus file cursor, etc.).
                   CreateFile might be a better idea. */
                hfile = getstdhandle(i);
                if (hfile == INVALID_HANDLE_VALUE){
fd_failure:
                    copy_w32error(errmsg_out, errmsg_len, GetLastError());
                    closefds(hfiles, i);
                    closefiles(pipe_ends_out, i);
                    return -1;
                }
dup_hfile:
                if (DuplicateHandle(GetCurrentProcess(), hfile,
                    GetCurrentProcess(), &hfiles[i], 0, TRUE,
                    DUPLICATE_SAME_ACCESS) == 0)
                {
                    goto fd_failure;
                }
                break;
            case FDMODE_FILENAME:
                if (i == STDIN_FILENO){
                    hfiles[i] = CreateFile(
                        fdi->info.filename,
                        GENERIC_READ,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                        &secattr,
                        OPEN_EXISTING,
                        FILE_ATTRIBUTE_NORMAL,
                        NULL);
                } else {
                    hfiles[i] = CreateFile(
                        fdi->info.filename,
                        GENERIC_WRITE,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                        &secattr,
                        CREATE_ALWAYS,
                        FILE_ATTRIBUTE_NORMAL,
                        NULL);
                }
                if (hfiles[i] == INVALID_HANDLE_VALUE){
                    goto fd_failure;
                }
                break;
            case FDMODE_FILEDES:
                if (DuplicateHandle(GetCurrentProcess(), fdi->info.filedes,
                    GetCurrentProcess(), &hfiles[i], 0, TRUE,
                    DUPLICATE_SAME_ACCESS) == 0)
                {
                    goto fd_failure;
                }
                break;
            case FDMODE_FILEOBJ:
                fd = _fileno(fdi->info.fileobj);
                if (fd == -1){
get_osf_failure:
                    strncpy(errmsg_out, strerror(errno), errmsg_len + 1);
failure:
                    closefds(hfiles, i);
                    closefiles(pipe_ends_out, i);
                    return -1;
                }
                hfile = (HANDLE) _get_osfhandle(fd);
                if (hfile == INVALID_HANDLE_VALUE) goto get_osf_failure;
                goto dup_hfile;
            case FDMODE_PIPE:
                if (CreatePipe(&piper, &pipew, &secattr, 0) == 0)
                    goto fd_failure;
                if (i == STDIN_FILENO){
                    hfiles[i] = piper;
                    fd = _open_osfhandle((intptr_t) pipew, 0);
                    if (fd == -1){
                        strncpy(errmsg_out, "_open_osfhandle failed", errmsg_len + 1);
                        goto failure;
                    }
                    pipe_ends_out[i] = _fdopen(fd, "w");
                    if (pipe_ends_out[i] == 0){
                        strncpy(errmsg_out, "_fdopen failed", errmsg_len + 1);
                        goto failure;
                    }
                } else {
                    hfiles[i] = pipew;
                    fd = _open_osfhandle((intptr_t) piper, _O_RDONLY);
                    if (fd == -1){
                        strncpy(errmsg_out, "_open_osfhandle failed", errmsg_len + 1);
                        goto failure;
                    }
                    pipe_ends_out[i] = _fdopen(fd, "r");
                    if (pipe_ends_out[i] == 0){
                        strncpy(errmsg_out, "_fdopen failed", errmsg_len + 1);
                        goto failure;
                    }
                }
                break;
            case FDMODE_STDOUT:
                if (i == STDERR_FILENO){
                    hfile = hfiles[STDOUT_FILENO];
                    goto dup_hfile;
                } else goto inherit;
        }
    }

    /* Find executable name */
    if (!executable){
        /* use first arg */
        executable = args[0];
    }

    /* Compile command line into string. Yuck. */
    cmdline = compile_cmdline(args);
    if (!cmdline){
        strncpy(errmsg_out, "memory full", errmsg_len + 1);
        closefds(hfiles, 3);
        closefiles(pipe_ends_out, 3);
        return -1;
    }

    
    if (CreateProcess(
        executable, /* lpApplicationName */
        cmdline,    /* lpCommandLine */
        NULL,       /* lpProcessAttributes */
        NULL,       /* lpThreadAttributes */
        FALSE,      /* bInheritHandles */
        0,          /* dwCreationFlags */
        NULL,       /* lpEnvironment */
        cwd,        /* lpCurrentDirectory */
        &si,        /* lpStartupInfo */
        &pi)        /* lpProcessInformation */
    == 0){
        copy_w32error(errmsg_out, errmsg_len, GetLastError());
        free(cmdline);
        closefds(hfiles, 3);
        closefiles(pipe_ends_out, 3);
        return -1;
    }
    CloseHandle(pi.hThread); /* Don't want this handle */
    free(cmdline);
    closefds(hfiles, 3); /* XXX: is this correct? */
    closefiles(pipe_ends_out, 3);
    ci_out->pid = pi.dwProcessId;
    ci_out->hProcess = pi.hProcess;
    return 0;
}
#endif

/* popen {arg0, arg1, arg2, ..., [executable=...]} */
static int superpopen(lua_State *L)
{
    struct child_info *cip;

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

    FILE *pipe_ends[3] = {NULL, NULL, NULL};
    int i, j, result;
    FILE *f;
    const char *s;

    char errmsg_buf[256];

    struct child_info ci;
    
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
        if (!direxists(cwd)){
            free(executable);
            freestrings(args, nargs);
            free(args);
            return luaL_error(L, "directory `%s' does not exist", cwd);
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
                lua_pushliteral(L, "STDOUT must be used only for stderr when stdout is set to PIPE");
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
                lua_pushliteral(L, "out of memory");
                goto files_failure;
            }
        } else if (lua_isnumber(L, -1)){
            /* use this fd */
            fdinfo[i].mode = FDMODE_FILEDES;
            fdinfo[i].info.filedes = (filedes_t) lua_tointeger(L, -1);
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

    result = dopopen(args, executable, fdinfo, close_fds, cwd, &ci, pipe_ends, errmsg_buf, 255);
    for (i=0; i<3; ++i)
        if (fdinfo[i].mode == FDMODE_FILENAME)
            free(fdinfo[i].info.filename);
    free(executable);
    freestrings(args, nargs);
    free(args);
    if (result == -1){
        /* failed */
        return luaL_error(L, "popen failed: %s", errmsg_buf);
    }

    /* Create popen object: it has two parts, the table part and the userdata part. */
    /* Create table for subprocess info */
    lua_createtable(L, 0, 2);
    luaL_getmetatable(L, SUBPROCESS_META);
    lua_setmetatable(L, -2);
    /* Create userdata */
    cip = lua_newuserdata(L, sizeof(struct child_info));
    luaL_getmetatable(L, SUBPROCESS_CI_META);
    lua_setmetatable(L, -2);
    *cip = ci;
    cip->done = 0;
    /* Put userdata in table */
    lua_setfield(L, -2, "_child_info");
    /* Set pid */
    lua_pushinteger(L, ci.pid);
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

/* Make sure first value on stack is a subprocess object
   and return pointer to the child_info */
static struct child_info *checksp(lua_State *L)
{
    struct child_info *ci;
    luaL_checktype(L, 1, LUA_TTABLE);
    lua_getfield(L, 1, "_child_info");
    ci = luaL_checkudata(L, -1, SUBPROCESS_CI_META);
    lua_pop(L, 1);
    return ci;
}

static int spu_gc(lua_State *L)
{
    struct child_info *ci = luaL_checkudata(L, 1, SUBPROCESS_CI_META);
    if (!ci->done){
#if defined(OS_POSIX)
        /* Try to wait for process to avoid leaving zombie.
           If the process hasn't finished yet, we'll end up leaving a zombie. */
        int stat;
        waitpid(ci->pid, &stat, WNOHANG);
#elif defined(OS_WINDOWS)
        CloseHandle(ci->hProcess);
#endif
        ci->done = 1; /* just in case */
    }
    return 0;
}

/* Push string representation of process on stack */
static int sp_tostring(lua_State *L)
{
    struct child_info *ci = checksp(L);
    lua_pushfstring(L, "subprocess (%d)", (int) ci->pid);
    return 1;
}

/* Wait for, or poll, a process */
static int sp_waitpid(lua_State *L, struct child_info *ci, int wait)
#if defined(OS_POSIX)
{
    int stat, options;

    if (ci->done){
        lua_pushinteger(L, ci->exitcode);
        return 1;
    }

    if (wait) options = 0;
    else options = WNOHANG;
    switch (waitpid(ci->pid, &stat, options)){
        case -1:
            return luaL_error(L, strerror(errno));
        case 0:
            /* child still running */
            lua_pushnil(L);
            return 1;
        default:
            if (WIFEXITED(stat)){
                /* child terminated */
                ci->exitcode = WEXITSTATUS(stat);
doret:
                ci->done = 1;
                lua_pushinteger(L, ci->exitcode);
                lua_pushvalue(L, -1);
                lua_setfield(L, -3, "exitcode");
                return 1;
            } else if (WIFSIGNALED(stat)){
                /* child died on a signal */
                ci->exitcode = -WTERMSIG(stat);
                goto doret;
            } else if (WIFSTOPPED(stat)){
                ci->exitcode = -WSTOPSIG(stat);
                goto doret;
            } else {
                /* ??? */
                ci->exitcode = 1;
                ci->done = 1;
                lua_pushliteral(L, "disappeared into black hole");
                lua_pushvalue(L, -1);
                lua_setfield(L, -3, "exitcode");
                return 1;
            }
    }
}
#elif defined(OS_WINDOWS)
{
    DWORD dwMilliseconds, retval, exitcode;

    if (ci->done){
        lua_pushinteger(L, ci->exitcode);
        return 1;
    }
    if (wait) dwMilliseconds = INFINITE;
    else dwMilliseconds = 0;
    retval = WaitForSingleObject(ci->hProcess, dwMilliseconds);
    switch (retval){
        case WAIT_FAILED:
failure:
            push_w32error(L, GetLastError());
            return lua_error(L);
        case WAIT_OBJECT_0:
            /* child finished */
            if (GetExitCodeProcess(ci->hProcess, &exitcode) == 0){
                goto failure;
            }
            CloseHandle(ci->hProcess);
            ci->exitcode = exitcode;
            ci->done = 1;
            lua_pushinteger(L, ci->exitcode);
            lua_pushvalue(L, -1);
            lua_setfield(L, -3, "exitcode");
            return 1;
        case WAIT_TIMEOUT:
        default:
            /* child still running */
            lua_pushnil(L);
            return 1;
    }
}
#endif

static int sp_poll(lua_State *L)
{
    return sp_waitpid(L, checksp(L), 0);
}

static int sp_wait(lua_State *L)
{
    return sp_waitpid(L, checksp(L), 1);
}

#if defined(OS_POSIX)
static int sp_send_signal(lua_State *L)
{
    struct child_info *ci = checksp(L);
    int sig = luaL_checkint(L, 2);
    if (!ci->done){
        if (kill(ci->pid, sig)){
            return luaL_error(L, "kill: %s", strerror(errno));
        }
        ci->done = 1;
        ci->exitcode = -sig;
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
#elif defined(OS_WINDOWS)
static int sp_terminate(lua_State *L)
{
    struct child_info *ci = checksp(L);
    if (!ci->done){
        if (TerminateProcess(ci->hProcess, -9) == 0){
            push_w32error(L, GetLastError());
            return lua_error(L);
        }
        CloseHandle(ci->hProcess);
        ci->exitcode = -9;
        ci->done = 1;
    }
    return 0;
}
#endif

static const luaL_Reg subprocess_meta[] = {
    {"__tostring", sp_tostring},
    {"poll", sp_poll},
    {"wait", sp_wait},
#if defined(OS_POSIX)
    {"send_signal", sp_send_signal},
    {"terminate", sp_terminate},
    {"kill", sp_kill},
#elif defined(OS_WINDOWS)
    {"terminate", sp_terminate},
    {"kill", sp_terminate},
#endif
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
    lua_pushliteral(L, "*a");
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

#if 0
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
        lua_pushliteral(L, "disappeared into black hole");
    }
    return 2;
}
#endif

static int test(lua_State *L)
{
#if 0
    HANDLE rpipe, wpipe;
    int rfd, wfd;
    FILE *rf, *wf;
    if (CreatePipe(&rpipe, &wpipe, NULL, 16384) == 0){
        return luaL_error(L, "CreatePipe failed");
    }
    rfd = _open_osfhandle((intptr_t) rpipe, _O_RDONLY);
    if (rfd == 0){
        lua_pushliteral(L, "_open_osfhandle failed");
        CloseHandle(rpipe);
        CloseHandle(wpipe);
        return lua_error(L);
    }
    rf = _fdopen(rfd, "r");
    if (!rf){
        lua_pushfstring(L, "_fdopen failed: %s", strerror(errno));
        _close(rfd);
        CloseHandle(wpipe);
        return lua_error(L);
    }
    wfd = _open_osfhandle((intptr_t) wpipe, 0);
    if (wfd == 0){
        lua_pushliteral(L, "_open_osfhandle failed");
        fclose(rf);
        CloseHandle(wpipe);
        return lua_error(L);
    }
    wf = _fdopen(wfd, "w");
    if (!wf){
        lua_pushfstring(L, "_fdopen failed: %s", strerror(errno));
        fclose(rf);
        _close(wfd);
        return lua_error(L);
    }
    ftopipe(L, rf);
    ftopipe(L, wf);
    return 2;
#endif

    SECURITY_ATTRIBUTES secattr;
    STARTUPINFO si;
    PROCESS_INFORMATION pi;
    HANDLE rpipe, wpipe, hFile0, hFile2;
    int fd;
    DWORD bread;
    FILE *f;
    char buf[256];

    secattr.nLength = sizeof secattr;
    secattr.lpSecurityDescriptor = NULL;
    secattr.bInheritHandle = TRUE;
    if (CreatePipe(&rpipe, &wpipe, &secattr, 0) == 0){
        puts("CreatePipe failed");
        return 0;
    }
    if (DuplicateHandle(GetCurrentProcess(), GetStdHandle(STD_INPUT_HANDLE),
        GetCurrentProcess(), &hFile0, 0, TRUE, DUPLICATE_SAME_ACCESS) == 0)
    {
        puts("DuplicateHandle failed");
        return 0;
    }
    if (DuplicateHandle(GetCurrentProcess(), GetStdHandle(STD_ERROR_HANDLE),
        GetCurrentProcess(), &hFile2, 0, TRUE, DUPLICATE_SAME_ACCESS) == 0)
    {
        puts("DuplicateHandle failed");
        return 0;
    }
    si.cb = sizeof si;
    si.lpReserved = NULL;
    si.lpDesktop = NULL;
    si.lpTitle = NULL;
    si.dwFlags = STARTF_USESTDHANDLES;
    si.cbReserved2 = 0;
    si.lpReserved2 = NULL;
    si.hStdInput = hFile0;
    si.hStdOutput = wpipe;
    si.hStdError = hFile2;
    if (CreateProcess(
        "printargs.exe",
        "printargs.exe one two three",
        NULL, NULL, FALSE, 0, NULL, NULL, &si, &pi)
    == 0){
        puts("CreateProcess failed");
        return 0;
    }
    CloseHandle(hFile0);
    CloseHandle(hFile2);
    CloseHandle(pi.hThread);
    CloseHandle(pi.hProcess);
    CloseHandle(wpipe);
    
    fd = _open_osfhandle((intptr_t) rpipe, _O_RDONLY);
    if (fd == -1){
        puts("_open_osfhandle failed");
        return 0;
    }
    f = _fdopen(fd, "r");
    if (!f){
        puts("_fdopen failed");
        return 0;
    }

    return ftopipe(L, f);
}

static const luaL_Reg subprocess[] = {
    /* {"pipe", superpipe}, */
    {"popen", superpopen},
    {"call", call},
    {"call_capture", call_capture},
    /* {"wait", superwait}, */
    {"test", test},
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
    luaL_newmetatable(L, SUBPROCESS_CI_META);
    lua_pushboolean(L, 0);
    lua_setfield(L, -2, "__metatable");
    lua_pushcfunction(L, spu_gc);
    lua_setfield(L, -2, "__gc");
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
