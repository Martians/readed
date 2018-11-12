#include "ProcessControl.h"
#include <stdlib.h>
#include <errno.h>
#include <signal.h>
#include <sys/wait.h>
#include <syslog.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <getopt.h>
#include "YfsArgs.h"

struct option s_process_start_long_opts[] = {
        {"daemon", no_argument, NULL, 'd'},
        {"fork",   no_argument, NULL, 'f'},
        {0, 0, 0, 0}
};


#ifdef USE_PTHREADS
static pthread_mutex_t sSignalLock = PTHREAD_MUTEX_INITIALIZER;
#endif

static int s_terminate = 0;

static int s_reload = 0;

static int s_terminal_signal[] = {
        SIGTERM,
        -1
};

static int s_reload_signal[] = {
        SIGHUP,
        -1
};

static int s_ignore_signal[] = {
        SIGQUIT,
#ifdef SIGPIPE
        SIGPIPE,
#endif
#ifdef SIGTSTP
        SIGTSTP,
#endif
#ifdef SIGTTIN
        SIGTTIN,
#endif
#ifdef SIGTTOU
        SIGTTOU,
#endif
#ifdef SIGINFO
        SIGINFO,
#endif
#ifdef SIGUSR1
        SIGUSR1,
#endif
#ifdef SIGUSR2
        SIGUSR2,
#endif
#if 0
#ifdef SIGCHLD
SIGCHLD,
#endif
#ifdef SIGCLD
SIGCLD,
#endif
#endif
        -1
};

static int s_daemon_ignores_signal[] = {
       // SIGINT,
        -1
};

static void
static_terminal_handler(int signo)
{
    (void) signo;
#ifdef USE_PTHREADS
    pthread_mutex_lock(&sSignalLock);
#endif

    if (s_terminate == 0) {
        s_terminate = 1;
    }

#ifdef USE_PTHREADS
    pthread_mutex_unlock(&sSignalLock);
#endif
}

static void
static_reload_handler(int signo)
{
    (void) signo;
#ifdef USE_PTHREADS
    pthread_mutex_lock(&sSignalLock);
#endif
    s_reload = 1;
#ifdef USE_PTHREADS
    pthread_mutex_unlock(&sSignalLock);
#endif
}

void
__set_signal_handlers(int daemon)
{
    struct sigaction sa;
    int i;
    sa.sa_flags = 0;
    sigemptyset(&sa.sa_mask);
    sa.sa_handler = static_terminal_handler;

    for (i = 0; s_terminal_signal[i] > 0; i++) {
        sigaction(s_terminal_signal[i], &sa, (struct sigaction *) 0);
    }

    sa.sa_handler = static_reload_handler;

    for (i = 0; s_reload_signal[i] > 0; i++) {
        sigaction(s_reload_signal[i], &sa, (struct sigaction *) 0);
    }

    sa.sa_handler = SIG_IGN;

    for (i = 0; s_ignore_signal[i] > 0; i++) {
        sigaction(s_ignore_signal[i], &sa, (struct sigaction *) 0);
    }

    sa.sa_handler = daemon != 0 ? SIG_IGN : static_terminal_handler;

    for (i = 0; s_daemon_ignores_signal[i] > 0; i++) {
        sigaction(s_daemon_ignores_signal[i], &sa, (struct sigaction *) 0);
    }
}


#if 0
static int
__check_output_format(char *str, const int slen, 
        const char *exp, const int elen)
{
    int ret = 1;
    char *ptr = NULL;

    if (str[slen - 1] == '\n') {
        str[slen - 1] = '\0';
    }

    ptr = rindex(str, '\n');

    if (ptr == NULL) {
        ptr = str;
    }

    if (strncmp(str, exp, elen) == 0) {
        ret = 0;
    }

    return ret;
}
#endif


int
__try_make_daemon(int argc, char **argv)
{
    int d = 0;
    char ch;
    optind = 0;
    opterr = 0;
    YfsArgs args(argv[0]);

    for (int i = 1; i < argc; i++) {
        args.insert(argv[i], NULL);
    }
    while ((ch = getopt_long(args.argc, args.argv, "d", s_process_start_long_opts, NULL)) != -1) {
        if (ch == 'd') {
            syslog(LOG_INFO, "daemon");
            d = 1;
            daemon(0, 0);
            break;
        }
        else {
            continue;
        }
    }

    optind = 0;
    return d;
}

/* return 0 if quit normally
 * return non-zero if abnormal */
int
__wait_child(pid_t pid)
{
    int ret = 1;
    pid_t wait_pid;
    int status = 0;

    do {
        wait_pid = waitpid(pid, &status, 0);
        if (wait_pid < 0) {
            syslog(LOG_ERR, "waitpid returns error: %s", strerror(errno));
            break;
        }

        if (wait_pid == pid) {
            syslog(LOG_INFO, "child %u exit with status %d", pid, status);
            if (WIFEXITED(status)) {
                exit(WEXITSTATUS(status));
                ret = 0;
            }
            break;
        }

        syslog(LOG_INFO, "another child %u exit, not the child(%u) i'm waiting", wait_pid, pid);

    } while (1);

    return ret;
}

int
__do_fork(pid_t *pid)
{
    int ret = 0;
    pid_t child = 0;

    do {
        syslog(LOG_DEBUG, "ready to fork");
        child = fork();
        if (child < 0) {
            ret = errno;
            syslog(LOG_ERR, "Fork error: %s", strerror(ret));
            assert(ret != 0);
            return ret;
        }
        else if (child > 0) {
            /* parent wait child exit */
            syslog(LOG_INFO, "create child %d for service", child);
            ret = __wait_child(child);
            syslog(LOG_INFO, "child %d exit", child);
        }
        else {
            /* child do nothing */
            ret = 0;
            break;
        }
    } while (ret != 0);

    *pid = child;

    return ret;
}

int
__try_fork(int argc, char **argv, pid_t *pid)
{
    int need_fork = 0;
    char ch;
    optind = 0;
    opterr = 0;
    YfsArgs args(argv[0]);

    for (int i = 1; i < argc; i++) {
        args.insert(argv[i], NULL);
    }
    while ((ch = getopt_long(args.argc, args.argv, "f", s_process_start_long_opts, NULL)) != -1) {
        if (ch == 'f') {
            syslog(LOG_DEBUG, "need fork");
            optind = 0;
            __set_signal_handlers(true);
            return __do_fork(pid);
        }
        else {
            continue;
        }
    }

    if (need_fork == 0) {
        *pid = 0;
    }

    optind = 0;
    return 0;
}

int
process_start(int argc, char **argv,
        start_func_t start_func)
{
    int ret = 0;
    int idx = 0;
    int isdaemon = 0;
    pid_t child = 0;
    do {
        if (argv == NULL || argc < 1) {
            syslog(LOG_ERR, "process start failed, error: %s", strerror(errno));
            ret = -EINVAL;
            break;
        }

        for (idx = 0; idx < argc; idx++) {
            if (argv[idx] == NULL) {
                ret = -EINVAL;
                break;
            }
        }

        if (ret != 0) {
            syslog(LOG_ERR, "process start failed, error: %s", strerror(errno));
            break;
        }

        isdaemon = __try_make_daemon(argc, argv);
        __set_signal_handlers(isdaemon);

        ret = __try_fork(argc, argv, &child);
        if (ret == 0 && child == 0) {
            ret = start_func(argc, argv);
        }

    } while (0);

    return ret;
}



