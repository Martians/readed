
#ifndef __FS_COMMON_PROCESS_CONTROL_H__
#define __FS_COMMON_PROCESS_CONTROL_H__

typedef int (*start_func_t)(int argc, char **argv);

/* WARNING: SIGCHLD will not ignored after this call
 * must set signal handler in start_func if you don't care about this event
 * */
extern int process_start(int argc, char **argv, 
        start_func_t start_func);

#endif

