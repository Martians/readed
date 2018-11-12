// vim: ts=4 sw=4 expandtab

#ifndef __YFS_ARGS_HPP__
#define __YFS_ARGS_HPP__

#include <stdlib.h>
#include <iostream>
#include <string.h>

class YfsArgs {
public:

    YfsArgs(char* name)
    {
        argc = 1;
        argv = (char**) malloc(sizeof (char*));
        argv[0] = strdup(name);
    }

    ~YfsArgs()
    {
        Free();
    }

    int
    insert(const char* opt, const char* optarg)
    {
        int tic = 0, tia = 0;
        if (opt != NULL) tic++;
        if (optarg != NULL) tia++;
        char** targv = (char**) realloc(argv, (argc + tic + tia) * sizeof (char*));
        if (targv == NULL) {
            return -1;
        }
        argv = targv;
        if (tic != 0) {
            argv[argc++] = strdup(opt);
        }
        if (tia != 0) {
            argv[argc++] = strdup(optarg);
        }
        return 0;
    }

    void
    dump()
    {
        for (int i = 0; i < argc; i++) {
            std::cout << "argv[" << i
                    << "] : " << argv[i] << std::endl;
        }
    }

private:

    void
    Free()
    {
        if (argv != NULL) {
            for (int i = 0; i < argc; i++) {
                if (argv[i] != NULL) {
                    free(argv[i]);
                    argv[i] = NULL;
                }
            }
            free(argv);
            argv = NULL;
        }
    }

public:
    int argc;
    char** argv;
};

#endif
