
#pragma once

#include <time.h>
#include <pthread.h>

#include "Mutex.hpp"
#include "Time.hpp"

namespace common {

class Cond {
	/** don't allow copying */
private:
    void operator = (Cond &C) {}
    Cond (const Cond &C) {}

public:
    Cond() {
        int DECLARE_UNUSED_PARAM(r);
        r = pthread_condattr_init(&_attr);
        assert(r == 0);
        r = pthread_condattr_setclock(&_attr, CLOCK_MONOTONIC);
        assert(r == 0);
        r = pthread_cond_init(&_c, &_attr);
        assert(r == 0);
    }

    virtual ~Cond() {
        pthread_cond_destroy(&_c);
        pthread_condattr_destroy(&_attr);
    }

    int wait(Mutex &mutex) {
        int r = pthread_cond_wait(&_c, &mutex._m);
        return r;
    }

    int wait(Mutex &mutex, char *s) {
        //cout << "Wait: " << s << endl;
        int r = pthread_cond_wait(&_c, &mutex._m);
        return r;
    }

    int wait_interval(Mutex &mutex, utime_t interval) {
        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        ts.tv_sec += interval.sec();
        ts.tv_nsec += interval.nsec();

        if (ts.tv_nsec > 1000000000) {
            ts.tv_nsec -= 1000000000;
            ts.tv_sec += 1;
        }

        int r = pthread_cond_timedwait(&_c, &mutex._m, &ts);
        return r;
    }

    int signal() {
        //int r = pthread_cond_signal(&_c);
        int r = pthread_cond_broadcast(&_c);
        return r;
    }

    int signal_one() {
        int r = pthread_cond_signal(&_c);
        return r;
    }

    int signal_all() {
        //int r = pthread_cond_signal(&_c);
        int r = pthread_cond_broadcast(&_c);
        return r;
    }

protected:
    pthread_cond_t _c;
    pthread_condattr_t _attr;
};

}

using common::Cond;

