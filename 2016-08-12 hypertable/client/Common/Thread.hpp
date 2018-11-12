//file: Thread.hpp
//desc:
//author: Wen Zhang(wenz.zhang@gmail.com)
//create: 2009-11-06

#pragma once

#define UNUSED_ATTR __attribute__((unused))

#include <pthread.h>
#include <signal.h>
#include <errno.h>
#include <assert.h>
#include <string.h>
#include "Mutex.hpp"
#include "Cond.hpp"

namespace common {

class ThreadCancelDisabler
{
public:
	ThreadCancelDisabler()
	{
		pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
	}
	~ThreadCancelDisabler()
	{
		pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
	}
};

class CommonThread {

public:
	CommonThread() : thread_id(0) {}
	virtual ~CommonThread() {}

protected:

	virtual void *entry() = 0;

private:
	static void *_entry_func(void *arg) {
		void *r = ((CommonThread*) arg)->entry();
		//_num_threads.dec();
		return r;
	}

public:
	pthread_t &get_thread_id() {
		return thread_id;
	}
	bool is_started() {
		return thread_id != 0;
	}
	bool am_self() {
		return (pthread_self() == thread_id);
	}

	//static int get_num_threads() { return _num_threads.test(); }

	int kill(int signal) {
		if (thread_id)
			return pthread_kill(thread_id, signal);
		else
			return -EINVAL;
	}

	void cancel() {
		if (thread_id) {
			pthread_cancel(thread_id);
		}
	}

	int create(bool detach = false) {
		//_num_threads.inc();
#if 0
		size_t stack_size;
		pthread_attr_getstacksize(&attr, &stack_size);
		stack_size *= 2;
		pthread_attr_setstacksize(&attr, stack_size);
#endif
		pthread_attr_t attr;
		memset(&attr, 0, sizeof(pthread_attr_t));
		pthread_attr_init(&attr);
		if (detach) {
			pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
		}

		int r = pthread_create(&thread_id, &attr, _entry_func, (void*) this);
		//generic_dout(10) << "thread " << thread_id << " start" << dendl;

		pthread_attr_destroy(&attr);
		return r;
	}

	int join(void **prval = 0) {
		if (thread_id == 0) {
			//generic_derr(0) << "WARNING: join on thread that was never started" << dendl;
			assert(0);
			return -EINVAL; // never started.
		}

		int status = pthread_join(thread_id, prval);
		if (status != 0) {
			switch (status) {
			case -EINVAL:
				//generic_derr(0) << "thread " << thread_id << " join status = EINVAL" << dendl;
				break;
			case -ESRCH:
				//generic_derr(0) << "thread " << thread_id << " join status = ESRCH" << dendl;
				assert(0);
				break;
			case -EDEADLK:
				//generic_derr(0) << "thread " << thread_id << " join status = EDEADLK" << dendl;
				break;
			default:
				;
				;
				//generic_derr(0) << "thread " << thread_id << " join status = " << status << dendl;
			}
			assert(0); // none of these should happen.
		}
		//generic_dout(10) << "thread " << thread_id << " stop" << dendl;
		thread_id = 0;
		return status;
	}

	int detach() {
		if (thread_id == 0) {
			return -EINVAL;
		}
		int status = pthread_detach(thread_id);
		if (status != 0) {
			switch (status) {
			case EINVAL:
				break;
			case ESRCH:
				break;
			default:
				break;
			}
		}
		return status;
	}

private:
	pthread_t thread_id;
};


#include "Time.hpp"
typedef void (*time_work_t)();

class ClientThread : public CommonThread
{
public:
	ClientThread(int time_ms = 1000, time_work_t handle = NULL)
		: mRunning(false), mLock("client thread", true), mHandle(handle)
	{
		set_wait(time_ms);
	}

public:
    /**
     * thread process
     **/
    virtual void *entry() {
    	if (!mHandle) return NULL;

		while (mRunning) {
			wait();

			mHandle();
		}
		return NULL;
	}

    void	set_wait(int time) {
    	mWait = utime_t(time / 1000, (time % 1000) * 1000);
    }

    int	 	start() {
        int err = 0;

        if (!mRunning) {
            mRunning = true;
            err = create();
        }
        assert(err == 0);
        return err;
    }

    void	stop() {
        if (!mRunning) {
            return;
        }
        mRunning = false;

        wakeup();
        join();
    }

    void	wait() {
		mLock.lock();
		mCond.wait_interval(mLock, mWait);
		mLock.unlock();
    }

    void	wakeup() {
		mLock.lock();
		mCond.signal_all();
		mLock.unlock();
	}

protected:
    bool	mRunning;
    Mutex 	mLock;
    Cond 	mCond;
    utime_t	mWait;
    time_work_t mHandle;
};
}

