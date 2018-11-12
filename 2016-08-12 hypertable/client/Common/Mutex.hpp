
#pragma once

#include <pthread.h>
#include <assert.h>

namespace common {

#define DECLARE_UNUSED_PARAM(p) p __attribute__((unused))

class Mutex {
private:
	// don't allow copying.
	Mutex (const Mutex &M) {}
	void operator = (Mutex &M) {}

#ifdef LOCKDEP
	void _register() {
		id = lockdep_register(name);
	}
	void _will_lock() { // about to lock
		id = lockdep_will_lock(name, id);
	}
	void _locked() { // just locked
		id = lockdep_locked(name, id, backtrace);
	}
	void _unlocked() { // just unlocked
		id = lockdep_unlocked(name, id);
	}
#else
	void _register() {
	}
	void _will_lock() {
	} // about to lock
	void _locked() {
	} // just locked
	void _unlocked() {
	} // just unlocked
#endif

public:
	Mutex(const char *n, bool r = false, bool ld = true, bool bt = false) :
		name(n), id(-1), recursive(r), lockdep(ld), backtrace(bt), nlock(0) {
		if (recursive) {
			pthread_mutexattr_t attr;
			pthread_mutexattr_init(&attr);
			pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
			pthread_mutex_init(&_m, &attr);
			pthread_mutexattr_destroy(&attr);
		} else {
			pthread_mutex_init(&_m, NULL);
		}
		//if (lockdep && g_lockdep) _register();
	}
	~Mutex() {
		assert(nlock == 0);
		pthread_mutex_destroy(&_m);
	}

	bool is_locked() {
		return (nlock > 0);
	}

	bool try_lock() {
		int r = pthread_mutex_trylock(&_m);
		if (r == 0) {
			//if (lockdep && g_lockdep) _locked();
			nlock++;
		}
		return r == 0;
	}

	void lock(bool no_lockdep = false) {
		//if (lockdep && g_lockdep && !no_lockdep) _will_lock();
		int DECLARE_UNUSED_PARAM(r); 
		r = pthread_mutex_lock(&_m);
		//if (lockdep && g_lockdep) _locked();
		assert(r == 0);
		nlock++;
	}

	void unlock() {
		int DECLARE_UNUSED_PARAM(r);
		assert(nlock > 0);
		--nlock;
		r = pthread_mutex_unlock(&_m);
		assert(r == 0);
		//if (lockdep && g_lockdep) _unlocked();
	}

	friend class Cond;

public:
	class Locker {

	public:
	    Locker(Mutex& m, bool _l = true) :
	    	_mutex(m), _lock(_l) {
	    	if (_lock) _mutex.lock();
	    }
	    ~Locker() {
	        if (_lock) {
	        	_lock = false;
	        	_mutex.unlock();
	        }
	    }

	    void	lock() {
	    	_lock = true;
	    	_mutex.lock();
	    }

	    void	unlock() {
	    	_lock = false;
	    	_mutex.unlock();
	    }
	public:
	    Mutex& 	_mutex;
	    bool 	_lock;
	};

private:
	const char *name;
	int id;
	bool recursive;
	bool lockdep;
	bool backtrace; // gather backtrace on lock acquisition

	pthread_mutex_t _m;
	int nlock;
};

#define USE_LOCK			Mutex::Locker locker(GetMutex())
/** lock again */
#define DO_LOCK				locker.lock()
/** unlock */
#define DO_UNLOCK			locker.unlock()

class SpinLock {
private:
	const char *mName;
	pthread_spinlock_t mSpin;
	int mLockCount;

	// don't allow copying.
	void operator=(SpinLock &M) {
	}
	SpinLock(const SpinLock &M) {
	}

public:
	SpinLock(const char *n, bool r = false) : mName(n)
    {
        mLockCount = 0;
        assert(0 == pthread_spin_init(&mSpin, PTHREAD_PROCESS_PRIVATE));
    }
	~SpinLock() {
		assert(mLockCount == 0);
		pthread_spin_destroy(&mSpin);
	}

	bool is_locked() {
		return (mLockCount > 0);
	}

	bool TryLock() {
		int r = pthread_spin_trylock(&mSpin);
		if (r == 0) {
			mLockCount++;
		}
		return r == 0;
	}

	void Lock() {
		int r = pthread_spin_lock(&mSpin);
		assert(r == 0);
		mLockCount++;
	}

	void Unlock() {
		assert(mLockCount > 0);
		--mLockCount;
		int r = pthread_spin_unlock(&mSpin);
		assert(r == 0);
	}

public:
	class Locker {
		SpinLock &mSpinLock;

	public:
		Locker(SpinLock& m) :
			mSpinLock(m) {
			mSpinLock.Lock();
		}
		~Locker() {
			mSpinLock.Unlock();
		}
	};
};

class RWLock {
public:
	RWLock() {
		pthread_rwlock_init(&locker, NULL);
	}
	virtual ~RWLock() {
		pthread_rwlock_destroy(&locker);
	}
private:
	pthread_rwlock_t locker;
public:
	void rdlock() {
		pthread_rwlock_rdlock(&locker);
	}
	void wrlock() {
		pthread_rwlock_wrlock(&locker);
	}
	void unlock() {
		pthread_rwlock_unlock(&locker);
	}

public:
    class WriteLocker 
    {
        RWLock &mLock;
    public:
        WriteLocker(RWLock &lock)
            : mLock(lock) {
                mLock.wrlock();
            }
        ~WriteLocker() {
            mLock.unlock();
        }
    };

    class ReadLocker
    {
        RWLock &mLock;
    public:
        ReadLocker(RWLock &lock)
            : mLock(lock) {
                mLock.rdlock();
            }
        ~ReadLocker() {
            mLock.unlock();
        }
    };
};

}

using common::Mutex;

