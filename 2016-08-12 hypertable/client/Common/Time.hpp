
#pragma once

#include <sys/time.h>

typedef uint64_t ctime_t;

static const int c_time_level1 = 1000;
static const int c_time_level2 = 1000000;

inline ctime_t u2m(ctime_t us) { return us / c_time_level1; }
inline ctime_t m2u(ctime_t ms) { return ms * c_time_level1; }

inline ctime_t ywb_time_now(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (((ctime_t)tv.tv_sec) * c_time_level2 + tv.tv_usec);
}

namespace common {

struct TimeRecord
{
public:
	TimeRecord() { begin(); }
	TimeRecord(const TimeRecord& v) {
		*this = v;
	}

	const TimeRecord& operator = (const TimeRecord& v) {
		_start = v._start;
		_time = v._time;
		_last = v._last;
		return *this;
	}
public:
	void	begin() { _start = _time = ywb_time_now(); _last = 0; }

	ctime_t	end() { return check(); }

	ctime_t	check() {
		ctime_t now = ywb_time_now();
		_last = now - _time;
		_time = now;
		return _last;
	}

	ctime_t	last() { return _last; }

	ctime_t	elapse() { return _time - _start; }

public:
	/** start time */
	ctime_t _start;
	/** current time */
	ctime_t _time;
	/** last interval */
	ctime_t _last;
};

/**
 * time util
 **/
struct TimeCheck
{
public:
	TimeCheck(int _wait = 0) : wait(_wait * 1000) { update(); }

	TimeCheck(const TimeCheck& v) {
		operator = (v);
	}

	const TimeCheck& operator = (const TimeCheck& v) {
		wait = v.wait;
		last = v.last;
		return *this;
	}
public:
	/**
	 * update current time
	 **/
	void	update() { last = ywb_time_now(); }

	/**
	 * check if timeout
	 **/
	bool	timeout(ctime_t* _now = NULL) {
		ctime_t now = _now ? *_now : ywb_time_now();
		if (last > now || last + wait <= now) {
			last = now;
			return true;
		}
		return false;
	}
public:
	/** wait time */
	ctime_t wait;
	/** last check time */
	ctime_t last;
};

class utime_t {
public:
	struct {
		uint32_t tv_sec, tv_usec;
	} tv;

	friend class Clock;

public:
	void normalize() {
		if (tv.tv_usec > 1000 * 1000) {
			tv.tv_sec += tv.tv_usec / (1000 * 1000);
			tv.tv_usec %= 1000 * 1000;
		}
	}

	// cons
	utime_t() {
		tv.tv_sec = 0;
		tv.tv_usec = 0;
		normalize();
	}
	//utime_t(time_t s) { tv.tv_sec = s; tv.tv_usec = 0; }
	utime_t(time_t s, int u) {
		tv.tv_sec = s;
		tv.tv_usec = u;
		normalize();
	}
	/*
	 utime_t(const struct ceph_timespec &v) {
	 decode_timeval(&v);
	 }
	 */

	utime_t(const struct timeval &v) {
		set_from_timeval(&v);
	}
	utime_t(const struct timeval *v) {
		set_from_timeval(v);
	}

	explicit utime_t(const double d) {
		set_from_double(d);
	}

	void set_from_double(const double d) {
//    tv.tv_sec = (uint32_t)trunc(d);
		tv.tv_usec = (uint32_t) ((d - (double) tv.tv_sec) * (double) 1000000.0);
	}

	// accessors
	time_t sec() const {
		return tv.tv_sec;
	}
	long usec() const {
		return tv.tv_usec;
	}
	int nsec() const {
		return tv.tv_usec * 1000;
	}

	// ref accessors/modifiers
	uint32_t& sec_ref() {
		return tv.tv_sec;
	}
	uint32_t& usec_ref() {
		return tv.tv_usec;
	}

	void copy_to_timeval(struct timeval *v) const {
		v->tv_sec = tv.tv_sec;
		v->tv_usec = tv.tv_usec;
	}
	void set_from_timeval(const struct timeval *v) {
		tv.tv_sec = v->tv_sec;
		tv.tv_usec = v->tv_usec;
	}
	/*
	 void encode(bufferlist &bl) const {
	 ::encode(tv.tv_sec, bl);
	 ::encode(tv.tv_usec, bl);
	 }
	 void decode(bufferlist::iterator &p) {
	 ::decode(tv.tv_sec, p);
	 ::decode(tv.tv_usec, p);
	 }
	 */
	/*
	 void encode_timeval(struct timespec *t) const {
	 t->tv_sec = tv.tv_sec;
	 t->tv_nsec = tv.tv_usec*1000;
	 }
	 void decode_timeval(const struct timespec *t) {
	 tv.tv_sec = t->tv_sec;
	 tv.tv_usec = t->tv_nsec / 1000;
	 }
	 */
	// cast to double
	operator double() {
		return (double) sec() + ((double) usec() / 1000000.0L);
	}
	/*
	 operator timespec() {
	 timespec ts;
	 ts.tv_sec = sec();
	 ts.tv_nsec = nsec();
	 return ts;
	 }
	 */
};

}

using common::utime_t;
using common::TimeRecord;
using common::TimeCheck;

