
#pragma once

#include "Logger.hpp"
#include "Util.hpp"
#include "Atomic.hpp"
#include "Thread.hpp"
#include <thread>
#include <vector>

/**
 * record statistic
 **/
struct StatisUnit
{
public:
	StatisUnit() : _count(0), _total(0), _last(0) {}

public:
	/**
	 * reset state
	 * */
	void	reset() {
		_total = 0;
		_count = 0;
		_last  = 0;
	}

	/**
	 * inc by 1
	 **/
	void 	inc() { common::atomic_inc64(&_count); }

	/**
	 * dec by 1
	 **/
	void	dec() { common::atomic_dec64(&_count); }

	/**
	 * inc count
	 **/
	int64_t	inc(int64_t v) { return common::atomic_add64(&_count, v); }

	/**
	 * dec count
	 **/
	int64_t	dec(int64_t v) { return common::atomic_add64(&_count, -v); }

	/**
	 * loop count
	 **/
	int64_t	loop() {
		_last = common::atomic_swap64(&_count, 0);
		_total += _last;
		return _last;
	}

	/**
	 * get last count
	 **/
	int64_t	last() { return _last; }

	/**
	 * get current count
	 **/
	int64_t	count() { return _count; }

	/**
	 * get total count
	 **/
	int64_t total() { return _total; }

public:
	int64_t _count;
	int64_t _total;
	int64_t	_last;
};

struct GlobalStat
{
	enum Type {
		GT_null = 0,
		GT_max = 30,
	};

	enum Index {
		GI_null = 0,
		GI_max = 30,
	};
	static StatisUnit unit[GT_max][GI_max];
};

#define STAT_FUNC_COUNT(name, index)				\
	inline int64_t name##_count(int type) {			\
	return GlobalStat::unit[index][type].count();	\
}
#define STAT_FUNC_COUNT_RANGE(name, index)			\
	inline int64_t name##_count(int beg, int end) {	\
	int64_t	total = 0;								\
	for (int type = beg; type <= end; type++) {		\
		total += GlobalStat::unit[index][type].count();	\
	}												\
	return total;									\
}
#define STAT_FUNC_LAST(name, index)					\
	inline int64_t name##_last(int type) {			\
	return GlobalStat::unit[index][type].last();	\
}
#define STAT_FUNC_LAST_RANGE(name, index)			\
	inline int64_t name##_last(int beg, int end) {	\
	int64_t	total = 0;								\
	for (int type = beg; type <= end; type++) {		\
		total += GlobalStat::unit[index][type].last();\
	}												\
	return total;									\
}
#define STAT_FUNC_INC(name, index)					\
	inline void name##_inc(int type) {				\
	GlobalStat::unit[index][type].inc();			\
}
#define STAT_FUNC_DEC(name, index)					\
	inline void name##_dec(int type) {				\
	GlobalStat::unit[index][type].dec();			\
}
#define STAT_FUNC_INC_COUNT(name, index)			\
	inline void name##_inc(int type, int64_t count) {\
	GlobalStat::unit[index][type].inc(count);		\
}
#define STAT_FUNC_DEC_COUNT(name, index)			\
	inline void name##_dec(int type, int64_t count) {\
	GlobalStat::unit[index][type].dec(count);		\
}
#define STAT_FUNC_LOOP(name, index)					\
	inline int64_t name##_loop(int type) {			\
	GlobalStat::unit[index][type].loop();			\
	return GlobalStat::unit[index][type].last();	\
}
#define STAT_FUNC_LOOP_RANGE(name, index)			\
	inline void name##_loop(int beg, int end) {		\
	for (int type = beg; type <= end; type++) {		\
		GlobalStat::unit[index][type].loop();		\
	}												\
}
#define STAT_FUNC_NEXT(name, index, step)			\
	inline void name##_next(int type) {				\
	GlobalStat::unit[index][type + step].dec();		\
	GlobalStat::unit[index][type].inc();			\
}
#define STAT_FUNC_NEXT_COUNT(name, index, step)		\
	inline void name##_next(int type, int64_t count) {\
	GlobalStat::unit[index][type + step].dec(count);\
	GlobalStat::unit[index][type].inc(count);		\
}
#define DEFINE_STATA_FUNC(name, index, step)\
		STAT_FUNC_COUNT(name, index)		\
		STAT_FUNC_COUNT_RANGE(name, index)	\
		STAT_FUNC_LAST(name, index)			\
		STAT_FUNC_LAST_RANGE(name, index)	\
		STAT_FUNC_INC(name, index)			\
		STAT_FUNC_DEC(name, index)			\
		STAT_FUNC_INC_COUNT(name, index)	\
		STAT_FUNC_DEC_COUNT(name, index)	\
		STAT_FUNC_LOOP(name, index)			\
		STAT_FUNC_LOOP_RANGE(name, index)	\
		STAT_FUNC_NEXT(name, index, step)	\
		STAT_FUNC_NEXT_COUNT(name, index, step)

/**
 * thread vectro for testing
 **/
class ThreadVector : public std::vector<std::thread*>
{
public:
	ThreadVector() {}
	virtual ~ThreadVector() {
		wait();

		for (const auto& thread : *this) {
			delete thread;
		}
		clear();
	}

public:
	static void wait() {
		for (auto& thread : s_pool) {
			if (thread->joinable()) {
				thread->join();
			}
		}
	}
	static ThreadVector s_pool;
};

template<class...Types>
void single(Types...args)
{
	std::thread* thread = new std::thread(args...);
	ThreadVector::s_pool.push_back(thread);
}

template<class ... Types>
void batchs(int count, Types...args)
{
	for (int i = 0; i < count; i++) {
		single(args...);
	}
}

/**
 * record stadge timer
 * */
struct StadgeTimer {

public:
	void	next() {
		if (total >= s_count) return;
		time[total++] = record.check();
	}

	ctime_t	operator [] (int i) {
		return i < total ? time[i] : 0;
	}

	ctime_t	get() {
		if (index == -1) index = 0;
		if (index >= total) return 0;
		return time[index++];
	}

	ctime_t	rget() {
		if (index == -1) index = total - 1;
		if (index < 0 ) return 0;
		return time[index--];
	}

public:
	static const int s_count = 20;

	TimeRecord 	record;
	ctime_t 	time[s_count];
	int			index {-1};
	int			total {};
};

class ThreadInfo
{
public:
	ThreadInfo(int _type = TT_normal, const char* _name = "", pthread_t _tid = 0)
		: type(_type), name(_name), tid(_tid) {}

	ThreadInfo(const ThreadInfo& v) {
		operator = (v);
	}

	const ThreadInfo& operator = (const ThreadInfo& v) {
		type = v.type;
		name = v.name;
		tid  = v.tid;
		time = v.time;
		return *this;
	}

	enum ThreadType
	{
		TT_null = 0,
		TT_main,
		TT_pool,
		TT_normal,
	};

public:
	/**
	 * do time check
	 **/
	ctime_t	check() { return time.check(); }

	/**
	 * get last time
	 **/
	ctime_t	last() { return time.last(); }

	/**
	 * check if thread need record time
	 **/
	bool	record() { return type != TT_normal; }

public:
	int		 	type;
	std::string name;
	pthread_t 	tid;
	TimeRecord 	time;
};

/** define current thread */
extern thread_local ThreadInfo* t_current;

/**
 * set current thread info
 **/
void	set_thread(const char* name = "", int type = ThreadInfo::TT_normal);

/**
 * get current thread name
 * */
inline const char* thread_name() {
	return !t_current ? NULL : t_current->name.c_str();
}

/**
 * reset thread timer
 * */
inline void	time_reset() {
	if (t_current) {
		t_current->check();
	}
}

/**
 * get thread last time
 * */
inline ctime_t time_last(bool check = false) {
	if (t_current && t_current->record()) {
		return check ? t_current->check() : t_current->last();
	}
	return 0;
}

/**
 * display thread last using
 * */
std::string	time_using(bool mute_warn = false);

/**
 * check if thread need bombed
 **/
bool		time_bombed();

/**
 * macro for time trace
 * */
#define time_log_using(x, log) 		time_reset(); log(x << time_using())
#define time_trace_using(x)			time_log_using(x, log_trace)
#define time_debug_using(x)			time_log_using(x, log_debug)

#define time_log_mute_warn(x, log)	time_reset(); log(x << time_using(true))
#define time_trace_mute_warn(x)		time_log_mute_warn(x, log_trace)
#define time_debug_mute_warn(x)		time_log_mute_warn(x, log_debug)

#define	time_bomb(x) 				if (time_bombed()) { log_debug(x << time_using()); }

/**
 * macro for time record
 **/
#define CREATE_TIMER				TimeRecord timer
#define	UPDATE_TIMER				timer.check();
#define EXPORT_TIMER				", use " << StringTimer(timer.Check())

#define STADGE_TIMER				StadgeTimer stadge
#define	STADGE_UPDATE				stadge.next()
#define STADGE_STRING				string_timer(stadge.rget())


enum _Update {

	ST_request,
	ST_request_cell,
	ST_request_size,

	ST_commit,
	ST_commit_cell,
	ST_commit_size,

	ST_respons,
	ST_respons_cell,
	ST_respons_size,

	ST_update_loop,

	ST_commit_done,
	ST_commit_done_cell,
	ST_commit_done_size,

	ST_cancel,
	ST_cancel_cell,
	ST_cancel_size,
	ST_range_shrink,
	ST_range_error,

	ST_transfer,
	ST_transfer_size,
	ST_transfer_time,

	ST_table_lock,
	ST_range_lock,
	ST_range_barrier,
	ST_range_metalog,

	ST_update_max,
};

enum _File {

	ST_file_loop,

	ST_read,
	ST_write,
	ST_sync,
	ST_sync_time,
	ST_remove,
	ST_remove_time,
	ST_rmdir,
	ST_rmdir_time,

	ST_file_max,
};

enum _Range {

	ST_take,
	ST_load,
	ST_normal,

	ST_compact,
	ST_split,
	ST_relinquish,
	ST_replay,

	ST_try_rw,
	ST_try_mem,

	ST_compact_major,
	ST_compact_minor,
	ST_compact_merge,
	ST_compact_gc,

	ST_range_loop,

	ST_compact_done,
	ST_done_rw,
	ST_done_mem,

	ST_replay_size,
	ST_replay_time,

	ST_range_max,
};

enum {
	GT_file = 1,
	GT_update,
	GT_range,
};
DEFINE_STATA_FUNC(update, GT_update, -3)
DEFINE_STATA_FUNC(file,   GT_file, 0)
DEFINE_STATA_FUNC(range,  GT_range, -1)
