
#ifndef __YFS_STATIC_H__
#define __YFS_STATIC_H__

#include <stdint.h> 
#include "Common/Atomic.hpp"
#include "Common/Thread.hpp"
#include "Common/Time.hpp"
#include "Common/Debug.hpp"


struct Statistic
{
public:
	Statistic() : total(1) {}

public:

	void	Reset(int64_t _total) {
		iops.reset();
		size.reset();
		total = _total;
	}

	void	Inc(int klen, int dlen) { iops.inc(); size.inc(klen + dlen); }
	void	Dec(int klen, int dlen) { iops.dec(); size.dec(klen + dlen); }

	void	Inc(int64_t count, int klen, int dlen) { iops.inc(count); size.inc(klen + dlen); }
	void	Dec(int64_t count, int klen, int dlen) { iops.dec(count); size.dec(klen + dlen); }

	void	Loop(int64_t& last_iops, int64_t& last_size) {
		last_iops = iops.loop();
		last_size = size.loop();
	}

public:
	StatisUnit	iops;
	StatisUnit	size;
	int64_t 	total;
};

class StatisticThread : public common::ClientThread
{
public:
	StatisticThread()
		: mOutput(false), mEmpty(0), mTotal(0), mRecord(false), mSummaryTime(60000), mDisableEmpty(false) {}

public:

	void	Record(bool set);

	void	Reset(bool start = false) {
		mOutput = false;
		mEmpty = 0;
		mTotal = 0;
		mRecord = false;

		Record(start);
	}

	void	Summary(bool force = false);

protected:
	virtual void *entry();

	void	Update(int64_t count);

	int64_t	WaitTime() { return (int64_t)mWait.tv.tv_sec * c_time_level2 + mWait.tv.tv_usec; }

	void	DisableEmpty() { mDisableEmpty = true; }

protected:
	 bool	mOutput;
	 int	mEmpty;
	 int	mTotal;
	 bool	mRecord;

	 TimeCheck 	mSummaryTime;
	 TimeRecord	mTime;
	 bool	mDisableEmpty;
};

extern Statistic g_statis;
extern StatisticThread 	g_thread;

#endif //__YFS_STATIC_H__

