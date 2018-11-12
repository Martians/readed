
#include "Statistic.hpp"
#include "Common/Util.hpp"
#include "Common/Logger.hpp"

Statistic g_statis;
StatisticThread g_thread;

void
StatisticThread::Update(int64_t iops)
{
	if (iops == 0) {
		mOutput = false;
		mEmpty++;
		mTotal++;

	} else {
		mOutput = true;
		mEmpty = 0;
	}
}

void
StatisticThread::Summary(bool force)
{
	if (force || (mRecord && mSummaryTime.timeout())) {
		log_info("=================================================");
		var_info("Empty: %3d total: %s size: %s, using: %s   iops: %s throughput: %s", mTotal,
				string_count(g_statis.iops.total()).c_str(),
				string_size(g_statis.size.total()).c_str(),
				string_elapse(mTime.elapse()).c_str(),
				string_iops(g_statis.iops.total(), mTime.elapse()).c_str(),
				string_speed(g_statis.size.total(), mTime.elapse()).c_str());
		log_info("=================================================");
	}
}

void
StatisticThread::Record(bool set)
{
	/** dump origin statistic */
	if (g_statis.iops.total() || g_statis.iops.count()) {
		mTime.check();
		int64_t iops = 0;
		int64_t size = 0;
		g_statis.Loop(iops, size);

		Summary(true);
		log_info("");
	}

	if (set) {
		mTime.begin();
		mRecord = true;

	} else {
		mRecord = false;
	}
}

void*
StatisticThread::entry()
{
	int64_t iops = 0;
	int64_t size = 0;

	while (mRunning) {
		wait();

		if (mRecord) {
			mTime.check();
			g_statis.Loop(iops, size);

			//if (iops > 0 || mOutput) {
			if (iops > 0) {
				var_info("%s iops: %6s,  lan: %9s,  output: %9s"
					"    total: %6s, size: %s,  iops: %5s, speed: %s [%s]",
					string_elapse(mTime.elapse()).c_str(),
					string_iops(iops, WaitTime()).c_str(),
					string_latancy(iops, WaitTime()).c_str(),
					string_speed(size, WaitTime()).c_str(),

					string_count(g_statis.iops.total()).c_str(),
					string_size(g_statis.size.total()).c_str(),
					string_iops(g_statis.iops.total(), mTime.elapse()).c_str(),
					string_speed(g_statis.size.total(), mTime.elapse()).c_str(),
					string_percent(g_statis.iops.total(), g_statis.total).c_str());

			} else if (!mDisableEmpty) {
				var_info("empty: %2d ... ", mEmpty);
			}
			Update(iops);

			Summary();
		}
	}
	Summary(true);
	return NULL;
}
