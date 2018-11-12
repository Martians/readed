
#include "Debug.hpp"
#include "Container.hpp"

thread_local ThreadInfo* t_current = NULL;
/** thread vector instance */
ThreadVector ThreadVector::s_pool;
/** global stat for recording */
StatisUnit GlobalStat::unit[GT_max][GI_max];

static ctime_t c_time_warn_level 	= 50000;
static ctime_t c_time_warn_level_1	= 500000;
static ctime_t c_time_warn_level_2	= 2000000;
static ctime_t c_time_warn_level_3	= 4000000;
static ctime_t c_time_warn_level_4	= 10000000;

int
pool_thread_index(const char* name)
{
	static TypeMap<std::string, int> s_pool_index;
	int* index = s_pool_index.get(name);
	if (index) {
		(*index)++;
		return *index;

	} else {
		s_pool_index.add(name, 0);
	}
	return 0;
}

void
set_thread(const char* name, int type)
{
	if (t_current) {
		return;
	}

	static Mutex s_mutex("time thread", true);
	Mutex::Locker lock(s_mutex);

	char data[64];
	if (type == ThreadInfo::TT_main) {
		static TypeSet<std::string> s_main_index;
		memcpy(data, name, strlen(name));

		std::string str(name);
		success(s_main_index.add(str));

	} else if (type == ThreadInfo::TT_pool) {
		snprintf(data, 64, "%s-%02d", name, pool_thread_index(name));

	} else {
		type = ThreadInfo::TT_normal;
		static int s_normal_index = 0;
		snprintf(data, 64, "%04d", s_normal_index++);
	}
	name = data;

	assert(t_current == NULL);
	t_current = new ThreadInfo(type, name, pthread_self());
}

bool
need_warn(ctime_t time)
{
	return time >= c_time_warn_level;
}

bool
time_bombed()
{
	ctime_t time = time_last(true);
	if (time > 0 && need_warn(time)) {
		return true;
	}
	return false;
}

const char*
time_warn_string(ctime_t last)
{
	if (last > c_time_warn_level) {

		if (last > c_time_warn_level_4) {
			return " ========== delay - 4 DISASTER ==========\n\n\n\n\n"
				   " ========== delay - 4 DISASTER ==========";

		} else if (last > c_time_warn_level_3) {
			return " ========== delay - 3 FATAL ==========";

		} else if (last > c_time_warn_level_2) {
			return " ========== delay - 2 WARN ==========";

		} else if (last > c_time_warn_level_1) {
			return " ========== delay - 1 INFO ==========";

		} else {
			return " ========== delay - 0 ==========";
		}
	}
	return "";
}

std::string
time_using(bool mute_warn)
{
	ctime_t time = time_last(false);
	if (time > 0) {

		char data[256] = {0};
		snprintf(data, 256, "  - %.3f ms", (float)time/1000);

		if (!mute_warn && need_warn(time)) {
			strcat(data, time_warn_string(time));
		}
		return data;
	}
	return "";
}

#if USING_TEST
	#include <unistd.h>

	void
	__test_thread(int type = ThreadInfo::TT_normal, const char* name = "", ctime_t sleep = 0)
	{
		set_thread(type, name);
		usleep(sleep);
		time_debug_using("thread " << pthread_self());
	}

	void
	debug_test_thread()
	{
		single(__test_thread, ThreadInfo::TT_main, "main", 0);
		single(__test_thread, ThreadInfo::TT_pool, "pool", 0);
		single(__test_thread, ThreadInfo::TT_pool, "pool", 0);
		single(__test_thread, ThreadInfo::TT_pool, "pool", 100);
		single(__test_thread, ThreadInfo::TT_normal, "", 30);
		single(__test_thread, ThreadInfo::TT_normal, "", 200);
		single(__test_thread, ThreadInfo::TT_normal, "", 0);

		batchs(10, __test_thread, ThreadInfo::TT_pool, "pool", 0);
	}
#endif
