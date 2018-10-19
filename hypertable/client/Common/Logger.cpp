
#include <unistd.h>
#include <sys/types.h>
#include <fcntl.h>
#include <dirent.h>
#include <assert.h>
#include <cstdarg>
#include <sys/time.h>
#include <algorithm>
#include <vector>

#include "Logger.hpp"
#include "Util.hpp"
#include "Atomic.hpp"
#include "Mutex.hpp"
#include "Debug.hpp"
#include "File.hpp"
#include "String.hpp"

const char* c_log_suffix = ".log";

static Mutex s_log_mutex("log mutex", true);
Logging Logging::s_instance[LOG_INDEX_MAX] = {};

uint32_t default_log_handler(int logfd, LogLevel level, const char* filename, int line, const char* string);

const char*
level_name(int level)
{
	static const char* level_names[] = { "INVALID", "TRACE", "DEBUG",
		"INFO ", "WARN", "ERROR", "FATAL"};

	if (level <= LOG_LEVEL_NULL ||
		level >= LOG_LEVEL_FATAL) level = 0;
	return level_names[level];
}

struct Logging::LogVec : public std::vector<int>
{
};

Logging::Logging()
	: handle(default_log_handler), logvec(new LogVec)
{
}

Logging::~Logging()
{
	close();

	reset(logvec);
}

void
Logging::close()
{
	if (logfd) {
		::close(logfd);
		logfd = -1;
	}
	length = 0;
}

void
Logging::set_name(const std::string& name, const std::string& path, int index)
{
	Logging& logger = s_instance[index];
	/** only initialize once */
	assert(logger.index == LOG_INDEX_INVALID);

	if (name.length() == 0) {
		std::stringstream ss;
		ss << index << "_" << logger.name;
		logger.name = ss.str();

	} else {
		logger.name = name;
	}

	logger.path  = path;
	/** set as initialized */
	logger.index = index;
}

void
Logging::set_limit(int64_t size, int count, int index)
{
	Logging& logger = s_instance[index];

	logger.limit.size  = size;
	logger.limit.count = count;
}

void
Logging::initialize()
{
	if (initialized()) return ;
    initial = true;

	if (path.length() == 0) {
		path = common::split_path(common::module_path());
	}
	path = common::cut_end_slash(path);

	if (!common::make_path(path)) {
		assert_syserr("make path %s for Logging failed", path.c_str());
	}

	struct dirent storge, *entry;
	DIR *dir = opendir(path.c_str());
	if (dir == NULL) {
		assert_syserr("read Logging dir %s failed", path.c_str());
	}
	int index = 0;
	while (readdir_r(dir, &storge, &entry) == 0 && entry) {
		const std::string curr = entry->d_name;

		/** check file type */
		if (common::file_dir(path + "/" + entry->d_name)) {
			continue;

		/** check file name length and suffix */
		} else if (curr.length() < name.length() ||
			curr.length() < strlen(c_log_suffix) ||
			strncmp(curr.c_str(), name.c_str(), name.length()) != 0) {
			continue;
		}

		int beg = name.length();
		int end = curr.length() - strlen(c_log_suffix);
		std::string si = curr.substr(beg, end - beg);

		/** check file suffix */
		if (strncmp(curr.substr(end).c_str(), c_log_suffix, strlen(c_log_suffix)) != 0) {
			continue;

		} else if (!common::isdigit(si)) {
			continue;
		}

		index = si.length() == 0 ? 0 : s2n<int>(si);
		logvec->push_back(index);
	}
	closedir(dir);

	std::sort(logvec->begin(), logvec->end(), std::greater<int>());
}

std::string
Logging::get_path(int num)
{
    std::string log_name = name + c_log_suffix;
    std::string full = path.length() == 0 ? path : (path + "/");
	if (num == 0) {
		log_name = full + log_name;

	} else {
		log_name = full + n2s(num);
	}
	return log_name;
}

void
Logging::roll()
{
	close();

	for (auto iter = logvec->begin(); iter != logvec->end();) {
		int index = (*iter);
		std::string _old = get_path(index);
		std::string _new = get_path(index + 1);

		if (!common::file_exist(_old)) {
			iter = logvec->erase(iter);
			continue;

		} else if ((int64_t)logvec->size() >= limit.count) {

			if (!common::file_rm(_old)) {
				assert_syserr("remove old Logging %s failed", _old.c_str());
			}
			iter = logvec->erase(iter);
			continue;

		} else if (!common::file_move(_old, _new)) {
			assert_syserr("move Logging %s to %s failed", _old.c_str(), _new.c_str());
			iter = logvec->erase(iter);
			continue;
		}

		(*iter)++;
		iter++;
	}
	logvec->push_back(0);
}

void
Logging::log_format(LogLevel level, const char* filename, int line, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    char data[c_log_max_len];
	vsnprintf(data, c_log_max_len - 1, fmt, ap);
    va_end(ap);

    format(level, filename, line, data);
}

void
Logging::format(LogLevel level, const char* filename, int line, const char* string)
{
	if (logfd == -1) {
		Mutex::Locker lock(s_log_mutex);
		if (logfd == -1) {
		    initialize();
			logfd = ::open(get_path().c_str(), O_RDWR | O_CREAT | O_APPEND, 0);
			assert(logfd != -1);
			length = ::common::file_len(logfd);

			format(LOG_LEVEL_INFO, "", 0, "");
			format(LOG_LEVEL_INFO, "", 0, "");
			format(LOG_LEVEL_INFO, "", 0, "new start ==============================");
		}
	}

	int64_t writen = handle(logfd, level, filename, line, string);
	common::atomic_add64(&length, writen);

	if (length > limit.size) {
		Mutex::Locker lock(s_log_mutex);
		if (length > limit.size) {
			roll();
		}
	}
}

uint32_t
default_log_handler(int logfd, LogLevel level, const char* filename, int line, const char* string)
{
	char temp[Logging::c_log_max_len];
	char* data = temp;
	int length = 0;

	/** format time and loglevel */
	{
		struct timeval tv;
		gettimeofday(&tv, NULL);
		::time_t lt = tv.tv_sec;
		struct tm* crttime = localtime(&lt);
		assert(crttime != NULL);

		data += snprintf(data, 64, "[%02d-%02d-%02d %02d:%02d:%02d.%06ld] %-5s - ",
			crttime->tm_mon + 1, crttime->tm_mday, crttime->tm_year + 1900 - 2000,
			crttime->tm_hour, crttime->tm_min, crttime->tm_sec, tv.tv_usec,
			level_name(level));
	}

	/** format thread name */
	{
		const char* thread = thread_name();
		if (thread) {
			data += snprintf(data, 64, "[%s] ", thread);
		}
	}

	/** format display string */
	{
		length = (int)strlen(string);
		/** check remain length, */
		if (Logging::c_log_max_len - (int)(data - temp) < length) {
			length = Logging::c_log_max_len - (int)(data - temp);
		}
		/** format log data */
		memcpy(data, string, length);
		data += length;

		memcpy(data, "\n", 1);
		data += 1;
	}

	length = data - temp;
	write(logfd, temp, length);

	return length;
}

#if USING_TEST
	#include <unistd.h>

	void
	__write_log(int count)
	{
		set_thread("main", ThreadInfo::TT_pool);
		while (count-- >= 0) {
			usleep(1);
			log_info("write Logging " << pthread_self());
		}
	}

	void
	debug_write_log()
	{
		Logging::set_limit(1024 * 10, 3);
		batchs(30, __write_log, 1000);
	}
#endif


