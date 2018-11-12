
#pragma once

#include <sstream>
#include "Type.hpp"

/**
 * log level enum
 * */
enum LogLevel {
	LOG_LEVEL_NULL	  = 0,
	LOG_LEVEL_TRACE   = 1,
	LOG_LEVEL_DEBUG   = 2,
	LOG_LEVEL_INFO    = 3,
	LOG_LEVEL_WARN	  = 4,
	LOG_LEVEL_ERROR   = 5,
	LOG_LEVEL_FATAL   = 6,
};

enum LogIndex {
	LOG_INDEX_INVALID = -1,
	LOG_INDEX_START	= 0,
	LOG_INDEX_MAX	= 10,
};

/** log handle type */
typedef uint32_t (*log_handle_t)(int logfd, LogLevel level, const char* filename, int line, const char* string);

/**
 * base logger
 **/
class Logging
{
public:
	Logging();
	virtual ~Logging();

public:
	/** max single log length */
	static const int c_log_max_len = 64 * 1024;

	/**
	 * log limit
	 **/
	struct LogLimit {
		int64_t size  = { 4 * c_length_1G };
		int64_t count = { 1 };
	};

	/** logger instance */
	static Logging s_instance[LOG_INDEX_MAX];

public:
	/**
	 * set current log level
	 */
	static void	set_level(LogLevel level, int index = LOG_INDEX_START) {
		s_instance[index].level = level;
	}

	/**
	 * set current log name
	 */
	static void	set_name(const std::string& name = "", const std::string& path = "", int index = LOG_INDEX_START);

	/**
	 * set current log size
	 * */
	static void set_limit(int64_t size, int count = 0, int index = LOG_INDEX_START);

public:
	/**
	 * set current log name
	 */
	void		log_name(const std::string& _name, bool suffix = true);

	/**
	 * get current log name
	 **/
	const std::string& log_name() { return name; }

	/**
	 * get current log handler
	 * */
	log_handle_t log_handler() { return handle; }

	/**
	 * set current log handle
	 **/
	void		log_handle(log_handle_t _handle) { handle = _handle; }

	/**
	 * set current log level
	 */
	void		log_level(LogLevel _level) { level = _level; }

	/**
	 * get current log level
	 **/
	LogLevel 	log_level() { return level; }

	/**
	 * log format helper
	 **/
	void		log_format(LogLevel level, const char* filename, int line, const char *fmt, ...);

	/**
	 * format logger
	 **/
	void		format(LogLevel level, const char* filename, int line, const char* string);

protected:

    struct LogVec;

	/**
	 * get logger path
	 **/
	std::string get_path(int num = 0);

	/**
	 * initialize logger
	 **/
	void		initialize();

	/**
	 * get initialize state
	 **/
	bool		initialized() { return initial; }

	/**
	 * roll logger
	 **/
	void		roll();

	/**
	 * close logger
	 **/
	void		close();

protected:
	/** initialize state */
	bool		initial = { false };
	/** logger index */
	int			index  	= {LOG_INDEX_INVALID};
	/** open file handle */
	int			logfd	= {-1};
	/** log file length */
	int64_t		length	= {0};
	/** log handle */
	log_handle_t handle;
	/** log path */
	std::string	path;
	/** log name */
	std::string	name	= { "output" };
	/** log level */
	LogLevel 	level	= { LOG_LEVEL_DEBUG };
	/** log limit */
	LogLimit	limit;
	/** log file vector */
	LogVec*		logvec;
};

/** set log level helper */
#define set_log_level(level, index) \
    Logging::set_level(LOG_LEVEL_##level, index)

/** c style log mode */
#define __VAR_LOG(index, level, fmt, arg...)				\
	do {													\
		Logging& logger = Logging::s_instance[index];		\
		if (LOG_LEVEL_##level >= logger.log_level()) {		\
			logger.log_format(LOG_LEVEL_##level,			\
                    __FILE__, __LINE__, fmt, ##arg);		\
		}													\
    } while(0)

/** cpp style log mode */
#define __BASE_LOG(index, level, display) 					\
	do {													\
		Logging& logger = Logging::s_instance[index];		\
		if (LOG_LEVEL_##level >= logger.log_level()) {		\
			std::stringstream __stream;						\
			__stream << display;							\
			logger.format(LOG_LEVEL_##level,				\
				__FILE__, __LINE__, __stream.str().c_str());\
		}													\
	} while(0)

/** helper for log index start */
#define VAR_LOG(level, fmt, arg...)		__VAR_LOG(LOG_INDEX_START, fmt, ##arg)
#define BASE_LOG(level, display)		__BASE_LOG(LOG_INDEX_START, level, display)

/** helper for log level */
#define VAR_TRACE(index, fmt, arg...)	__VAR_LOG(index, TRACE, fmt, ##arg)
#define VAR_DEBUG(index, fmt, arg...)	__VAR_LOG(index, DEBUG, fmt, ##arg)
#define VAR_INFO(index, fmt, arg...)	__VAR_LOG(index, INFO,  fmt, ##arg)
#define VAR_WARN(index, fmt, arg...)	__VAR_LOG(index, WARN,  fmt, ##arg)
#define VAR_ERROR(index, fmt, arg...)	__VAR_LOG(index, ERROR, fmt, ##arg)
#define VAR_FATAL(index, fmt, arg...)	__VAR_LOG(index, FATAL, fmt, ##arg)

#define BASE_TRACE(index, info)		__BASE_LOG(index, TRACE, info)
#define BASE_DEBUG(index, info)		__BASE_LOG(index, DEBUG, info)
#define BASE_INFO(index, info)		__BASE_LOG(index, INFO,  info)
#define BASE_WARN(index, info)		__BASE_LOG(index, WARN,  info)
#define BASE_ERROR(index, info)		__BASE_LOG(index, ERROR, info)
#define BASE_FATAL(index, info) 	__BASE_LOG(index, FATAL, info)

	#define var_trace(fmt, arg...) 	VAR_TRACE(LOG_INDEX_START, fmt, ##arg)
	#define var_debug(fmt, arg...)	VAR_DEBUG(LOG_INDEX_START, fmt, ##arg)
	#define var_info(fmt, arg...)	VAR_INFO(LOG_INDEX_START,  fmt, ##arg)
	#define var_warn(fmt, arg...)	VAR_WARN(LOG_INDEX_START,  fmt, ##arg)
	#define var_error(fmt, arg...)	VAR_ERROR(LOG_INDEX_START, fmt, ##arg)
	#define var_fatal(fmt, arg...) 	VAR_FATAL(LOG_INDEX_START, fmt, ##arg)

	#define log_trace(info) 		BASE_TRACE(LOG_INDEX_START, info)
	#define log_debug(info)	 		BASE_DEBUG(LOG_INDEX_START, info)
	#define log_info(info)	 		BASE_INFO(LOG_INDEX_START,  info)
	#define log_warn(info)	 		BASE_WARN(LOG_INDEX_START,  info)
	#define log_error(info)	 		BASE_ERROR(LOG_INDEX_START, info)
	#define log_fatal(info) 		BASE_FATAL(LOG_INDEX_START, info)

