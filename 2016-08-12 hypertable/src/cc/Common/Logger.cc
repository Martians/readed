/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 3
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/** @file
 * Logging routines and macros.
 * The LogWriter provides facilities to write debug, log, error- and other
 * messages to stdout. The Logging namespaces provides std::ostream-
 * and printf-like macros and convenience functions.
 */

#include <Common/Compat.h>

#include "String.h"
#include "Logger.h"

#include <iostream>
#include <stdio.h>
#include <stdarg.h>
#include <mutex>

#if TESTING_SUPPORT
#include <cstdarg>
#include <sys/time.h>

const char*
i_last_name(const char* name)
{
    const char* last = strrchr(name, '/');
    return last ? last + 1 : name;
}
#endif

namespace Hypertable { namespace Logger {

static String logger_name;
static LogWriter *logger_obj = 0;
static std::mutex mutex;

void initialize(const String &name) {
  logger_name = name;
}

LogWriter *get() {
  if (!logger_obj)
    logger_obj = new LogWriter(logger_name);
  return logger_obj;
}

void LogWriter::log_string(int priority, const char* filename, int line, const char *message) {
  static const char *priority_name[] = {
    "FATAL",
    "ALERT",
    "CRIT ",
    "ERROR",
    "WARN ",
    "NOTICE",
    "INFO ",
    "DEBUG",
    "NOTSET"
  };

  std::lock_guard<std::mutex> lock(mutex);
  if (m_test_mode) {
    fprintf(m_file, "%s %s : %s\n", priority_name[priority], m_name.c_str(),
            message);
  }
  else {
#if !TESTING_SUPPORT
    time_t t = ::time(0);
    fprintf(m_file, "%u %s %s : %s\n", (unsigned)t, priority_name[priority],
            m_name.c_str(), message);
#endif
    struct timeval tv;
	gettimeofday(&tv, NULL);
	::time_t lt = tv.tv_sec;
	struct tm* crttime = localtime(&lt);

	if (show_line_numbers() && line != 0) {
		fprintf(m_file, "[%02d-%02d-%02d %02d:%02d:%02d.%06ld] %s : %s [%s:%d]\n",
				crttime->tm_mon + 1, crttime->tm_mday, crttime->tm_year + 1900 - 2000,
				crttime->tm_hour, crttime->tm_min, crttime->tm_sec, tv.tv_usec,
				priority_name[priority], message, i_last_name(filename), line);
	} else {
		fprintf(m_file, "[%02d-%02d-%02d %02d:%02d:%02d.%06ld] %s : %s\n",
				crttime->tm_mon + 1, crttime->tm_mday, crttime->tm_year + 1900 - 2000,
				crttime->tm_hour, crttime->tm_min, crttime->tm_sec, tv.tv_usec,
				priority_name[priority], message);
	}
  }

  flush();
}

void LogWriter::log_varargs(int priority, const char* filename, int line, const char *format, va_list ap) {
  char buffer[1024 * 16];
  vsnprintf(buffer, sizeof(buffer), format, ap);
  log_string(priority, filename, line, buffer);
}

void LogWriter::debug(const char* filename, int line, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  log_varargs(Priority::DEBUG, filename, line, format, ap);
  va_end(ap);
}

void LogWriter::log(int priority, const char* filename, int line, const char *format, ...) {
  va_list ap;
  va_start(ap, format);
  log_varargs(priority, filename, line, format, ap);
  va_end(ap);
}

}} // namespace Hypertable::
