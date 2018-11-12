
#pragma once

#include <string>
#include <sstream>
#include <assert.h>
#include "Time.hpp"

/** used for printf convert */
#define UINT64	long long

/** must be success, or assert */
#define success(value)		do { if (!value) { ASSERT(value); } } while(0)

/** assert export */
#define	ASSERT(value)		__assert_fail(#value, __FILE__, __LINE__, __ASSERT_FUNCTION)

/**
 * assert string
 * */
#define assert_string(value) __assert_fail(value, __FILE__, __LINE__, __ASSERT_FUNCTION)

/**
 * assert and format
 **/
#define	assert_format(fmt, arg...)	do { 			\
	std::string _export = ::format(fmt, ##arg); 	\
	assert_string(_export.c_str()); 				\
} while(0)

/**
 * assert with system error
 * */
#define	assert_syserr(fmt, arg...)	do { 			\
	std::string _export = ::format(fmt ", ", ##arg);\
	assert_string((_export + get_syserr()).c_str());\
} while(0)

/**
 * @brief give raise to core
 */
#define	core_dump			do { int* x = 0; int y = *x; y += 1; } while(0)

/**
 * @brief printf c++ style info
 */
#define prints(s)			std::cout << s << std::endl

/**
 * reset pointer
 **/
template<class T>
void reset(T& ptr) {
	if (ptr) {
		delete ptr;
		ptr = NULL;
	}
}

/**
 * set bitmap
 **/
template<class T>
inline void set_bit(T state, T bit, bool set) {
	if (set) {
		state |= bit;
	} else {
		state &= ~bit;
	}
}

/**
 * get bit map
 **/
template<class T>
inline bool get_bit(T state, T bit) {
	return state & bit;
}

/**
 * number to string
 **/
template<class T>
std::string n2s(T v) {
    std::stringstream ss;
    ///< in case scientific notation
    ss.imbue(std::locale("C"));
    ss << v;
    return ss.str();
}

/**
 * string to number
 **/
template<class T>
T s2n(const std::string& s) {
    if (s.length() == 0) return 0;
    std::stringstream ss;
    T v;
    ss << s;
    ss >> v;
    return v;
}

/**
 * get formated system error string
 * */
std::string get_syserr(int err = 0, bool format = true);

/**
 * get elapse time, like [00:01:56.56781]
 * */
std::string string_elapse(const ctime_t last);

/**
 * get timer using, like 0, 100 ms, 10.567 ms, 10.456 s
 **/
std::string string_timer(uint64_t last, bool simple = false, bool us = true);

/**
 * get count string, like 1093, 89w, 89w
 **/
std::string	string_count(uint64_t count, bool space = true);

/**
 * get size string, like 893, 10 M, 20M, 10 K
 **/
std::string string_size(uint64_t size, bool space = true);

/**
 * get iops string, like 1089
 * */
std::string	string_iops(uint64_t iops, ctime_t last);

/**
 * get latancy, like 35.893 ms
 * */
std::string string_latancy(uint64_t iops, ctime_t last);

/**
 * get speed string, like 89.789 M/s, 984 B/s
 **/
std::string string_speed(uint64_t size, ctime_t last);

/**
 * get percent, like 89.1%
 * */
std::string string_percent(uint64_t size, uint64_t total);

/**
 * get current data, like 2015-11-09
 * */
std::string	string_date();

/**
 * get current time, like 12:25:40.16459 or 12:25:40
 * */
std::string	string_time(bool usec = false);

/**
 * @brief format string and return tstring
 */
std::string	format(const char* fmt, ...);

/**
 * @brief format string and return tstring
 * @param s the dst string
 * @return formated string s
 */
std::string& format(std::string& str, const char* fmt, ...);

/**
 * @brief trace data content
 */
std::string data_trace(const void* data, uint32_t len);

/**
 * dump display reset split
 **/
void		dump_reset(const std::string& split = " ");

/**
 * dump value if exist
 **/
std::string	dump_exist(bool exist, const std::string& value);

/**
 * dump last if exist something, with prefix and suffix
 **/
std::string	dump_last(const std::string& prefix = "", const std::string& suffix = "");

#if USING_UUID
/**
 * get uuid string
 **/
void		get_UUID(char* data);

/**
 * get uuid string
 **/
std::string	get_UUID();
#endif

