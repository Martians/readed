
#include "Util.hpp"
#include "Type.hpp"
#include <assert.h>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include "Time.hpp"
#include <cstdarg>

#if USING_UUID
#	include <uuid/uuid.h>
#endif

std::string
get_syserr(int err, bool format)
{
	if (err == 0) err = errno;
	else if (err < 0) err = -err;

    if (format) {
        char data[256];
        snprintf(data, sizeof(data), "sys err: %d - %s",
                 err, strerror(err));
        return data;
    } else {
        return strerror(err);
    }
}

std::string
string_elapse(const ctime_t last)
{
	char data[64];
	ctime_t st = last / c_time_level2;
	ctime_t tp = st % 3600;

	snprintf(data, sizeof(data), "[%02d:%02d:%02d.%06ld]",
		(int)st/3600, (int)tp/60, (int)tp%60, last % c_time_level2);
	return data;
}

std::string
string_timer(uint64_t last, bool simple, bool us)
{
	char data[64];

	const char* level = "ms";
	if (last > c_time_level2) {
		last = last / c_time_level1;

		simple = false;
		level = "s";
	}

	if (last == 0) {
		return "0";

	} else if (us) {
		if (simple) {
			snprintf(data, 64, "%lld %s", (UINT64)last/c_time_level1, level);

		} else {
			snprintf(data, 64, "%.3f %s", (float)last/c_time_level1, level);
		}

	} else {
		snprintf(data, 64, "%lld %s", (UINT64)last, level);
	}
	return data;
}

std::string
string_count(uint64_t count, bool space)
{
	char data[64];
	if (space) {
		if (count >= 100000000)	   		snprintf(data, sizeof(data), "%3.2f E", (float)count/100000000);
		else if(count >= 10000)  		snprintf(data, sizeof(data), "%3.1f W", (float)count/10000);
		else						   	snprintf(data, sizeof(data), "%3lld",   (long long int)count);
	} else {
		if (count >= 100000000)	   		snprintf(data, sizeof(data), "%lldE",	(long long int)count/100000000);
		else if(count >= 10000)  		snprintf(data, sizeof(data), "%lldW", 	(long long int)count/10000);
		else						   	snprintf(data, sizeof(data), "%lld", 	(long long int)count);
	}
	return data;
}

std::string
string_size(uint64_t size, bool space)
{
	char data[64];
	if (space) {
		if (size >= c_length_1E) 		snprintf(data, sizeof(data), "%.3f T",(float)size/c_length_1E);
		else if(size >= c_length_1G)	snprintf(data, sizeof(data), "%.3f G",(float)size/c_length_1G);
		else if(size >= c_length_1M) 	snprintf(data, sizeof(data), "%.2f M",(float)size/c_length_1M);
		else if(size >= c_length_1K) 	snprintf(data, sizeof(data), "%.2f K",(float)size/c_length_1K);
		else							snprintf(data, sizeof(data), "%d",   (uint32_t)size);
	} else {
		if (size >= c_length_1E) 		snprintf(data, sizeof(data), "%lldT", (long long int)size/c_length_1E);
		else if(size >= c_length_1G)	snprintf(data, sizeof(data), "%lldG", (long long int)size/c_length_1G);
		else if(size >= c_length_1M) 	snprintf(data, sizeof(data), "%lldM", (long long int)size/c_length_1M);
		else if(size >= c_length_1K) 	snprintf(data, sizeof(data), "%lldK", (long long int)size/c_length_1K);
		else							snprintf(data, sizeof(data), "%lld",  (long long int)size);
	}
	return data;
}

std::string
string_iops(uint64_t iops, ctime_t last)
{
	last = std::max(last, (ctime_t)1);

	if (iops == 0) {
		return "0";
	}
	char data[64];
	snprintf(data, sizeof(data), "%lld", (long long int)(iops * c_time_level2 / last));
	return data;
}

std::string
string_latancy(uint64_t iops, ctime_t last)
{
	iops = std::max(iops, (uint64_t)1);

	char data[64];
	snprintf(data, sizeof(data), "%2.3f ms", (float)last / c_time_level1 / iops);
	return data;
}

std::string
string_speed(uint64_t size, ctime_t last)
{
	last = std::max(last, (ctime_t)1);
	float speed = (float)size * c_time_level2 / last;

	char data[64];
	if (speed >= c_length_1M)	  	snprintf(data, sizeof(data), "%3.2f M/s", speed/c_length_1M);
	else if(speed >= c_length_1K)  	snprintf(data, sizeof(data), "%3.1f K/s", speed/c_length_1K);
	else						   	snprintf(data, sizeof(data), "%3.1f B/s", speed);
	return data;
}

std::string
string_percent(uint64_t size, uint64_t total)
{
	total = std::max(total, size);

	if (size == 0) {
		return "0";
	}
	char data[64];
	snprintf(data, sizeof(data), "%2.1f%%", (float)size * 100 / total);
	return data;
}

std::string
string_date()
{
	/** format time */
	struct timeval tv;
	gettimeofday(&tv, NULL);
	::time_t lt = tv.tv_sec;
	struct tm* crttime = localtime(&lt);
	assert(crttime != NULL);

	char data[64];
	snprintf(data, 64, "%02d-%02d-%02d",
			crttime->tm_mon + 1, crttime->tm_mday, crttime->tm_year + 1900 - 2000);
	return data;
}

std::string
string_time(bool usec)
{
	/** format time */
	struct timeval tv;
	gettimeofday(&tv, NULL);
	::time_t lt = tv.tv_sec;
	struct tm* crttime = localtime(&lt);
	assert(crttime != NULL);

	char data[64];
	if (usec) {
		snprintf(data, 64, "%02d:%02d:%02d.%06d",
				crttime->tm_hour, crttime->tm_min, crttime->tm_sec, (int)tv.tv_usec);
	} else {
		snprintf(data, 64, "%02d:%02d:%02d",
				crttime->tm_hour, crttime->tm_min, crttime->tm_sec);
	}
	return data;
}

std::string
format(const char * fmt,...)
{
	char data[4096];
	va_list args;
	va_start(args, fmt);
	vsnprintf(data, 4096 , fmt, args);
	va_end(args);
	return data;
}

std::string&
format(std::string& str, const char * fmt,...)
{
	char data[4096];
	va_list args;
	va_start(args, fmt);
	vsnprintf(data, 4096 , fmt, args);
	va_end(args);
	str = data;
	return str;
}

std::string
data_trace(const void* buffer, uint32_t len)
{
	char data[4096];
	std::string str;
	for(uint32_t index = 0; index < len; index++){
		snprintf(data, sizeof(data), "<%-3d %2d. ", index, *((char*)buffer + index));
		if (index % 8 == 0 && index != 0) {
			str += "\n";
		}
		str += data;
	}
	return str;
}

static std::string s_dump;
static int s_index = 0;
static std::string s_split = " ";

void
dump_reset(const std::string& split)
{
	s_index = 0;
    s_dump  = "";
	s_split = split;
}

std::string
dump_exist(bool exist, const std::string& value)
{
	if (!exist) return "";

	s_dump = value + (s_index > 0 ? s_split : "") + s_dump;
	s_index++;
	return "";
}

std::string
dump_last(const std::string& prefix, const std::string& suffix)
{
	if (s_index == 0) return s_dump;
	s_dump = prefix + s_dump + suffix;

	std::string temp = s_dump;
	dump_reset(s_split);
	return temp;
}

#if 0
int
ywb_hexdecodechar(char v)
{
	if(v >= '0' && v <= '9'){
		return v - '0';
	}
	if(v >= 'a' && v <= 'z'){
		return v - 'a' + 10;
	}
	if(v >= 'A' && v <= 'Z'){
		return v - 'A' + 10;
	}
	return -1;
}

int
ywb_hexdecodeint(const char *buf, int buflen, int l, ywb_uint64_t *v)
{
	if(NULL == v){
		return -1;
	}
	*v = 0;
	if(buflen > 16){
		buflen = 16;
	}
    int i = 0;
	for(i = 0; i < buflen; i ++){
		int a = ywb_hexdecodechar(buf[i]);
		if(a < 0){
			return YWB_EINVAL;
		}
		*v = ((*v) << 4) | ywb_hexdecodechar(buf[i]);
	}
	return 0;
}

int
ywb_hexencode(const ywb_byte_t *d, ywb_size_t d_len, char *buf, ywb_size_t buflen)
{
	if(buflen < 2 * d_len){
		return YWB_EINVAL;
	}
    ywb_size_t i = 0;
	for(i = 0; i < d_len; i ++){
		ywb_hexencodeint(1, d[i], buf + (i << 1), 2);
	}
	return 0;
}
#endif

#if USING_UUID
std::string
get_UUID()
{
    uuid_t uuid;

    char data[64];
	uuid_generate(uuid);
	uuid_unparse(uuid, data);
    return data;
}

void
get_UUID(char* data)
{
    uuid_t uuid;
	uuid_generate(uuid);
	uuid_unparse(uuid, data);
}
#endif

