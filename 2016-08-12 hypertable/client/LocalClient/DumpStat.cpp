
#include "../Common/Debug.hpp"
#include "../Common/Util.hpp"
#include "DumpStat.hpp"

#include <Hypertable/RangeServer/RangeServer.h>

namespace Hypertable {
namespace Apps {
	RangeServer* g_range = NULL;
}}

void
display_table()
{
	table_info(DumpTable());
}

std::string DumpUpdateUnexpect();
void
display_update()
{
	update_info(DumpUpdate());

#if 0
	std::string unexpect = DumpUpdateUnexpect();
	if (unexpect.length()) {
		update_info(unexpect);
	}
#endif
}

void
display_state()
{
	char temp[64 * 1024] = {0};
	char* data = temp;
	data += snprintf(data, sizeof(temp), "%s, task %s",
			DumpLogger().c_str(), DumpTask(true).c_str());
	state_info(temp);
}

void
SetTimerThread()
{
	Logging::set_name("output", "/var/opt/hypertable/log/");
	Logging::set_name("state",	"/var/opt/hypertable/log/", LT_state);
	Logging::set_name("table", 	"/var/opt/hypertable/log/", LT_table);
	Logging::set_name("update", "/var/opt/hypertable/log/", LT_update);

	//set_log_level(LOG_LEVEL_TRACE);
	static common::ClientThread s_table(3000,  display_table);
	static common::ClientThread s_state(1000,  display_state);
	static common::ClientThread s_update(1000, display_update);
	s_table.start();
	s_state.start();
	s_update.start();
}

std::string
DumpTable()
{
	TableInfoMapPtr table = Hypertable::Apps::g_range->GetTableInfoMap();
	return table->DumpStat();
}

std::string
DumpUpdateUnexpect()
{
	char temp[1024 * 64] = {0};
	char* data = temp;

	const int64_t limit = m2u(500);
	if (update_last(ST_transfer_time) > limit ||
		update_last(ST_table_lock, ST_range_metalog) > limit ||
		update_last(ST_cancel_size, ST_range_error))
	{
		int index = ST_table_lock;
		int64_t last = 0;
		if (update_last(ST_table_lock, ST_range_metalog) > limit) {
			for (int i = ST_table_lock; i <= ST_range_metalog; i++) {
				if (update_last(i) > last) {
					last = update_last(i);
					index = i;
				}
			}
		}
		data += snprintf(data, sizeof(temp), ", transfer: %4s, lock: %d time: %6s, cancel: %6s",
				string_timer(update_last(ST_transfer_time), true).c_str(),
				//StringTimer(update_last(ST_table_lock, ST_range_metalog)).c_str(),
				index - ST_table_lock, string_timer(last).c_str(),
				string_size(update_last(ST_cancel_size)).c_str());

		if (update_last(ST_range_shrink)) {
			data += snprintf(data, sizeof(temp), ", shrink");
			//(UINT64)update_last(ST_range_shrink)
		}
		//(UINT64)update_last(ST_range_error)
	}
	return temp;
}

std::string
DumpUpdate()
{
	char temp[1024 * 64];
	char* data = temp;

	static TimeRecord s_timer;
	s_timer.check();

	file_loop(ST_file_loop, ST_file_max);
	update_loop(ST_update_loop, ST_update_max);

	data += snprintf(data, sizeof(temp), "req: %8s, commit: %8s, done: %8s, "//, resp: %8s, "
			"sync: %6s, read: %8s, write %8s",
			string_size(update_count(ST_request_size)).c_str(),
			string_size(update_count(ST_commit_size)).c_str(),
			string_size(update_last(ST_commit_done_size)).c_str(),
			//StringSize(update_last(ST_respons_size)).c_str(),
			string_timer(file_last(ST_sync_time), true).c_str(),
			string_speed(file_last(ST_read), s_timer.last()).c_str(),
			string_speed(file_last(ST_write), s_timer.last()).c_str());

	strcat(data, DumpUpdateUnexpect().c_str());
	return temp;
}

std::string
DumpLogger()
{
	LogStat system;
	if (Global::system_log) {
		Global::system_log->DumpStat(&system);
	}

	if (Global::metadata_log) {
		Global::metadata_log->DumpStat(&system);
	}

	if (Global::root_log) {
		Global::root_log->DumpStat(&system);
	}

	if (Global::user_log) {
		char data[4096] = {0};
		//snprintf(data, sizeof(data), ", other: %d, size: %s",
		//	system.count, StringSize(system.size).c_str());
		return Global::user_log->DumpStat() + data;
	}
	return "";
}

std::string
DumpTask(bool timer)
{
#if 0
	const int total = 10;
	char stat[total] = {0};
	if (timer) {
		memset(stat, '*', total);

		int len = 0;
		if (replay || split || compact || access) {
			len = (int)(replay + split + compact + compact);
			len = std::min(total - 1, len);
		}
		stat[len] = '\0';
	}
#endif
	char temp[4096];
	char* data = temp;

	range_loop(ST_range_loop, ST_range_max);
	data += snprintf(data, sizeof(temp), "[%lld - split %lld compact %lld-%-2lld,"
			"done: %8s, read %8s, write %8s]",
			(UINT64)range_count(ST_normal), (UINT64)range_count(ST_split), (UINT64)range_count(ST_compact),
			(UINT64)(range_count(ST_compact_major, ST_compact_gc) + range_last(ST_compact_done)),
			string_size(range_last(ST_done_rw) * 2 + range_last(ST_done_mem)).c_str(),
			string_size(range_count(ST_try_rw)).c_str(),
			string_size(range_count(ST_try_rw, ST_try_mem)).c_str());

	if (range_last(ST_replay_size)) {
		data += snprintf(data, sizeof(temp), " replay: %s, %s",
			string_size(range_last(ST_replay_size)).c_str(),
			string_timer(range_last(ST_replay_time)).c_str());
	}
	//(UINT64)range_last(ST_relinquish)
	return temp;
}


