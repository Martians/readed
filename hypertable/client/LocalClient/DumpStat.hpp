
#pragma once

enum LogType
{
	LT_null = 0,
	LT_state,
	LT_table,
	LT_update,
};

#define state_info(x)	BASE_INFO(LT_state, x)
#define table_info(x)	BASE_INFO(LT_table, x)
#define update_info(x)	BASE_INFO(LT_update, x)

#define state_time_log(x)				state_info(time_thread_name() << x)
/** display time from last trace log */
#define state_time_trace(x) 			state_info(time_thread_name() << x << time_elapse())
/** only output when time exceed */
#define	state_time_trace_bomb(x) 		if (time_bomb()) { state_info(time_thread_name() << x << time_elapse(NULL, true)); }
//#define time_trace_bomb_time(x) log_debug(time_thread_name() << x << time_elapse())
/** output warning when timer exceed, or last trace log too long */
//#define time_trace_timer(x) 	log_debug(time_thread_name() << x << time_elapse(&timer))
/** never output waring */
#define state_time_trace_mute_warn(x)	state_info(time_thread_name() << x << time_elapse(NULL, false, true))

std::string	DumpTable();
std::string	DumpLogger();
std::string DumpUpdate();
std::string DumpTask(bool timer = false);
void 		SetTimerThread();


