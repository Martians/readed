
#include "String.hpp"
#include "Util.hpp"
#include "Debug.hpp"

void debug_test_thread();
void debug_write_log();

int
main(int argc, char* argv[])
{
	debug_test_thread();
	debug_write_log();

    return 0;
}
