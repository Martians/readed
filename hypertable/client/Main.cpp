
#include "ClientTest.hpp"
#include "Util.hpp"
#include "Logger.hpp"
#include "String.hpp"
#include "Statistic.hpp"

#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

using namespace std;
namespace po = boost::program_options;

void
help(po::options_description& opdesc)
{
	cout << opdesc << endl;
	exit(-1);
}

void
fix_unit(int& v, std::string& s)
{
	int unit = 1;
	if (s.length() >= 2) {
		std::string::size_type pos;;
		if ((pos = s.find_first_of("Kk"))
			!= std::string::npos)
		{
			unit = 1024;
		} else if ((pos = s.find_first_of("Mm"))
			!= std::string::npos)
		{
			unit = 1024 * 1024;
		}
	}
	v = s2n<int>(s) * unit;

	/** set as invalid data */
	if (v == 0 && s.length() == 0) {
		v = -1;
	}
}

void
get_range(const string& value, int& v_min, int& v_max)
{
	int max, min;
	std::string s1 = value;
	std::string s2 = common::string_next(s1);
	fix_unit(min, s1);
	fix_unit(max, s2);

	v_min = std::min(max, min);
	v_max = std::max(max, min);
	if (v_min == -1) {
		v_min = v_max;
	}
}

int
main(int argc, char* argv[])
{
	ClientConfig config;

	po::options_description opdesc("options");
	opdesc.add_options()
		("help", "help")
		//("config,c", po::value<string>(&config)->default_value(""), "config file path")
		//("clear", 		"clear all table")
		("host,h", 		po::value<string>(&config.global.host)->default_value(config.global.host), "server address")
		("thread,t", 	po::value<int>(&config.global.thread)->default_value(0), "thread count")
		("seed", 		po::value<int>(&config.global.seed)->default_value(0), "default 0 for random seed")
		("logger", 		"use specific logger name for option")
		("thrift", 		"use thrift api")
		("single",		"use single or mutator op for thrift api")
		("type",		po::value<int>(&config.test.type)->default_value(config.test.type), "test type, 0: put, 1: get, 3: scan")
		("total", 		po::value<uint64_t>(&config.test.total)->default_value(config.test.total), "total count")
		("batch,b", 	po::value<int>(&config.test.batch)->default_value(config.test.batch), "batch request")
		("seqn_key", 	"seqn key")
		("uuid_key", 	"uuid key")
		("uuid_data", 	"uuid data")
		("char_data", 	"use char as data")
		("key_size,k", 	po::value<string>()->default_value("40"), "key size range")
		("data_size,d", po::value<string>()->default_value("4K"), "data size range")
		("verify,v", 	po::value<int>(&config.test.verify)->default_value(0), "verify count")
		("", "")
		("no_flush", 	"mutator put no flush")
		("no_sync_log", "update log not sync")
		//("no_log", 		"mutator no log")
		("flush_interval", po::value<int>(&config.mutator.flush_interval)->default_value(0), "mutator flush interval")
		("", "")
		("drop_table", 	"drop old table data")
		//("diff_table", 	"use diff table every thread")
		//("diff_group", 	"use diff group every thread")
		("block_size", 	po::value<string>()->default_value("0"), "store block size")
		("memory", 		"using memory table")
		("commit_interval", po::value<int>(&config.table.commit_interval)->default_value(0), "commit interval")
		("output", 		po::value<string>(&config.load.output), "output data to file");
		//("load_data", 	po::value<string>(&config.test.load_data), "load data infile")
		//("dump_data", 	po::value<string>(&config.test.dump_data), "dump data infile");

	po::variables_map vm;
	try {
		po::store(po::parse_command_line(argc, argv, opdesc), vm);
		po::notify(vm);

	} catch (exception& e) {
		cout << "client: " << e.what() << "\n";
		help(opdesc);
	}

	if (vm.count("help")) {
		help(opdesc);
		return -1;
	}

	if (vm.count("thrift")) {
		config.global.origin = false;
	}

	/*
	if (vm.count("clear")) {
		config.global.clear = true;
	}
	*/

	if (vm.count("single")) {
		config.global.single = true;
	}

	if (vm.count("drop_table")) {
		config.table.drop_table = true;
	}

	if (vm.count("diff_table")) {
		config.table.diff_table = true;
	}

    /*
	if (vm.count("diff_group")) {
		config.table.diff_group = true;
	}
	*/

	if (vm.count("seqn_key")) {
		config.test.rand_key = false;
	}

	if (vm.count("uuid_key")) {
		config.test.uuid_key = true;
	}

	if (vm.count("uuid_data")) {
		config.test.uuid_data = true;
	}

	if (vm.count("char_data")) {
		config.test.char_data = true;
	}

	if (vm.count("key_size")) {
		get_range(vm["key_size"].as<string>(),
			config.test.key_min,
			config.test.key_max);
	}

	if (vm.count("data_size")) {
		get_range(vm["data_size"].as<string>(),
			config.test.data_min,
			config.test.data_max);
	}

	if (vm.count("no_flush")) {
		config.mutator.no_flush = true;
	}

	if (vm.count("no_sync_log")) {
		config.mutator.no_sync_log = true;
	}

	if (vm.count("no_log")) {
		config.mutator.no_log = true;
	}

	if (vm.count("memory")) {
		config.table.memory = true;
	}

	if (vm.count("block_size")) {
		string value = vm["block_size"].as<string>();
		fix_unit(config.table.block_size, value);
	}

	if (config.load.output.length()) {
		config.test.type = ClientConfig::CT_load;
	}

	/** looger must put in the end */
	if (vm.count("logger")) {
        Logging::set_name("output_" + config.DumpTest("", true));
	}

	ClientManage manage;
	manage.Start(config);
	manage.Stop();
	return 0;
}
