#include <unistd.h>
#include <getopt.h>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>
#include "UUIDTest.hpp"
#include "PutTest.hpp"
#include "ScanInstance.hpp"
#include "ProcessControl.h"

HyperClient g_client;
using namespace boost;
namespace bpo = boost::program_options;

void Help()
{
    printf("\r\nUsage:  <-c cfgfile> [-h]\n"
            "Options:\n"
            "\t-c cfgfile   use given config file\n"
            "\t-h           display this help and exit\n");
    exit(1);
}

    int64_t
SizeToInt(std::string &sizestr)
{
    int64_t value = 0;
    boost::trim(sizestr);
    if(sizestr.empty()) {
        return -1;
    }

    if(sizestr[sizestr.size()-1] != 'b'
            && sizestr[sizestr.size()-1] != 'B'
            && sizestr[sizestr.size()-1] != 'k'
            && sizestr[sizestr.size()-1] != 'K'
            && sizestr[sizestr.size()-1] != 'm'
            && sizestr[sizestr.size()-1] != 'M'
            && sizestr[sizestr.size()-1] != 'g'
            && sizestr[sizestr.size()-1] != 'G'
            && sizestr[sizestr.size()-1] != 't'
            && sizestr[sizestr.size()-1] != 'T'
            && !isdigit(sizestr[sizestr.size() -1])) {
        return -1;
    } else {
        if(sizestr[sizestr.size()-1]== 'b'
                || sizestr[sizestr.size()-1] == 'B'
                || isdigit(sizestr[sizestr.size()-1])) {
            value = 1;
        } else if (sizestr[sizestr.size()-1]== 'k'
                || sizestr[sizestr.size()-1] == 'K') {
            value = 1024;
        } else if (sizestr[sizestr.size()-1]== 'm'
                || sizestr[sizestr.size()-1] == 'M') {
            value = 1048576;
        } else if (sizestr[sizestr.size()-1]== 'g'
                || sizestr[sizestr.size()-1] == 'G') {
            value = (1<<30);
        } else if (sizestr[sizestr.size()-1]== 't'
                || sizestr[sizestr.size()-1] == 'T') {
            value = 1;
            value = (value << 40);
        }
    }

    for(size_t i = 0; i < sizestr.size()-1; i ++) {
        if(!isdigit(sizestr[i])) {
            return -1;
        }
    }
    if(!isdigit(sizestr[sizestr.size()-1])) {
        sizestr = sizestr.substr(0, sizestr.size()-1);
    }
    value = value * atoll(sizestr.c_str());

    return value;
}

    std::string
ExtractOneElemnt(std::string &element_list)
{
    std::string element;

    do {
        boost::trim(element_list);
        if(element_list[0] != '('
                || element_list[element_list.size()-1] != ')') {
            break;
        }
        element = element_list.substr(0, element_list.find_first_of(")"));
        element = element + ")";
        if(element.size() == element_list.size()) {
            element_list = "";
        } else {
            element_list = element_list.substr(element_list.find_first_of(")") + 1);
        }
    }while(0);

    return element;
}

/**
 * (30:[0-1k])
 * */
    int
GetTestConfig(PutTestConfig &config, std::string &elementstr)
{
    std::string temp;

    boost::trim(elementstr);
    if(elementstr.size() <= 2
            || elementstr[0] != '('
            || elementstr[elementstr.size()-1] != ')') {
        return -1;
    }
    elementstr = elementstr.substr(1, elementstr.size()-2);
    temp = elementstr.substr(0, elementstr.find_first_of(':'));
    config.mPercentage = SizeToInt(temp);
    if(-1 == config.mPercentage) {
        return -1;
    }
    elementstr = elementstr.substr(elementstr.find_first_of(':'));
    if(elementstr.size() <= 3) {
        return -1;
    }
    elementstr = elementstr.substr(1);
    boost::trim(elementstr);
    if(elementstr.size() <= 2 
            || elementstr[0] != '['
            || elementstr[elementstr.size()-1] != ']') {
        return -1;
    }
    elementstr = elementstr.substr(1, elementstr.size()-2);
    boost::trim(elementstr);
    if(elementstr.find_first_of('-') == std::string::npos
            || elementstr.find_first_of('-') == elementstr.size()-1) {
        return -1;
    }
    temp = elementstr.substr(0, elementstr.find_first_of('-'));
    config.mMin = SizeToInt(temp);
    temp = elementstr.substr(elementstr.find_first_of('-')+1);
    config.mMax = SizeToInt(temp);
    if(-1 == config.mMin
            || -1 == config.mMax) {
        return -1;
    }

    return 0;
}

/**
 * putparam: groups:{(percent:[min_size-maxsize])(percent:[min_size-maxsize])}
 * example: 4:{(30:[0-1k])(60:[1k~10k])(10:[10k-1M])}
 * */
void
TryToDoPutTest(string &putparam)
{
    int ret = 0;
    std::string temp, temp1;
    int64_t groups = 0;
    std::vector<PutTestConfig> puttests;
    bool sequential_test = false;

    do {
        boost::trim(putparam);
        if(putparam.empty()) {
            break;
        }
        temp = putparam.substr(0, putparam.find_first_of(":"));
        if(temp.empty() 
                || temp.size() == putparam.size()) {
            break;
        }
        temp1 = boost::trim_copy(temp);
        groups = SizeToInt(temp1);        
        if(0 == groups
                || -1 == groups) {
            break;
        }
        if(temp.size() + 1 >= putparam.size()) {
            break;
        }
        temp = putparam.substr(temp.size()+1);
        if(temp.empty()) {
            break;
        }
        boost::trim(temp);
        if(temp.size() <= 2
                || (temp[0] != 's' && temp[0] != 'r')
                || temp[1] != ':') {
            break;
        }
        if(temp[0] == 's') {
            sequential_test = true;
        } else {
            sequential_test = false;
        }
        temp = temp.substr(2);
        boost::trim(temp);
        if(temp.size() <= 2
                || temp[0] != '{'
                || temp[temp.size()-1] != '}') {
            break;
        }
        temp = temp.substr(1, temp.size()-2);
        boost::trim(temp);
        if(temp.size() <= 2
                || temp[0] != '('
                || temp[temp.size()-1] != ')') {
            break;
        }
        while(1) {
            PutTestConfig testconfig;
            std::string elementstr;
            elementstr = ExtractOneElemnt(temp);
            if(elementstr.empty()) {
                break;
            }
            ret = GetTestConfig(testconfig, elementstr);
            if(0 != ret) {
                break;
            }
            puttests.push_back(testconfig);
        }
        PutTest(groups, sequential_test, puttests);
    }while(0);

    if(0 == groups
            || 0 == puttests.size()) {
        cout << "invalid put test parameter" << endl;
    }

    return;
}

/**
 * sequential_threads:random_threads:random_object_set_size
 * 1:2:100M
 * */
    void
TryToDoGetTest(std::string &getparam)
{
    std::string temp;
    int sequential_threads = 0, random_threads = 0;
    int64_t random_object_size = 0;

    do {
        boost::trim(getparam);
        if(getparam.size() < 5) {
            break;
        }
        temp = getparam.substr(0, getparam.find_first_of(":"));
        if(temp.size() == getparam.size()
                || temp.size() == getparam.size()-1) {
            break;
        }
        getparam = getparam.substr(temp.size()+1);
        sequential_threads = SizeToInt(temp);
        
        temp = getparam.substr(0, getparam.find_first_of(":"));
        if(temp.size() == getparam.size()
                || temp.size() == getparam.size()-1) {
            break;
        }
        getparam = getparam.substr(temp.size()+1);
        random_threads = SizeToInt(temp);

        random_object_size = SizeToInt(getparam);
        if(0 != sequential_threads
                || 0 != random_threads) {
            GetTest(sequential_threads, random_threads, random_object_size);
        }
    }while(0);

    if(0 == sequential_threads
            && 0 == random_threads) {
        cout << "invalid get test parameter" << endl;
    }

    return;
}

int StartObjectService(int argc, char *argv[])
{
    std::string config_path;
    UUIDTest uuidtest;
    std::string putparam;
    std::string getparam;
    int printinterval = 1;
    int totalseconds = 0;

    bpo::options_description opdesc("options description");
    opdesc.add_options()
        ("help,h", "produce help message")
        ("uuid_test,u", "test uuid generate speed")
        ("config,c", bpo::value<string>(&config_path)->default_value("config"), "path of the configuration")
        ("put,p", bpo::value<string>(&putparam)->default_value(""), "test put object speed groups:{(percentage:[min_size-maxsize])(percentage:[min_size-maxsize])}")
        ("get,g", bpo::value<string>(&getparam)->default_value(""), "test get objcect speed sequentital_threads:random_threads:random_object_set_size")
        ("print_interval,i", bpo::value<int>(&printinterval)->default_value(1), "interval to print statics");
    bpo::variables_map vm;
    bpo::store(bpo::parse_command_line(argc, argv, opdesc), vm);
    bpo::notify(vm);
    if(vm.count("help")) {
        cout << opdesc << endl;
        return -1;
    }


    if(vm.count("uuid_test")) {
        uuidtest.Create();
    } else {
    	g_client.Test();
        //start_object_service(config_path.c_str());
        if(!putparam.empty()) {
            TryToDoPutTest(putparam);
        }
        if(!getparam.empty()) {
            TryToDoGetTest(getparam);
        }
    }

    while(1) {
        sleep(printinterval);
        totalseconds += printinterval;
        PutPrintStatics(totalseconds);
        SequentialPrintStatics(totalseconds);
        RandomPrintStatics(totalseconds);
    }

}

#if 0
int
callonce(int argc, char *argv[])
{
    std::string config_path;
    int putthreads = 1;
    bpo::options_description opdesc("options description");
    opdesc.add_options()
        ("help,h", "produce help message")
        ("uuid_test,u", "test uuid generate speed")
        ("config,c", bpo::value<string>(&config_path)->default_value("config"), "path of the configuration")
        ("put,p", "test put object speed")
        ("put_threads,t", bpo::value<int>(&putthreads)->default_value(1), "count of the put threads");
    bpo::variables_map vm;
    bpo::store(bpo::parse_command_line(argc, argv, opdesc), vm);
    bpo::notify(vm);
    if(vm.count("help")) {
        cout << opdesc << endl;
        return -1;
    }
    if(vm.count("put")) {
    } else if(vm.count("uuid_test")) {
    }
    cout << "call onece done, config path: " << config_path << endl;
    return 0;
}

int
call_twice(int argc, char *argv[])
{
    std::string config_path;
    int putthreads = 1;
    bpo::options_description opdesc("options description");
    opdesc.add_options()
        ("help,h", "produce help message")
        ("uuid_test,u", "test uuid generate speed")
        ("config,c", bpo::value<string>(&config_path)->default_value("config"), "path of the configuration")
        ("put,p", "test put object speed")
        ("put_threads,t", bpo::value<int>(&putthreads)->default_value(1), "count of the put threads");
    bpo::variables_map vm;
    bpo::store(bpo::parse_command_line(argc, argv, opdesc), vm);
    bpo::notify(vm);
    if(vm.count("help")) {
        cout << opdesc << endl;
        return -1;
    }
    if(vm.count("put")) {
    } else if(vm.count("uuid_test")) {
    }
    cout << "call twice done, config path: " << config_path << endl;
    return 0;
}
#endif

#if 0
int
main(int argc, char *argv[])
{
    int ret = 0;
    if(0 == ret) {
        process_start(argc, argv, StartObjectService);
    } else {
        fprintf(stderr, "create log failed"); 
        exit(1);
    }

    return ret;
}
#endif

/* vim:set ts=4 sw=4 expandtab : */
