
add_subdirectory(Common)

cmake_minimum_required(VERSION 2.8)

set(HYPER_CXX_FLAGS "-g -Wall -D_REENTRANT --std=c++11")
set(CMAKE_CXX_FLAGS "${HYPER_CXX_FLAGS} ${CMAKE_CXX_FLAGS}")

include_directories("../src/cc" 
	"/usr/local/include/thrift" 
	"/usr/local/BerkeleyDB.4.8/include")
include_directories(. ./Common ./ObjectTest)

add_definitions(-DUSING_UUID=1)
set(DEVEL_HOME "/root/devel/")
link_directories(${DEVEL_HOME}/static_lib ${DEVEL_HOME}/hyper_lib)

set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
set(CMAKE_INSTALL_RPATH ./hyper_lib:/root/work/hyper/build/lib;/root/work/hyper/build/src/cc/Hypertable/Lib:/root/work/hyper/build/src/cc/FsBroker/Lib:/root/work/hyper/build/src/cc/Hyperspace:/root/work/hyper/build/src/cc/Tools/Lib:/root/work/hyper/build/src/cc/AsyncComm:/root/work/hyper/build/src/cc/Common:/root/work/hyper/build/src/cc/ThriftBroker/)
set(CMAKE_INSTALL_RPATH ${CMAKE_INSTALL_RPATH}:./hyper_lib:../hyper_lib:/root/devel/hyper_lib:/root/hyper_lib:/opt/hypertable/current/lib:/usr/local/lib)

file(GLOB SRCS *.cpp)

add_executable(client ${SRCS})
add_dependencies(client common)

OPTION(ORIGIN_SUPPORT "support origin" ON)

IF(ORIGIN_SUPPORT)
	add_definitions(-DORIGIN_SUPPORT=1)
	target_link_libraries(client -Wl,-rpath=./hyper_lib -Wl,-Bdynamic -lHyperThrift -lHypertable -lHyperComm -lHyperspace -lHyperFsBroker -lHyperTools -lHyperCommon 
		-lthrift -Wl,-Bdynamic -lboost_system -lboost_program_options -lboost_filesystem -lboost_thread -lboost_iostreams
	 	-Wl,-Bdynamic -ltcmalloc_minimal -lsigar-amd64-linux -levent -lcurses -lpthread -luuid -lre2 -lexpat common)
	
ELSE(ORIGIN_SUPPORT)
	target_link_libraries(client -Wl,-rpath=./hyper_lib -Wl,-Bdynamic -lHyperCommon -lHyperThrift -lHypertable 
		-lthrift -Wl,-Bdynamic -lboost_system -lboost_program_options -lboost_filesystem 
	 	-Wl,-Bdynamic -ltcmalloc_minimal -lsigar-amd64-linux -levent -lcurses -lpthread -luuid -lcommon)
ENDIF(ORIGIN_SUPPORT)


#target_link_libraries(client -Wl,-rpath=./lib -Wl,-rpath=./hyper_lib -Wl,-Bstatic -lHyperCommon -lHyperThrift -lHypertable 
#	-lthrift -Wl,-Bstatic -lboost_system -lboost_program_options -lboost_filesystem 
# 	-Wl,-Bdynamic -ltcmalloc_minimal -lsigar-amd64-linux -levent -lcurses -lpthread -luuid)


