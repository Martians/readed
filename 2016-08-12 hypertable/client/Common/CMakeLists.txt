
cmake_minimum_required(VERSION 2.8)

OPTION(USING_TEST "complie all test" OFF)
OPTION(USING_UUID "using uuid" ON)
OPTION(CHECK_GENERATE "check generate data" OFF)


set(COMMON_CXX_FLAGS "-g -Wall -D_REENTRANT --std=c++11")
set(CMAKE_CXX_FLAGS "${COMMON_CXX_FLAGS} ${CMAKE_CXX_FLAGS}")

file(GLOB SRCS *.cpp)
set(COMMON_LIB "-lpthread")

IF(USING_TEST)
	add_definitions(-DUSING_TEST=1)
ENDIF(USING_TEST)

IF(USING_UUID)
	add_definitions(-DUSING_UUID=1)
	set(COMMON_LIB "${COMMON_LIB} -luuid")	
ENDIF(USING_UUID)

IF(CHECK_GENERATE)
	add_definitions(-DCHECK_GENERATE_DATA=1)
ENDIF(CHECK_GENERATE)

IF(USING_TEST)
	add_executable(test Main.cxx)
    add_dependencies(test common)
    target_link_libraries(test common) 
ENDIF(USING_TEST)

add_library(common ${SRCS})
target_link_libraries(common ${COMMON_LIB})


# -rdynamic
# -Wl,-Bdynamic -Wl,-Bstatic
# -Wl,-rpath=. 
