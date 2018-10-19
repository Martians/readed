#include <vector>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <string.h>
#include <iomanip>
#include "ScanInstance.hpp"

using namespace std;

static int64_t s_scanned_objects = 0;
static int64_t s_random_get_objects = 0;
static int64_t s_random_read_objects_size = 0;
static pthread_mutex_t s_scan_lock;
static pthread_mutex_t s_random_lock;
std::vector<std::string> s_random_objects_set;
static int s_merged_threads = 0;
static int s_random_threads = 0;
static int s_sequential_threads = 0;
static bool s_random_start = false;

void *
RandomInstance::entry(void *args)
{
    RandomInstance *instance = NULL;
    instance = (RandomInstance *)args;
    instance->_DoRandomGetTest();
    return NULL;
}

void
RandomInstance::DoRandomGetTest()
{
    for(int i = 0; i < mThreads; i ++) {
        pthread_t thread_id;
        pthread_attr_t attr;
        memset(&attr, 0, sizeof(pthread_attr_t));
        pthread_attr_init(&attr);

        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&thread_id, &attr, entry, (void*) this);
        pthread_attr_destroy(&attr);
    }
}

void
RandomInstance::_DoRandomGetTest()
{
#if 0
    //int ret = 0;
    size_t select = 0;
    void *value = NULL;
    long long value_length = 0;
    bool random_start = false;

    while(1) {
        if(!s_random_start) {
            sleep(1);
            continue;
        }
        if(!random_start) {
            printf("random test start\r\n");
            random_start = true;
        }
        select = rand()%s_random_objects_set.size();
        //ret = get_object(s_random_objects_set[select].c_str(),
        get_object(s_random_objects_set[select].c_str(),
                s_random_objects_set[select].size(),
                &value,
                &value_length);
        free(value);
        value = NULL;
        pthread_mutex_lock(&s_random_lock);
        s_random_get_objects ++;
        pthread_mutex_unlock(&s_random_lock);
    }
#endif
}

ScanInstance::ScanInstance(int threads)
    :mMutex("scan_mutex")
{
    mThreads = threads;

    mPartitionCount = 0;
    mCurrentPartition = 0;
}

ScanInstance::~ScanInstance()
{
}

    void *
ScanInstance::entry(void *args)
{
    ScanInstance *instance = NULL;

    instance = (ScanInstance *)args;
    instance->_DoScanTest();
    return NULL;
}

    void
ScanInstance::_DoScanTest()
{
	#if 0
    int scanpartition = 0;
    std::string name;
    bool thread_exit = false;
    std::vector<std::string> random_objects_set;
    bool merged_to_mainstream;

    while(1) {
        if(thread_exit) {
            break;
        }
        {
            Mutex::Locker lock(mMutex);
            mCurrentPartition ++;
            if(mCurrentPartition >= mPartitionCount) {
                mCurrentPartition = 0;
            }
            scanpartition = mCurrentPartition;
        }
        name  = std::string("/") 
            + std::string(mPartitions[scanpartition].name, mPartitions[scanpartition].name_length) 
            + "/";
        while(1) {
            get_object_list(name.c_str(), name.size(), 1000, &objectlist, &objectlist_size);
            for(int i = 0; i < objectlist_size; i ++) {
                random_objects_set.push_back(std::string((char *)objectlist[i].name, objectlist[i].name_length));
                if((objectlist_size - 1) == i) {
                    name = std::string((char *)objectlist[i].name, objectlist[i].name_length);
                }
                free(objectlist[i].name);
                free(objectlist[i].value);
            }
            free(objectlist);
            if(0 == objectlist_size) {
                break;
            }
            pthread_mutex_lock(&s_scan_lock);
            s_scanned_objects += objectlist_size;
            if(s_scanned_objects >= s_random_read_objects_size
                    && s_random_read_objects_size != 0
                    && !merged_to_mainstream) {
                s_random_objects_set.insert(s_random_objects_set.end(), 
                        random_objects_set.begin(), 
                        random_objects_set.end());
                merged_to_mainstream = true;
                if(s_merged_threads < s_random_threads) {
                    printf("quit one sequential threads\r\n");
                    s_merged_threads ++;
                    thread_exit = true;
                    if(s_merged_threads == s_random_threads) {
                        s_random_start = true;
                    }
                    pthread_mutex_unlock(&s_scan_lock);
                    break;
                } else {
                    s_random_start = true;
                }
            }
            pthread_mutex_unlock(&s_scan_lock);
        }
    }
#endif
}

    void
ScanInstance::DoScanTest()
{
#if 0
    DataZone datazone;
    //int ret = 0;
    
    datazone.name = NULL;
    datazone.name_length = 0;
    //ret = list_partitions(datazone, &mPartitions, &mPartitionCount);
    list_partitions(datazone, &mPartitions, &mPartitionCount);
    cout << "partition list: " << endl;
    for(int i = 0; i < mPartitionCount; i ++) {
        cout << "\t" << std::string(mPartitions[i].name, mPartitions[i].name_length) << endl;
    }
    cout << "partition list end" << endl;

    for(int i = 0; i < mThreads; i ++) {
        pthread_t thread_id;
        pthread_attr_t attr;
        memset(&attr, 0, sizeof(pthread_attr_t));
        pthread_attr_init(&attr);

        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&thread_id, &attr, entry, (void*) this);
        pthread_attr_destroy(&attr);
    }
#endif
}

    void
GetTest(int sequential_threads,
        int random_threads,
        int64_t random_objects_size)
{
    ScanInstance *instance = NULL;
    pthread_mutexattr_t attr;
    RandomInstance *rinstance = NULL;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&s_scan_lock, &attr);
    pthread_mutex_init(&s_random_lock, &attr);
    pthread_mutexattr_destroy(&attr);

    s_scanned_objects = 0;
    s_merged_threads = 0;
    s_random_threads = random_threads;
    s_sequential_threads = sequential_threads;
    s_random_read_objects_size = random_objects_size;
    s_random_start = false;

    if(sequential_threads + random_threads > 0) {
        instance = new ScanInstance(sequential_threads + random_threads);
        instance->DoScanTest();
    }

    if(0 != random_threads
            && 0 != s_random_read_objects_size) {
        rinstance = new RandomInstance(random_threads);
        rinstance->DoRandomGetTest();
    }
}

    void
SequentialPrintStatics(int seconds)
{
    pthread_mutex_lock(&s_scan_lock);
    cout << setw(16) << "seq total: "
        << setw(8) << s_scanned_objects
        << setw(8) << "avg:"
        << setw(8) << s_scanned_objects/seconds
        << endl;
    pthread_mutex_unlock(&s_scan_lock);
}

    void
RandomPrintStatics(int seconds)
{
    pthread_mutex_lock(&s_scan_lock);
    cout << setw(16) << "random total: "
        << setw(8) << s_random_get_objects
        << setw(8) << "avg:"
        << setw(8) << s_random_get_objects/seconds
        << endl;
    pthread_mutex_unlock(&s_scan_lock);
}

