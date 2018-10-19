#ifndef __RW_134a3112_5559_159d1512_SCANINSTANCE_HPP__
#define __RW_134a3112_5559_159d1512_SCANINSTANCE_HPP__

#include "Mutex.hpp"

class RandomInstance
{
    public:
        RandomInstance(int threads)
        :mThreads(threads)
        {
        }
        
        static void *entry(void *args);
        void DoRandomGetTest();

    private:
        void _DoRandomGetTest();

        int mThreads;
};

class ScanInstance
{
    public:
        ScanInstance(int threads);
        ~ScanInstance();

        static void *entry(void *args);
        void DoScanTest();

    private:
        void _DoScanTest();

    private:
        int         mThreads;
        //Partition   *mPartitions;
        int         mPartitionCount;
        int         mCurrentPartition;
        Mutex       mMutex;
};

void GetTest(int sequential_threads,
        int random_threads,
        int64_t random_objects_size);
void SequentialPrintStatics(int seconds);
void RandomPrintStatics(int seconds);

#endif

