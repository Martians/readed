#include <vector>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <pthread.h>
#include <sys/time.h>
#include <uuid/uuid.h>
#include <string.h>
#include <iomanip>
#include <assert.h>
#include <sstream>
#include "PutTest.hpp"

using namespace std;

struct PutStatics
{
    int64_t mMin;
    int64_t mMax;
    int64_t mOps;
};

static PutStatics *s_put_ops_list = NULL;
int s_put_ops_list_size = 0;
pthread_mutex_t s_ops_lock;
static char *random_buffer = NULL;

PutInstance::PutInstance(PutGroup *group, int limit, int min, int max)
    :mGroup(group)
     , mStageLimit(limit)
     , mMin(min)
     , mMax(max)
{
    mCurrentStagePutDone = 0;
    mSequentialKey = 0;
}

PutInstance::~PutInstance()
{
}

    void
PutInstance::IncreaseTotalOps()
{
    pthread_mutex_lock(&s_ops_lock);
    s_put_ops_list[mIndex].mOps++;
    pthread_mutex_unlock(&s_ops_lock);
}

    void
PutInstance::LockForBalance()
{
    mGroup->LockForBalance();
}

    bool
PutInstance::OthersDone()
{
    return mGroup->OthersDone(this);
}

    void
PutInstance::WakeupOthersForBalance()
{
    mGroup->WakeupOthersForBalance();
}

    void
PutInstance::WaitForBalance()
{
    mGroup->WaitForBalance();
}

    void
PutInstance::UnlockForBalance()
{
    mGroup->UnlockForBalance();
}

    void *
PutInstance::entry(void *args)
{
    PutInstance *test = NULL;
    
    test = (PutInstance*)args;

    test->Test();

    return NULL;
}

    void
PutInstance::Test()
{
    while(1) {
        PutOneObject();
        mCurrentStagePutDone ++;
        IncreaseTotalOps(); 
        if(mCurrentStagePutDone >= mStageLimit) {
            LockForBalance();
            if(OthersDone()) {
                WakeupOthersForBalance();
            } else {
                WaitForBalance();
            }
            mCurrentStagePutDone = 0;
            UnlockForBalance();
        }
    }
}

    void
PutInstance::PutOneObject()
{
    uuid_t uuid;
    char buffer[39];
    int size = 0;

    snprintf(buffer, 39, "/./");
    if(!SequentialTest()) {
        uuid_generate(uuid);
        uuid_unparse(uuid, buffer + 3);
    } else {
        snprintf(buffer + 3, 36, "%36d", mSequentialKey);
        mSequentialKey ++;
    }
    if(mMin != mMax) {
        size = rand() % (mMax-mMin) + mMin;
    } else {
        size = mMin;
    }

    //put_object(buffer, 39, random_buffer, size);
    g_client.Put(string(buffer, 39), string(random_buffer, size));
}

    void
PutInstance::Create()
{
    pthread_t thread_id;
    pthread_attr_t attr;
    memset(&attr, 0, sizeof(pthread_attr_t));
    pthread_attr_init(&attr);

    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_create(&thread_id, &attr, entry, (void*) this);
    pthread_attr_destroy(&attr);

}

bool
PutInstance::SequentialTest()
{
    return mGroup->SequentialTest();
}

PutGroup::PutGroup(const t_TestConfigVec &vec, bool sequential_test)
{
    int r = 0;

    mConfigVec.assign(vec.begin(), vec.end());
    for(size_t i = 0; i < mConfigVec.size(); i ++) {
        mConfigVec[i].mPercentage *= 10;
        mConfigVec[i].mStageCount = 0;
    }
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&mBalanceLock, &attr);
    pthread_mutexattr_destroy(&attr);

    r = pthread_condattr_init(&mCondAttr);
    assert(r == 0);
    r = pthread_condattr_setclock(&mCondAttr, CLOCK_MONOTONIC);
    assert(r == 0);
    r = pthread_cond_init(&mBalanceCond, &mCondAttr);
    assert(r == 0);

    mSequentialTest = sequential_test;
}

PutGroup::~PutGroup()
{
    pthread_mutex_destroy(&mBalanceLock);
    pthread_cond_destroy(&mBalanceCond);
    pthread_condattr_destroy(&mCondAttr);
}

    void
PutGroup::Create()
{
    PutInstance *instance = NULL;

    for(size_t i = 0; i < mConfigVec.size(); i ++) {
        instance = new PutInstance(this,
                mConfigVec[i].mPercentage,
                mConfigVec[i].mMin,
                mConfigVec[i].mMax);
        instance->SetIndex(i);
        mInstances.push_back(instance);
    }

    for(size_t i = 0; i < mInstances.size(); i ++) {
        mInstances[i]->Create();
    }
}

    bool
PutGroup::OthersDone(PutInstance *instance)
{
    for(size_t i = 0; i < mInstances.size(); i ++) {
        if(mInstances[i] != instance
                && !mInstances[i]->StageDone()) {
            return false;
        }
    }
    return true;
}

void
PutGroup::WakeupOthersForBalance()
{
    pthread_cond_broadcast(&mBalanceCond);
}

void
PutGroup::WaitForBalance()
{
    pthread_cond_wait(&mBalanceCond, &mBalanceLock);
}

void
PutGroup::LockForBalance()
{
    pthread_mutex_lock(&mBalanceLock);
}

void
PutGroup::UnlockForBalance()
{
    pthread_mutex_unlock(&mBalanceLock);
}

    void
PutTest(int groups,
        bool sequential_test,
        t_TestConfigVec &configvec)
{
    PutGroup *group = NULL;
    std::vector<PutGroup *> putgroups;
    pthread_mutexattr_t attr;

    pthread_mutexattr_init(&attr);
    pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    pthread_mutex_init(&s_ops_lock, &attr);
    pthread_mutexattr_destroy(&attr);

    s_put_ops_list_size = configvec.size();
    s_put_ops_list = (PutStatics *)malloc(s_put_ops_list_size * sizeof(PutStatics));
    for(int i = 0; i < s_put_ops_list_size; i ++) {
        memset(&(s_put_ops_list[i]), 0, sizeof(PutStatics));
        s_put_ops_list[i].mMin = configvec[i].mMin;
        s_put_ops_list[i].mMax = configvec[i].mMax;
    }

    random_buffer = (char *)malloc(1048576);

    for(int i = 0; i < groups; i ++) {
        group = new PutGroup(configvec, sequential_test);
        putgroups.push_back(group);
    }

    for(size_t i = 0; i < putgroups.size(); i ++) {
        putgroups[i]->Create();
    }
    /*
    PutInstance *test = NULL;
    std::vector<PutInstance *> putvecs;

    for(int i = 0; i < threads; i ++) {
        test = new PutInstance();
        putvecs.push_back(test);
    }

    for(size_t i = 0; i < putvecs.size(); i ++) {
        putvecs[i]->Create();
    }
    */
}

    void
PutPrintStatics(int seconds)
{
    std::ostringstream ostr;
    int64_t total = 0;
    pthread_mutex_lock(&s_ops_lock);
    for(int i = 0; i < s_put_ops_list_size; i ++) {
        total += s_put_ops_list[i].mOps;
    }
    pthread_mutex_unlock(&s_ops_lock);
    cout << setw(16) << "put total: "
        << setw(8) << total
        << setw(8) << "avg:"
        << setw(8) << total/seconds;
    for(int i = 0; i < s_put_ops_list_size; i ++) {
        ostr << s_put_ops_list[i].mMin << "-" << s_put_ops_list[i].mMax;
        cout << setw(16) << ostr.str() << ":"
            << setw(8) << s_put_ops_list[i].mOps/seconds;
        ostr.str("");
    }
    for(int i = 0; i < s_put_ops_list_size; i ++) {
        ostr << s_put_ops_list[i].mMin << "-" << s_put_ops_list[i].mMax;
        cout << setw(16) << ostr.str() << ":"
            << setw(8) << s_put_ops_list[i].mOps;
        ostr.str("");
    }
    cout << endl;
}


