#ifndef __RW_134a3112_5559_159d1512_PUTTEST_HPP__
#define __RW_134a3112_5559_159d1512_PUTTEST_HPP__

#include "Client.hpp"
extern HyperClient g_client;

class PutGroup;

class PutTestConfig
{
    public:
        int     mPercentage;
        int64_t mMin;
        int64_t mMax;
        int     mStageCount;
};
typedef std::vector<PutTestConfig> t_TestConfigVec;

class PutInstance
{
    public:
        PutInstance(PutGroup *group, int limit, int min, int max);
        ~PutInstance();

        void IncreaseTotalOps();
        void LockForBalance();
        bool OthersDone();
        void WakeupOthersForBalance();
        void WaitForBalance();
        void UnlockForBalance();

        static void *entry(void *args);
        void Test();
        void PutOneObject();
        void Create();
        bool SequentialTest();
        bool StageDone() {
            return mCurrentStagePutDone == mStageLimit;
        }
        void SetIndex(int index) {
            mIndex = index;
        }

    private:
        PutGroup *mGroup;
        int mCurrentStagePutDone;
        int mStageLimit;
        int mIndex;
        int mMin;
        int mMax;
        int mSequentialKey;
};
typedef std::vector<PutInstance *> t_Instances;

class PutGroup
{
    public:
        PutGroup(const t_TestConfigVec &vec, bool sequential_test);
        ~PutGroup();

        void Create();
        bool OthersDone(PutInstance *instance);
        void WakeupOthersForBalance();
        void WaitForBalance();
        void LockForBalance();
        void UnlockForBalance();
        bool SequentialTest() {
            return mSequentialTest;
        }

    private:
        t_TestConfigVec mConfigVec;
        t_Instances     mInstances;
        pthread_mutex_t  mBalanceLock;
        pthread_cond_t  mBalanceCond;
        pthread_condattr_t mCondAttr;
        bool            mSequentialTest;
};

void PutTest(int groups,
        bool sequential_test,
        t_TestConfigVec &configvec);
void PutPrintStatics(int seconds);

#endif

