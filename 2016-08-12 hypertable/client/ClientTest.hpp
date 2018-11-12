
#ifndef _HYPER_CLINET_TEST_H
#define _HYPER_CLINET_TEST_H

#include "HyperClient.hpp"
#include "Thread.hpp"
#include "Container.hpp"

namespace Hyper {

	class ClientManage;

	enum ClientStauts
	{
		CS_null = 0,
		CS_work,
		CS_done,
	};

	enum WaitType
	{
		WT_null = 0,
		WT_start,
		WT_work,
		WT_prepare,
		WT_complete
	};

	/**
	 * client interface
	 * */
	class ClientTest : public HyperClient
	{
	public:
		ClientTest(int index, ClientConfig& config, ClientManage* manage = NULL)
			: HyperClient(index, config.global.origin), mStatus(CS_null), mNotifyWork(false),
			  mThread(this), mManage(manage) { mConfig = config; }

		virtual ~ClientTest() {}

		/**
		 * used for key data verify
		 * */
		class KeyRecord
		{
		public:
			KeyRecord() : mIndex(0), mCount(0) {}

		public:
			/**
			 * set count
			 **/
			void	Set(int count) { mIndex = 0; mCount = count; }

			/**
			 * record new entry
			 **/
			bool	Add(const char* key, int klen, const char* data, int dlen) {
				if (mCount == 0) return true;

				if (mMap.size() >= mCount || mIndex++ % 17 != 0) {
					/** update value */
					std::string* value = mMap.get(string(key, klen));
					if (value) {
						value->assign(data, dlen);
					}
					return true;
				}
				std::string ds(data, dlen);
				return mMap.add(string(key, klen), &ds);
			}
			/**   */
		public:
			/** loop index */
			size_t	mIndex;
			/** count count */
			size_t	mCount;
			TypeMap<std::string, std::string> mMap;
		};

	public:
		/**
		 * get status
		 **/
		int		Status() { return mStatus; }

		/**
		 * do test work
		 **/
		void	Work(bool thread = false);

		/**
		 * test simple operation
		 */
		void	SimpleTest();

	protected:
		class ClientThread : public common::CommonThread
		{
		public:
			ClientThread(ClientTest* client) : mClient(client) {}

		public:

			virtual void* entry() {
				mClient->Work();
				return NULL;
			}
		protected:
			ClientTest*	mClient;
		};

		/**
		 * recording some data
		 * */
		bool	Recording(const char* key, int klen, const char* data, int dlen) {
			return mRecord.Add(key, klen, data, dlen);
		}

		/**
		 * waiting for start
		 **/
		void	NotifyWait(int type);

		/**
		 * notify client recv first
		 * */
		void	NotifyWork();

		/**
		 * test complete
		 **/
		void	Complete();

		/**
		 * fix config
		 **/
		void	FixConfig();

		/**
		 * prepare for test
		 * */
		void	PrepareTest();

		/**
		 * do put test
		 */
		void	SinglePutTest();

		/**
		 * do put test
		 */
		void	SingleGetTest();

		/**
		 * do mutator test
		 **/
		void	PutTest();

		/**
		 * do mutator test
		 **/
		void	GetTest();

		/**
		 * do mutator test
		 **/
		void	ScanTest();

		/**
		 * output data to file for load
		 * */
		void	Output();

		/**
		 * load data from file into table
		 * */
		int  	LoadData();

		/**
		 * dump data out into file
		 */
		int		DumpData();

		/**
		 * verify key and data
		 **/
		void	VerifyKey();

	protected:
		int		mStatus;
		bool	mNotifyWork;
		KeyRecord 	  mRecord;
		ClientThread  mThread;
		ClientManage* mManage;
	};

	class ClientManage
	{
	public:
		ClientManage() : mMutex("client manage", true), mAllocID(0),
			mWaiting(0), mWaitCount(0), mComplete(0), mWaitType(WT_null) {}
		virtual ~ClientManage() { Stop(); }

	public:
		/**
		 * keep key cache
		 * */
		class KeyPool
		{
		public:
			KeyPool() {}

		public:
			void	Init(ClientConfig& config) {
				if (mVec.size() != 0) return;
				mVec.reserve(config.test.total);
			}

			void	Add(const char* key);

			size_t	Rand() {
				return ::random() % mVec.size();
			}

			const char* Next() {
				if (mVec.size() == 0) return NULL;
				return mVec[Rand()];
			}

		protected:
			typedef std::vector<const char*> KeyVec;
			KeyVec	mVec;
		};

	public:
		/**
		 * add new client config
		 **/
		ClientTest*	Add(ClientConfig& config) {
			ClientTest* client = new ClientTest(NextID(), config, this);
			ClientTest** pclient = mClients.add(client->Index(), &client);
			return *pclient;
		}

		void	ResetStatistic();

		/**
		 * start all client
		 **/
		void	Start(ClientConfig& config);

		/**
		 * stop all client
		 **/
		void	Stop();

		/**
		 * try to work
		 **/
		void	Work();

		/**
		 * client wait to start
		 **/
		void	SetWait(int type, int count = 0);

		/**
		 **/
		void	NotifyWait(int type, int index);

		/**
		 * client complete, try to wakeup all
		 **/
		void	Complete(int index);

		/**
		 * manage wait until all client complete
		 **/
		void	WaitComplete() {
			mMutex.lock();
			do {
				mCond.wait(mMutex);
			} while(mWaitType != WT_complete);
			mMutex.unlock();
		}

		/**
		 * wakeup waiters
		 **/
		void	Wakeup() {
			mMutex.lock();
			mCond.signal_all();
			mMutex.unlock();
		}


		void	InitRandRead() {
			USE_LOCK;
			mKeyPool.Init(mConfig);
		}

		void	AddKey(const char* key) { mKeyPool.Add(key); }

		const char* Next() { return mKeyPool.Next(); }

	protected:

		ClientTest* GetClient(int index) {
			ClientTest** pclient = mClients.get(index);
			assert(pclient != NULL);
			return *pclient;
		}

		int		NextID() { return ++mAllocID; }

        Mutex&	GetMutex() { return mMutex; }

		void	FixConfig(ClientConfig& config);

		void	PrepareTable();

	protected:
		TypeMap<int, ClientTest*> mClients;
		ClientConfig mConfig;

        Mutex	mMutex;
		Cond	mCond;
		int		mAllocID;
		int		mWaiting;
		int		mWaitCount;
		int		mComplete;
		int		mWaitType;

		KeyPool	mKeyPool;
	};
}

#endif //_HYPER_CLINET_TEST_H
