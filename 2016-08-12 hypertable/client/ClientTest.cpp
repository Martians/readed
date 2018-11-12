
#include "ClientTest.hpp"
#include "Logger.hpp"
#include "Statistic.hpp"
#include "File.hpp"
#include "Util.hpp"

#include <Common/PageArena.h>

void
ClientTest::FixConfig()
{
	/** more than 1 thread, they do not use same table */
	if (mConfig.global.thread > 1) {
		if (mConfig.table.diff_table){
			mTable.table_name = mTable.table_name + "_" + n2s(mIndex);
		}
	}
}

void
ClientTest::Work(bool thread)
{
	if (thread) {
		mStatus = CS_work;
		int ret = mThread.create();
		assert(ret == 0);
		return;
	}

	FixConfig();

	Restart();

	PrepareTest();

	NotifyWait(WT_start);

	switch(mConfig.test.type) {

	case ClientConfig::CT_load:{
		if (mConfig.load.output.length()) {
			SetCharIgnore(true);
			Output();
		}
		#if 0
			if (mConfig.load.dump_data.length()) {
				DumpData();
				return 0;
			}
			if (mConfig.load.load_data.length()) {
				LoadData();
				return 0;
			}
		#endif
	}break;

	case ClientConfig::CT_put:{
		if (!Origin() && mConfig.global.single) {
			SinglePutTest();

		} else {
			PutTest();
		}

	}break;
	case ClientConfig::CT_get:{

		/** wait client 0 to do scan */
		if (mIndex == 1) {
			ScanTest();
		}
		NotifyWait(WT_prepare);

		if (!Origin() && mConfig.global.single) {
			assert(0);
			SingleGetTest();

		} else {
			GetTest();
		}

	}break;
	case ClientConfig::CT_scan:{
		ScanTest();

	}break;
	default:
		std::cout << "invalid test type " << mConfig.test.type << std::endl;
		assert(0);
		break;
	}

	Complete();
}

void
ClientTest::NotifyWait(int type)
{
	mManage->NotifyWait(type, mIndex);
}

void
ClientTest::NotifyWork()
{
	if (!mNotifyWork) {
		mNotifyWork = true;

		NotifyWait(WT_work);
	}
}

void
ClientTest::Complete()
{
	mStatus = CS_done;
	mManage->Complete(mIndex);
}

int
ClientTest::LoadData()
{
	string output;
	return HqlQuery(std::string("Load data infile ") + "\"./infile/" +
			mConfig.load.load_data + "\" into table " + TableName(), output, "load data infile: \n");
}

int
ClientTest::DumpData()
{
	mConfig.table.drop_table = false;

	string output;
	return HqlQuery(std::string("select * from ") + TableName() +
			" into file \"./infile/" + mConfig.load.dump_data + "\"", output, "dump data to file: \n");
}

void
ClientTest::SimpleTest()
{
	CreateSchema();

	Open();
	//DropTable();
	
	CreateTable();
	//QuerySchema();

	const char* key   = "test_key";
	const char* value = "test_value";

	Record record;

	if (Put(key, value) == 0 && 
		Get(key, record) == 0 &&
		(record.value == value) &&
		Del(key) == 0)
	{
		log_info("test success");

	} else {
		log_error("test failed");
		assert("test failed" || 0);
	}
}

void
ClientTest::VerifyKey()
{
	std::string key;
	std::string* data;

	LOOP_KEY_FOR_EACH(key, data, mRecord.mMap) {
		Record record;
		if (Get(key, record) != 0) {
			log_warn("verify and get key failed, key: " << key);

		} else if (record.value != *data) {
			size_t length = std::min(record.value.length(), data->length());
			size_t pos = 0;
			for (; pos < length; pos++) {
				if (record.value[pos] != (*data)[pos]) break;
			}
			log_warn("verify failed, maybe duplicate put, key: " << key
				<< ", len compare [" << data->length() << " - " << record.value.length() << "]"
				<< ", mismatch at " << pos);
			//assert(0);
		}
	}
}

void
ClientTest::PrepareTest()
{
	mGen.SetKey(mConfig.test.rand_key, true, mConfig.test.uuid_key);
	mGen.SetData(true, mConfig.test.char_data, mConfig.test.uuid_data);

	mGen.KeyRange(mConfig.test.key_max, mConfig.test.key_min);
	mGen.DataRange(mConfig.test.data_max, mConfig.test.data_min);
	mRecord.Set(mConfig.test.verify);
}

void
ClientTest::Output()
{
	class Output {
	public:
		Output() : mPos(0) {}
		~Output() { Close(); }

	public:
		void	Open(ClientConfig& config) {
			if (!mFile.open("./infile/" + config.load.output,
				FileBase::op_create | FileBase::op_write | FileBase::op_trunc))
			{
				assert(0);
			}
			mFile.seek(0);
		}
		void	Write(const char* data, int size, const string& end = "") {

			if (mPos + size > (int)sizeof(mData)) {
				Swap();
			}
			memcpy(mData + mPos, data, size);
			mPos += size;

			memcpy(mData + mPos, end.c_str(), end.length());
			mPos += end.length();
		}

		void	Swap() {
			if (mPos != 0) {
				if (mFile.write(mData, mPos) < 0) {
					assert(0);
				}
				#if DATA_CHECK_CHAR
				for (int i = 0; i < mPos; i++) {
					if( mData[i] == '\\' ){
						/** the first line */
						if ((char)mData[0] == '#') break;
						assert(0);
					}
				}
				#endif
				mPos = 0;
			}
		}

		void	Close() {
			Swap();
			mFile.close();
		}
	public:
		FileBase mFile;
		int	 	mPos;
		char	mData[256 * 1024];
	};

	char* key;
	char* data;
	int klen, dlen;

	std::string header = "#row\tcolumn\tvalue\n";
	std::string column = ColumnFamily();

	Output out;
	out.Open(mConfig);
	out.Write(header.c_str(), header.length());

	uint64_t total = mConfig.EachCount();
	for (uint64_t count = 0; count < total;
		count += mConfig.test.batch)
	{
		mGen.Key (key, klen);
		out.Write(key, klen, "\t");

		out.Write(column.c_str(), column.length(), "\t");

		mGen.Data(data, dlen);
		out.Write(data, dlen, "\n");

#if DATA_CHECK_CHAR
		/** load data when output */
		if (count % 1000 == 0) {
			out.Close();
			string output;
			//if (HqlQuery(std::string("Load data infile ") + "\"/mnt/ssd/hyper/client/build/infile/file" + "\" into table " + TableName(), output, "load data infile: \n") != 0) {
			if (HqlQuery(std::string("Load data infile ") + "\"infile/file" + "\" into table " + TableName(), output, "load data infile: \n") != 0) {
				out.mData[100] = 0;
				log_info("error count " << count << out.mData);
				assert(0);
			}
			out.Open(config);
		}
#endif
	}
	out.Close();
}

void
ClientTest::SinglePutTest()
{
    char* key;
	char* data;
	int klen, dlen;

	uint64_t total = mConfig.EachCount();
	for (uint64_t count = 0; count < total;
		count += mConfig.test.batch)
	{
		if (mConfig.test.batch > 1) {

			for (int i = 0; i < mConfig.test.batch; i++) {
				mGen.Key(key, klen);
				mGen.Data(data, dlen);

				Recording(key, klen, data, dlen);
				Cache(key, klen, data, dlen);
			}
			Update();

		} else {
			mGen.Key(key, klen);
			mGen.Data(data, dlen);

			Recording(key, klen, data, dlen);
			Put(string(key, klen), string(data, dlen));
		}

		NotifyWork();
	}
	VerifyKey();
}

void
ClientTest::SingleGetTest()
{
	NotifyWork();

	const char* key  = NULL;
	const char* data = NULL;
	int dlen = 0;

	if (!mManage->Next()) {
		log_info("can't find any pre read key");
		return;
	}

	uint64_t total = mConfig.EachCount();

	for (uint64_t count = 0; count < total; count++) {
		key = mManage->Next();

		if (Get(key, data, dlen) != 0) {
			log_warn("get key " << key << " failed");
			break;
		}
	}
}

void
ClientTest::PutTest()
{
	CreateMutator();

	char* key;
	char* data;
	int klen, dlen;

	uint64_t total = mConfig.EachCount();
	for (uint64_t count = 0; count < total;
		count += mConfig.test.batch)
	{
		for (int i = 0; i < mConfig.test.batch; i++) {
			mGen.Key(key, klen);
			mGen.Data(data, dlen);

			Recording(key, klen, data, dlen);
			Cache(key, klen, data, dlen);
		}
		if (MutatorUpdate(mConfig.mutator.no_flush) != 0) {
			break;
		}

		NotifyWork();
	}
	FlushMutator();

	VerifyKey();
}

void
ClientTest::GetTest()
{
	NotifyWork();

	const char* key  = NULL;
	const char* data = NULL;
	int dlen = 0;

	if (!mManage->Next()) {
		log_info("can't find any pre read key");
		return;
	}

	uint64_t total = mConfig.EachCount();

	for (uint64_t count = 0; count < total; count++) {
		key = mManage->Next();

		if (Get(key, data, dlen) != 0) {
			log_warn("get key " << key << " failed");
			break;
		}
	}
}

void
ClientTest::ScanTest()
{
	bool pre_read = (mConfig.test.type == ClientConfig::CT_get);

	if (pre_read) {
		mManage->InitRandRead();

	} else {
		NotifyWork();
	}
	CreateScanner("", "", pre_read);

	const char* key  = NULL;
	const char* data = NULL;
	int dlen = 0;

	while (ScanNext(key, data, dlen) == 0) {

		if (pre_read) {
			mManage->AddKey(key);
		}
	}
}

Hypertable::CharArena s_arena(256 * 1024);

void
ClientManage::KeyPool::Add(const char* key)
{
//	assert(mVec.capacity() == mConfig.test.total);

	key = s_arena.dup(key);

	if (mVec.size() < mVec.capacity()) {
		mVec.push_back(key);

	} else {
		mVec[Rand()] = key;
	}
}

void
ClientManage::FixConfig(ClientConfig& config)
{
	mConfig = config;

	if (mConfig.global.seed == 0) {
	    struct timeval tv;
	    gettimeofday(&tv, NULL);
		mConfig.global.seed = tv.tv_usec;
	}
	g_generator.Init(mConfig.global.seed);

	mConfig.global.thread =
		std::max(mConfig.global.thread, 1);

	/** should change table, we not use alter table yet */
	if (//mConfig.table.diff_group ||
		mConfig.table.memory ||
		mConfig.table.block_size != 0 ||
		mConfig.table.commit_interval != 0 )
	{
		mConfig.table.drop_table = true;
	}

	if (mConfig.mutator.flush_interval) {
		mConfig.mutator.no_flush = true;
	}

	if (mConfig.table.drop_table) {
		//std::cout << "will drop all table ... " << std::endl;
	}

	mConfig.Check();
	log_info(mConfig.DumpTable());
	log_info(mConfig.DumpTest("test suit, "));
	log_info("");
}

void
ClientManage::PrepareTable()
{
	log_info("preparing ...");
	ClientTest client(-1, mConfig, this);
	client.Restart(true);

	/** if all client use one table, every one of them will do create or 
	 *  clear work, here we do it only once */
	if (mConfig.table.drop_table) {
	//	ClientTest client(-1, mConfig, this);
	//	client.Restart(true);
		/** no need clear table again */
		mConfig.table.drop_table = false;
	}
}

void
ClientManage::ResetStatistic()
{
	g_thread.Reset(true);

	g_statis.Reset(mConfig.test.type == ClientConfig::CT_scan ?
		INT64_MAX : mConfig.test.total);
}

void
ClientManage::Start(ClientConfig& config)
{
	FixConfig(config);

	PrepareTable();

	int thread = mConfig.global.thread;
	while (thread-- > 0) {
		Add(mConfig);
	}

	g_thread.start();

	Work();
}

void
ClientManage::Stop()
{
	g_thread.stop();

	LOOP_PTR_FOR_EACH(ClientTest, client, mClients) {
		client->Close();
		delete client;
	}
	mClients.clear();
}

void
ClientManage::Work()
{
	SetWait(WT_start, mClients.size() + 1);

	LOOP_PTR_FOR_EACH(ClientTest, client, mClients)  {
		client->Work(true);
	}
	/** wait all client connected */
	NotifyWait(WT_start, -1);

	/** wait all client completed */
	WaitComplete();
}

void
ClientManage::Complete(int index)
{
	USE_LOCK;

	ClientTest* client = GetClient(index);
	assert(client->Status() == CS_done);

	mComplete++;
	int remain = mClients.size() - mComplete;
	uint64_t count = mConfig.test.total - (g_statis.iops.total() + g_statis.iops.count());
	log_info("complete, index " << index << ", remain client " << remain
		<< ", count " << count);

	if (remain == 0) {
		/** can't do this in NotifyWait */
		SetWait(WT_complete);
		Wakeup();
	}
}

void
ClientManage::SetWait(int type, int count)
{
	USE_LOCK;

	mWaiting = 0;
	mWaitCount = count;
	mWaitType = type;
}

void
ClientManage::NotifyWait(int type, int index)
{
	mMutex.lock();

	if (type != mWaitType) {
		assert(0);
	}

	/** n-client or manager, the last one wakeup */
	if (++mWaiting < mWaitCount) {
		mCond.wait(mMutex);
		mMutex.unlock();

	} else {
		switch(type) {
			case WT_start:{
				log_info("****************************");
				log_info("connected ...");

				if (mConfig.test.type == ClientConfig::CT_get) {
					/** set wait for client 0 */
					SetWait(WT_prepare, mClients.size());
				} else {
					SetWait(WT_work, mClients.size());
				}
				ResetStatistic();

			}break;

			case WT_prepare:{
				log_info("");
				log_info("----------------------------");
				log_info("pre-scanned ...");
				SetWait(WT_work, mClients.size());

				ResetStatistic();
			}break;

			case WT_work:{
				log_info("----------------------------");

			}break;

			default:{
				assert(0);
			}break;
		}
		mMutex.unlock();
		Wakeup();
	}
}


