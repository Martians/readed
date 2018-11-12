
#include <cstdlib>
#include <iostream>
#include <fstream>

#include "HyperClient.hpp"
#include "Logger.hpp"
#include "Time.hpp"
#include "Util.hpp"
#include "Statistic.hpp"

#if ORIGIN_SUPPORT
#	include <Hypertable/Lib/Client.h>
#	include <Hypertable/Lib/Config.h>
#endif

using namespace std;
using namespace Hypertable;
using namespace Hypertable::ThriftGen;

static HyperClient::Table s_default_table = {
	"work", "test_table",
	"access_group_1", "cf_1", ""
};

const char* default_origin_config = "/opt/hypertable/current/conf/hypertable.cfg";

HyperClient::HyperClient(int index, bool origin, const string& config)
	: mIndex(index), mOrigin(origin), mTable(s_default_table), mSpace(0), mClient(nullptr), mCreateMutator(false)
#if ORIGIN_SUPPORT
	, mOriginBuilder(NULL), mOriginConfig(config.empty() ? default_origin_config : config)
#endif
{
	if (Origin()) {
#if ORIGIN_SUPPORT
		mOriginBuilder.reset(new CellsBuilder(1024));
#endif
	}
}

HyperClient::~HyperClient()
{
	Close();
}

#if ORIGIN_SUPPORT
ClientPtr
GetOriginClient(const string& config)
{
	static Mutex s_mutex("client", true);

    Mutex::Locker locker(mutex);
	static ClientPtr s_client = make_shared<Hypertable::Client>("", config);
	return s_client;
}
#endif

int
HyperClient::ReportError(const string& operation, ClientException& e)
{
	log_warn(operation << " - " << e.message);
	return -1;
}

int
HyperClient::Open()
{
	try {
		if (Thrift()) {
			/** always using thrift for table operation */
			if (!mClient) {
				mClient = new Thrift::Client(mConfig.global.host, mConfig.global.port);
			}

			if (!mClient->namespace_exists(SpaceName())) {
				mClient->namespace_create(SpaceName());
			}
			mSpace = mClient->namespace_open(SpaceName());
		}

		if (Origin()) {
#if ORIGIN_SUPPORT
			if (!mOriginClient) {
				mOriginClient = GetOriginClient(mOriginConfig);
			}

			if (!mOriginClient->exists_namespace(SpaceName())) {
				mOriginClient->create_namespace(SpaceName());
			}
			mOriginSpace = mOriginClient->open_namespace(SpaceName());
#endif
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::Close()
{
	if (mClient) {

		CloseMutator();

		CloseScanner();

		if (SpaceHandle() != 0) {
			mClient->namespace_close(SpaceHandle());
			mSpace = 0;
		}

		if (mClient) {
			delete mClient;
			mClient = NULL;
		}
	}

	if (Origin()) {
#if ORIGIN_SUPPORT
		mOriginClient 	= NULL;
		mOriginSpace 	= NULL;
		mOriginTable	= NULL;
		mOriginMutator	= NULL;
		mOriginScanner	= NULL;
#endif
	}
	return 0;
}

	int
HyperClient::CreateSchema()
{
	map<string, ThriftGen::AccessGroupSpec> ag_specs;
	map<string, ThriftGen::ColumnFamilySpec> cf_specs;

	/* Set table defaults */
	{
		ThriftGen::AccessGroupOptions ag_options;
		if (mConfig.table.block_size) {
			ag_options.__set_blocksize(mConfig.table.block_size);
		}
		if (mConfig.table.memory) {
			ag_options.__set_in_memory(mConfig.table.memory);
		}
		mSchema.__set_access_group_defaults(ag_options);
	}

	{
		ThriftGen::ColumnFamilyOptions cf_options;
		/** this will active gc, which will made large io */
		//cf_options.__set_max_versions(1);
		mSchema.__set_column_family_defaults(cf_options);
	}

	{
		ThriftGen::AccessGroupSpec ag;
		//ag.__set_name(i == 0 ? AccessGroup() :
		//		AccessGroup() + "_" + i);
		ag.__set_name(AccessGroup());

		//ThriftGen::ColumnFamilyOptions cf_options;
		//ag.__set_defaults(cf_options);
		ag_specs[AccessGroup()] = ag;
	}

	/** Column */
	{
		ThriftGen::ColumnFamilySpec cf;
		cf.__set_name(ColumnFamily());
		cf.__set_access_group(AccessGroup());
		//cf.__set_value_index(true);
		//cf.__set_qualifier_index(true);
		cf_specs[ColumnFamily()] = cf;
	}

	mSchema.__set_access_groups(ag_specs);
	mSchema.__set_column_families(cf_specs);

	if (mConfig.table.commit_interval != 0) {
		mSchema.__set_group_commit_interval(mConfig.table.commit_interval);
	}
	return 0;
}

int
HyperClient::CreateTable()
{
	try {
		if (!mClient->table_exists(SpaceHandle(), TableName())) {
			mClient->table_create(SpaceHandle(), TableName(), mSchema);
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::DropTable(const string& name, bool total)
{
	if (total) {
		std::vector<Hypertable::ThriftGen::NamespaceListing> list;
		mClient->namespace_get_listing(list, SpaceHandle());

		for (size_t i = 0; i < list.size(); i++) {
			if (list[i].is_namespace) continue;
			DropTable(list[i].name);
		}

	} else {
		try {
			bool if_exists = true;
			string table = name.length() == 0 ? TableName() : name;

			mClient->table_drop(SpaceHandle(), table, if_exists);
			log_info("client " << Index() << " drop table " << table);

		} catch (ClientException &e) {
			return ERROR(e);
		}
	}
	return 0;
}

int
HyperClient::HqlQuery(const string& command,
		std::string& output, const std::string& prefix)
{
	try {
		HqlResult result;
		mClient->hql_query(result, SpaceHandle(), command);

		if (!result.results.empty()) {
			output = result.results[0];
		}

		if (prefix.length()) {
			log_info(prefix << output);
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::QuerySchema()
{
	string output;
	return HqlQuery(std::string("SHOW CREATE TABLE ")
		+ TableName(), output, "query schema: \n");
}

int
HyperClient::Update(const string& key, const string& data, bool deleted)
{
	try {
		ThriftGen::Cell cell;

		cell.key.__set_timestamp(AUTO_ASSIGN);
		cell.key.__set_flag(ThrfitCellFlag(deleted));
		cell.key.__set_column_family(ColumnFamily());
		cell.key.__set_column_qualifier(Column());
		cell.key.__set_row(key);

		if (data.length()) {
			cell.__set_value(data);
		}

		mClient->set_cell(SpaceHandle(), TableName(), cell);

		g_statis.Inc(key.length() + ExtraKeyLen(), data.length());

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::Update()
{
	try {
		mClient->set_cells(SpaceHandle(), TableName(), mMutatorCache);
		mMutatorCache.clear();

		RecordUpdate();

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::Get(const string& key, Record& record)
{
	try {
		mClient->get_cell(record.value, SpaceHandle(), TableName(), key, ColumnFamily());

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::GetArray(RecordArray& array)
{
	try {
		ThriftGen::ScanSpec ss;
		std::vector<string> columns;

		columns.push_back(ColumnFamily());
		ss.__set_columns(columns);

		mClient->get_cells(array, SpaceHandle(), TableName(), ss);

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

void
HyperClient::RecordUpdate()
{
	g_statis.Inc(mAccuCount, mAccuKeyLen, mAccuDataLen);
	mAccuCount	 = 0;
	mAccuKeyLen  = 0;
	mAccuDataLen = 0;
}

void
HyperClient::RecordUpdate(int klen, int dlen)
{
	mAccuKeyLen  += klen + ExtraKeyLen();
	mAccuDataLen += dlen;
	mAccuCount++;
}

void
HyperClient::RecordScan(int klen, int dlen)
{
	g_statis.Inc(klen + ExtraKeyLen(), dlen);
}

void
HyperClient::Cache(const char* key, int klen,
		const char* data, int dlen, bool deleted)
{

	if (Origin()) {
		Hypertable::Cell cell(key, ColumnFamily().c_str(), Column().c_str(),
			AUTO_ASSIGN, AUTO_ASSIGN, (uint8_t*)data, dlen, OriginCellFlag(deleted));
		mOriginBuilder->add(cell);

		#if CHECK_GENERATE_DATA
		assert((int)strlen(key) == klen);
		#endif

	} else {
		Record record;
		record.key.row.assign(key, klen);
		record.value.assign(data, dlen);
		record.__isset.value = true;
		//record.__set_value(data, dlen);

		record.key.__set_timestamp(AUTO_ASSIGN);
		record.key.__set_flag(ThrfitCellFlag(deleted));
		record.key.__set_column_family(ColumnFamily());
		record.key.__set_column_qualifier(Column());

		#if CHECK_GENERATE_DATA
		assert((int)record.key.row.length() == klen);
		assert((int)record.value.length() == dlen);
		#endif

		mMutatorCache.push_back(record);
	}
	RecordUpdate(klen, dlen);
}

int
HyperClient::CreateMutator()
{
	try {
		if (Origin()) {
#if ORIGIN_SUPPORT

			uint32_t flag = mConfig.mutator.no_sync_log ? TableMutator::FLAG_NO_LOG_SYNC : 0 |
					mConfig.mutator.no_log ? TableMutator::FLAG_NO_LOG : 0;
			if (!mOriginTable) {
				mOriginTable = mOriginSpace->open_table(TableName());
			}
			mOriginMutator.reset(mOriginTable->create_mutator(0, flag, mConfig.mutator.flush_interval));
#endif
		} else {
			uint32_t flag = mConfig.mutator.no_sync_log ? MutatorFlag::NO_LOG_SYNC : 0 |
				mConfig.mutator.no_log ? MutatorFlag::NO_LOG : 0;
			mMutator = mClient->mutator_open(SpaceHandle(), TableName(), flag, mConfig.mutator.flush_interval);
			mCreateMutator = true;
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::CloseMutator()
{
	try {
		if (Origin()) {
#if ORIGIN_SUPPORT
			mOriginMutator = NULL;
#endif
		} else {
			if (!mClient || !mCreateMutator) return 0;
			mClient->mutator_close(mMutator);
			mCreateMutator = false;
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::FlushMutator()
{
	try {
		if (Origin()) {
#if ORIGIN_SUPPORT
			mOriginMutator->flush();
#endif
		} else {
			mClient->mutator_flush(mMutator);
		}
	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::MutatorUpdate(bool no_flush)
{
	try {
		if (Origin()) {
#if ORIGIN_SUPPORT
			Cells array;
			mOriginBuilder->get(array);
			mOriginMutator->set_cells(array);

			if (!no_flush) {
				mOriginMutator->flush();
			}
			mOriginBuilder->clear();
#endif
		} else {
			mClient->mutator_set_cells(mMutator, mMutatorCache);

			if (!no_flush) {
				mClient->mutator_flush(mMutator);
			}
			mMutatorCache.clear();
		}

		RecordUpdate();

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::CreateScanner(const std::string& start, const std::string& end, bool key_only)
{
	int ret = CloseScanner();
	if (ret != 0) return ret;

	try {
		if (Origin()) {
#if ORIGIN_SUPPORT
			if (!mOriginTable) {
				mOriginTable = mOriginSpace->open_table(TableName());
			}
			Hypertable::Lib::ScanSpecBuilder scan_spec;
			scan_spec.add_row_interval(start, true, end, true);
			scan_spec.set_max_versions(1);
			scan_spec.set_keys_only(key_only);

			scan_spec.add_column(ColumnFamily());

			#if 0
			scan_spec.set_scan_and_filter_rows(true);
			#endif

		    mOriginScanner.reset(mOriginTable->create_scanner(scan_spec.get()));
#endif
		} else {
		    ThriftGen::RowInterval thrift_row_interval;
		    thrift_row_interval.__set_start_row(start);
		    thrift_row_interval.__set_start_inclusive(true);
		    thrift_row_interval.__set_end_row(end);
		    thrift_row_interval.__set_end_inclusive(true);

		    ThriftGen::ScanSpec thrift_scan_spec;
		    thrift_scan_spec.row_intervals.push_back(thrift_row_interval);
		    thrift_scan_spec.__isset.row_intervals = true;

		    thrift_scan_spec.__set_versions(1);
		    thrift_scan_spec.__set_keys_only(key_only);

		    thrift_scan_spec.columns.push_back(ColumnFamily());
		    thrift_scan_spec.__isset.columns = true;

			mScanner = mClient->scanner_open(SpaceHandle(), TableName(), thrift_scan_spec);
			mCreateScanner = true;

			mScannerCache.clear();
			mScannerIndex = 0;
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::CloseScanner()
{
	try {
		if (Origin()) {
#if ORIGIN_SUPPORT
			mOriginScanner = NULL;
#endif
		} else {
			if (!mClient || !mCreateScanner) return 0;
			mClient->scanner_close(mScanner);
			mCreateScanner = false;
		}

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::ScanNext(const char*& key, const char*& data, int& dlen)
{
	try {
		if (Origin()) {
			Hypertable::Cell cell;
			if (mOriginScanner->next(cell)) {
				key  = cell.row_key;
				data = (const char*)cell.value;
				dlen = cell.value_len;

				#if 0
				cell.column_family;
				cell.column_qualifier;
				#endif

			} else {
				key = NULL;
			}

		} else {
			/** read to cache end */
			if (mScannerIndex >= mScannerCache.size()) {
				mScannerCache.clear();
				mScannerIndex = 0;

				mClient->scanner_get_cells(mScannerCache, mScanner);
			}

			if (mScannerIndex < mScannerCache.size()) {
				Record& record = mScannerCache[mScannerIndex];
				mScannerIndex++;

				key  = record.key.row.c_str();
				data = record.value.c_str();
				dlen = record.value.size();

			} else {
				key = NULL;
			}
		}

		if (!key) return -1;
		RecordScan(strlen(key), dlen);

	} catch (ClientException &e) {
		return ERROR(e);
	}
	return 0;
}

int
HyperClient::Get(const char* key, const char*& data, int& dlen)
{
	int ret = 0;
	if ((ret = CreateScanner(key)) != 0) {
		return ret;
	}

	if ((ret = ScanNext(key, data, dlen)) != 0) {
		return ret;
	}
	return 0;
}

void
HyperClient::Restart(bool drop_total)
{
	Close();
	Open();

	if (Thrift()) {
		if (mConfig.table.drop_table) {
			DropTable("", drop_total);
		}
		/** use same table, no need create again */
		CreateSchema();
		CreateTable();
	}
}

string
ClientConfig::DumpTable()
{
	std::stringstream ss;
	dump_reset();

    ss << "table info";
    ss << dump_last(", table(", "), ")
       << dump_exist(table.diff_table, "diff")
	   << dump_exist(table.drop_table, "drop");

    ss << dump_last(", mode(", ")")
       << dump_exist(table.memory, "memory")
	   << dump_exist(table.block_size, "bk[" + string_size(table.block_size, false) + "]")
	   << dump_exist(table.commit_interval, "ci[" + n2s(table.commit_interval) + "]");

	return ss.str();
}

string
ClientConfig::DumpTest(const std::string& prefix, bool time)
{
	std::stringstream ss;
	ss << prefix;

	if (time) {
		ss << string_date() << "_" << string_time() << "_";
	}

	ss << string_count(test.total, false)
	   << "_" << (test.type == CT_put ? "put" : (test.type == CT_get ? "get" : "scan"))
	   << "_" << (test.uuid_key ? "uuid" : (test.rand_key ? "rand" : "seqn"))
	   << "_t[" << std::max(global.thread, 1) << "-" << test.batch << "]";

	if (test.key_max == test.key_min) {
		ss << "_k[" << string_size(test.key_max, false) << "]";
	} else {
		ss << "_k[" << string_size(test.key_min, false) << "-" << string_size(test.key_max, false) << "]";
	}

	if (test.data_max == test.data_min) {
		ss << "_d[" << string_size(test.data_max, false) << "]";
	} else {
		ss << "_d[" << string_size(test.data_min, false) << "-" << string_size(test.data_max, false) << "]";
	}

	if (mutator.no_flush || mutator.no_sync_log || mutator.flush_interval) {
		dump_reset(",");
		ss << dump_last("_m(", ")")
		   << dump_exist(mutator.no_flush, "no_flush")
		   << dump_exist(mutator.no_sync_log,  "no_sync_log")
		   << dump_exist(mutator.flush_interval, "i[" + n2s(mutator.flush_interval) + "]");
	}

	//dump_reset();
    ss << dump_last(", api(", ")")
       << dump_exist(!global.origin, "thrift");
	return ss.str();
}


