
#ifndef _HYPER_CLINET_H
#define _HYPER_CLINET_H

#include <Common/Compat.h>
#include <Common/Logger.h>
#include <Common/System.h>

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/KeySpec.h>

#include <ThriftBroker/Client.h>
#include <ThriftBroker/gen-cpp/Client_types.h>
#include <ThriftBroker/gen-cpp/HqlService.h>
#include <ThriftBroker/ThriftHelper.h>
#include <ThriftBroker/SerializedCellsReader.h>
#include <ThriftBroker/SerializedCellsWriter.h>

#include <vector>
#include <string>

#include "Generator.hpp"
#include "Container.hpp"

#if _WIN32
	/** only for display */
#	define ORIGIN_SUPPORT 1
#endif

using namespace Hypertable;

namespace Hypertable {
	class Namespace;
	class Table;
	class CellsBuilder;
	class TableMutator;
	class TableScanner;
	class Client;

	typedef std::shared_ptr<Client> ClientPtr;
	typedef std::shared_ptr<Namespace> NamespacePtr;
	typedef std::shared_ptr<Table> TablePtr;
	typedef std::shared_ptr<CellsBuilder> CellBuilderPtr;
	typedef std::shared_ptr<TableMutator> TableMutatorPtr;
	typedef std::shared_ptr<TableScanner> TableScannerPtr;
}

namespace Hyper {

	/** define error display info, __FUNCTION__ */
	#define	ERROR(exception)	\
		ReportError(__PRETTY_FUNCTION__, exception)

	const uint16_t c_thrift_port = 15867;

	struct ClientConfig
	{
	public:
		ClientConfig() {
			global.host = "localhost";
			global.port = c_thrift_port;
			global.thread = 0;
			global.seed	= 0;
			global.origin = true;
			global.single = false;
			global.clear  = false;


			test.type	   = CT_put;
			test.total	   = 1000000;
			test.batch	   = 1;
			test.rand_key  = true;
			test.char_data = false;
			test.uuid_key  = false;
			test.uuid_data = false;
			test.key_min   = -1;
			test.key_max   = 40;
			test.data_min  = -1;
			test.data_max  = 4096;
			test.verify	   = 0;

			mutator.no_flush = false;
			mutator.no_sync_log  = false;
			mutator.no_log   = false;
			mutator.flush_interval = 0;

			//load.output;
			//load.dump_data;
			//load.load_data;

			table.diff_table = false;
			table.drop_table = false;
			//table.diff_group = false;

			table.memory	= false;
			table.block_size = 0;
			table.commit_interval	= 0;
		}

		ClientConfig(const ClientConfig& v) {
			operator = (v);
		}

		const ClientConfig& operator = (const ClientConfig& v) {
			global 	= v.global;
			test 	= v.test;
			mutator = v.mutator;
			load 	= v.load;
			table 	= v.table;
			return *this;
		}

	public:
		void	Check() {
			if (test.uuid_key) {
				test.key_min = test.key_max = 0;
			}

			if (test.uuid_data) {
				test.data_min = test.data_max = 0;
			}
		}

		uint64_t	EachCount() {
			return test.total / global.thread;
		}

		string	DumpTable();

		string	DumpTest(const std::string& prefix = "", bool time = false);

	public:
		enum Type {
			CT_put = 0,
			CT_get,
			CT_scan,
			CT_load
		};

		struct Global {
			string	host;
			int	 	port;
			int		thread;
			/** random seed */
			int		seed;
			/** use origin api */
			bool	origin;
			/** use mutator or not */
			bool	single;
			bool	clear;
		} global;

		struct Test {
			/** test put or get */
			int 	type;
			/** total loop */
			uint64_t total;
			/** batch request */
			int		batch;
			/** rand or sequnce */
			bool	rand_key;
			/** readable */
			bool	char_data;
			/** using uuid key */
			bool	uuid_key;
			/** using uuid data */
			bool	uuid_data;
			/** key min */
			int		key_min;
			/** key max */
			int		key_max;
			/** data min */
			int		data_min;
			/** data max */
			int		data_max;
			/** verify count */
			int		verify;

		} test;

		struct Mutator {
			bool	no_log;
			/** mutator no need flush */
			bool	no_flush;
			/** mutator flush interval */
			int		flush_interval;
			/** not sync log */
			bool	no_sync_log;
		} mutator;

		struct Load {
			string	output;
			string	dump_data;
			string	load_data;
		} load;

		struct Table {
			/** multi-thread use same table */
			bool	diff_table;
			/** drop old table data */
			bool	drop_table;
			/** different access group */
			//int		diff_group;
			//int		diff_column;

			bool	memory;
			int		block_size;
			int		commit_interval;
		} table;
	};

	/**
	 * client interface
	 * */
	class HyperClient
	{
	public:
		HyperClient(int index = 0, bool origin = false, const string& config = "");
		virtual ~HyperClient();

		typedef ThriftGen::Cell	Record;
		typedef std::vector<Record> RecordArray;

		/**
		 * operation enum
		 **/
		enum {
			PF_insert,
			PF_delete,
		};
		struct Table {
			string	name_space;
			string	table_name;
			string	access_group;
			string	column_family;
			string	column;
		};

	public:
		/**
		 * set client option
		 **/
		void	Set(ClientConfig& config) {
			mConfig = config;
		}

		/**
		 * get client id
		 **/
		int		Index() { return mIndex; }

		/**
		 * open  namesdpace
		 **/
		int		Open();

		/**
		 * close namespace
		 **/
		int		Close();
		
		/**
		 * create default schema
		 */
		int		CreateSchema();

		/**
		 * create defualt table
		 **/
		int		CreateTable();

		/**
		 * drop table
		 **/
		int		DropTable(const string& name = "", bool total = false);

		/**
		 * keep connection and drop table
		 * */
		void	Restart(bool drop_total = false);

		/**
		 * open origin
		 **/
		int		OpenOrigin(const string& config);

	public:
		/** thrift simple api */

		/**
		 * put data into table
		 * */
		int		Put(const string& key, const string& data) { return Update(key, data); }

		/**
		 * delete data from table
		 * */
		int		Del(const string& key) { return Update(key, "", true);	}

		/**
		 * put data into table
		 * */
		int		Update(const string& key, const string& data, bool deleted = false);

		/**
		 * write cached key value
		 **/
		int		Update();

		/**
		 * get record
		 **/
		int		Get(const string& key, Record& record);

		/**
		 * get record array
		 * */
		int 	GetArray(RecordArray& array);

	public:
		/**
		 * create mutator by config
		 */
		int		CreateMutator();

		/**
		 * close mutator
		 **/
		int		CloseMutator();

		/**
		 * flush mutator rightly
		 **/
		int		FlushMutator();

		/**
		 * cache a record
		 * */
		void	Cache(const char* key, int klen, const char* data, int dlen, bool deleted = false);

		/**
		 * put record into table
		 **/
		int		MutatorUpdate(bool no_flush = false);

		/**
		 * create scanner
		 */
		int		CreateScanner(const std::string& start, const std::string& end = "", bool key_only = false);

		/**
		 * close scanner
		 **/
		int		CloseScanner();

		/**
		 * get next scanner block
		 **/
		int		ScanNext(const char*& key, const char*& data, int& dlen);

		int		Get(const char* key, const char*& data, int& dlen);

	protected:
		/**
		 * using thrift
		 **/
		bool	Origin() { return mOrigin; }

		/**
		 * need thrift conenct
		 **/
		bool	Thrift() {
			return true;
			//return !Origin() || mIndex == -1;
		}

		/**
		 * get table name
		 * */
		const string& TableName() { return mTable.table_name; }

		/**
		 * get namespace name
		 * */
		const string& SpaceName() { return mTable.name_space; }

		/**
		 * get access group name
		 * */
		const string& AccessGroup() { return mTable.access_group; }

		/**
		 * get column family name
		 * */
		const string& ColumnFamily() { return mTable.column_family; }

		/**
		 * get column name
		 * */
		const string& Column() { return mTable.column; }

		/**
		 * get namespace handle
		 * */
		ThriftGen::Namespace SpaceHandle() { return mSpace; }

		/**
		 * report error, will exit
		 **/
		int		ReportError(const string& operation, ThriftGen::ClientException& e);

		/**
		 * get cell put flag
		 **/
		ThriftGen::KeyFlag::type ThrfitCellFlag(bool deleted) {
			return !deleted ? ThriftGen::KeyFlag::INSERT : ThriftGen::KeyFlag::DELETE_ROW;
		}

		/**
		 * get origin cell flag
		 * */
		int 	OriginCellFlag(bool deleted) {
			return !deleted ? Hypertable::FLAG_INSERT : Hypertable::FLAG_DELETE_ROW;
		}

		/**
		 * hql query
		 **/
		int		HqlQuery(const string& command, std::string& display, const std::string& prefix = "");

		/**
		 * query table schema
		 * */
		int		QuerySchema();

		/**
		 * put data array into table
		 */
		int		UpdateArray(int flag);

		/**
		 * extra length in the key
		 **/
		size_t	ExtraKeyLen() {
			return ColumnFamily().length() + Column().length() + 8 + 8 + 2;
		}

		/**
		 * increase statistic
		 **/
		void	RecordUpdate(int klen, int dlen);

		/**
		 * current update total length
		 * */
		void	RecordUpdate();

		/**
		 * increase statistic
		 **/
		void	RecordScan(int klen, int dlen);

	protected:
		int				mIndex;
		bool			mOrigin;
		KeyDataGen 		mGen;
		Table			mTable;
		ClientConfig 	mConfig;
		int				mAccuCount {};
		int				mAccuKeyLen {};
		int				mAccuDataLen {};

		ThriftGen::Namespace mSpace;
		ThriftGen::Schema mSchema;
		Thrift::Client*	mClient;
		RecordArray		mMutatorCache;
		RecordArray		mScannerCache;
		size_t			mScannerIndex;
		ThriftGen::Mutator mMutator;
		ThriftGen::Scanner mScanner;
		bool			mCreateMutator;
		bool			mCreateScanner;

#if ORIGIN_SUPPORT
		ClientPtr 		mOriginClient;
		NamespacePtr 	mOriginSpace;
		TablePtr 		mOriginTable;
		CellBuilderPtr	mOriginBuilder;
		TableMutatorPtr mOriginMutator;
		TableScannerPtr mOriginScanner;
		std::string		mOriginConfig;
#endif
	};
}

using namespace Hyper;

#endif //_HYPER_CLINET_H

