#ifndef FsBroker_Lib_Local_Client_h
#define FsBroker_Lib_Local_Client_h

#include <FsBroker/Lib/ClientBufferedReaderHandler.h>
#include <FsBroker/Lib/Broker.h>
#include <FsBroker/Lib/MetricsHandler.h>
#include <FsBroker/Lib/StatusManager.h>
#include <FsBroker/Lib/Client.h>

#include <Common/Filesystem.h>
#include <Common/Properties.h>
#include <Common/Status.h>
#include <Common/Properties.h>

#include <memory>
#include <mutex>
#include <unordered_map>
#include <FsBroker/Lib/MetricsHandler.h>
#include "LocalQueue.h"

namespace Hypertable {
namespace FsBroker {
namespace Lib {

class LocalClient: public Client
{
public:
	LocalClient(PropertiesPtr &cfg, int thread, bool logpath);
	virtual ~LocalClient();

public:
	void open(const String &name, uint32_t flags, DispatchHandler *handler) override { assert(0); }
	int open(const String &name, uint32_t flags) override;
	int open_buffered(const String &name, uint32_t flags, uint32_t buf_size,
			uint32_t outstanding, uint64_t start_offset = 0,
			uint64_t end_offset = 0) override;
	void decode_response_open(EventPtr &event, int32_t *fd) override { assert(0); }

	void create(const String &name, uint32_t flags,
			int32_t bufsz, int32_t replication,
			int64_t blksz, DispatchHandler *handler) override { assert(0); }
	int create(const String &name, uint32_t flags, int32_t bufsz,
			int32_t replication, int64_t blksz) override;
	void decode_response_create(EventPtr &event, int32_t *fd) override { assert(0); }

	void close(int32_t fd, DispatchHandler *handler) override {
		LocalQueue::LocalRequest* request = Request(LocalQueue::LocalRequest::LT_close, handler);
		request->Set(fd);
		m_queue->add(request);
	}
	void close(int32_t fd) override { close(fd, false); }

	void read(int32_t fd, size_t amount, DispatchHandler *handler) override {
		LocalQueue::LocalRequest* request = Request(LocalQueue::LocalRequest::LT_read, handler);
		request->Set(fd, amount);
		m_queue->add(request);
	}
	size_t read(int32_t fd, void *dst, size_t amount) override { return read(fd, dst, amount, NULL, false); }
	void decode_response_read(EventPtr &event, const void **buffer,
			uint64_t *offset, uint32_t *length) override;

	void append(int32_t fd, StaticBuffer &buffer, Flags flags,
			DispatchHandler *handler) override;
	size_t append(int32_t fd, StaticBuffer &buffer,
			Flags flags = Flags::NONE) override { return append(fd, buffer, flags, NULL, false); }
	void decode_response_append(EventPtr &event, uint64_t *offset,
			uint32_t *length) override;

	void seek(int32_t fd, uint64_t offset, DispatchHandler *handler) override { assert(0); }
	void seek(int32_t fd, uint64_t offset) override;

	void remove(const String &name, DispatchHandler *handler) override { assert(0); }
	void remove(const String &name, bool force = true) override;

	void length(const String &name, bool accurate,
			DispatchHandler *handler) override { assert(0); }
	int64_t length(const String &name, bool accurate = true) override;
	int64_t decode_response_length(EventPtr &event) override { assert(0); }

	void pread(int32_t fd, size_t len, uint64_t offset,
			bool verify_checksum, DispatchHandler *handler) override {
		LocalQueue::LocalRequest* request = Request(LocalQueue::LocalRequest::LT_pread, handler);
		request->Set(fd, len, offset);
		m_queue->add(request);
	}
	size_t pread(int32_t fd, void *dst, size_t len, uint64_t offset,
			bool verify_checksum) override { return pread(fd, dst, len, offset, verify_checksum, false); }
	void decode_response_pread(EventPtr &event, const void **buffer,
	uint64_t *offset, uint32_t *length) override { decode_response_read(event, buffer, offset, length); }

	void mkdirs(const String &name, DispatchHandler *handler) override { assert(0); }
	void mkdirs(const String &name) override;

	void flush(int32_t fd, DispatchHandler *handler) override { assert(0); }
	void flush(int32_t fd) override;

	void sync(int32_t fd) override /* { sync(fd, false); } */;

	void rmdir(const String &name, DispatchHandler *handler) override { assert(0); }
	void rmdir(const String &name, bool force = true) override;

	void readdir(const String &name, DispatchHandler *handler) override { assert(0); }
	void readdir(const String &name, std::vector<Dirent> &listing) override;
	void decode_response_readdir(EventPtr &event,
			std::vector<Dirent> &listing) override { assert(0); }

	void exists(const String &name, DispatchHandler *handler) override { assert(0); }
	bool exists(const String &name) override;
	bool decode_response_exists(EventPtr &event) override { assert(0); }

	void rename(const String &src, const String &dst,
			DispatchHandler *handler) override { assert(0); }
	void rename(const String &src, const String &dst) override;

	void status(Status &status, Timer *timer = 0) override;
	void decode_response_status(EventPtr &event, Status &status) override { assert(0); }

	void debug(int32_t command, StaticBuffer &serialized_parameters) override { assert(0); }
	void debug(int32_t command, StaticBuffer &serialized_parameters,
			DispatchHandler *handler)  override { assert(0); }

	enum
	{
		/// Perform immediate shutdown
		SHUTDOWN_FLAG_IMMEDIATE = 0x01
	};

	void shutdown(uint16_t flags, DispatchHandler *handler) { m_open_file_map.remove_all(); }

	LocalQueue::LocalRequest* Request(int type, DispatchHandler *handler);
	void	Work(LocalQueue::LocalRequest* request);

	void	async_read(LocalQueue::LocalRequest* request);
	void	async_append(LocalQueue::LocalRequest* request);
	void	async_pread(LocalQueue::LocalRequest* request);
	//void	async_sync(LocalQueue::LocalRequest* request);
	void	async_close(LocalQueue::LocalRequest* request);
	size_t  read(int32_t fd, void *dst, size_t len, uint64_t* _offset, bool async=false);
	size_t	pread(int32_t fd, void *dst, size_t len, uint64_t offset, bool verify_checksum, bool async=false);
	size_t  append(int32_t fd, StaticBuffer &buffer, Flags flags, uint64_t* offset, bool async=false);
	void	close(int32_t fd, bool async);
protected:
	std::mutex m_mutex;
	std::unordered_map<uint32_t, ClientBufferedReaderHandler *> m_buffered_reader_map;

	/// Metrics collection handler
	MetricsHandlerPtr m_metrics_handler;

	/// Server status manager
	StatusManager m_status_manager;

	String m_rootdir;
	bool m_verbose;
	bool m_directio;
	bool m_no_removal;
	bool m_logpath;

	OpenFileMap m_open_file_map;
	LocalQueuePtr m_queue;
};
/// Smart pointer to Client
typedef std::shared_ptr<Client> ClientPtr;
}
}
}

#endif // FsBroker_Lib_Local_Client_h

