#include <Common/Compat.h>
#include <Common/Error.h>
#include <Common/Filesystem.h>
#include <Common/Logger.h>
#include <Common/FileUtils.h>
#include <Common/Path.h>
#include <Common/String.h>
#include <Common/System.h>
#include <Common/SystemInfo.h>
#include <AsyncComm/Protocol.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
}

#include "FsBroker/Lib/Request/Handler/Factory.h"
#include "FsBroker/Lib/Request/Parameters/Append.h"
#include "FsBroker/Lib/Request/Parameters/Close.h"
#include "FsBroker/Lib/Request/Parameters/Create.h"
#include "FsBroker/Lib/Request/Parameters/Debug.h"
#include "FsBroker/Lib/Request/Parameters/Exists.h"
#include "FsBroker/Lib/Request/Parameters/Flush.h"
#include "FsBroker/Lib/Request/Parameters/Length.h"
#include "FsBroker/Lib/Request/Parameters/Mkdirs.h"
#include "FsBroker/Lib/Request/Parameters/Open.h"
#include "FsBroker/Lib/Request/Parameters/Pread.h"
#include "FsBroker/Lib/Request/Parameters/Readdir.h"
#include "FsBroker/Lib/Request/Parameters/Read.h"
#include "FsBroker/Lib/Request/Parameters/Remove.h"
#include "FsBroker/Lib/Request/Parameters/Rename.h"
#include "FsBroker/Lib/Request/Parameters/Rmdir.h"
#include "FsBroker/Lib/Request/Parameters/Seek.h"
#include "FsBroker/Lib/Request/Parameters/Shutdown.h"
#include "FsBroker/Lib/Request/Parameters/Sync.h"
#include "FsBroker/Lib/Response/Parameters/Append.h"
#include "FsBroker/Lib/Response/Parameters/Exists.h"
#include "FsBroker/Lib/Response/Parameters/Length.h"
#include "FsBroker/Lib/Response/Parameters/Open.h"
#include "FsBroker/Lib/Response/Parameters/Read.h"
#include "FsBroker/Lib/Response/Parameters/Readdir.h"
#include "FsBroker/Lib/Response/Parameters/Status.h"
#include "FsBroker/local/LocalBroker.h"

#include "LocalClient.h"
#include "../Common/Mutex.hpp"
#include "../Common/Logger.cpp"
#include "../Common/Util.cpp"
#include "../Common/Debug.cpp"
#include "../Common/File.cpp"
#include "../Statistic.cpp"

using namespace Hypertable;
using namespace Hypertable::FsBroker::Lib;
using namespace std;

static uint32_t s_extra_read = 0;
static uint32_t s_extra_append = 0;

void
LocalQueue::LocalRequest::Work()
{
	client->Work(this);
}

struct sockaddr_in s_addr;
LocalClient::LocalClient(PropertiesPtr &cfg, int thread, bool logpath)
	: Client(NULL, s_addr, 0), m_logpath(logpath)
{
	m_verbose = cfg->get_bool("verbose");
	m_no_removal = cfg->get_bool("FsBroker.DisableFileRemoval");

	if (cfg->has("DfsBroker.Local.DirectIO")) {
		m_directio = cfg->get_bool("DfsBroker.Local.DirectIO");
	} else {
		m_directio = cfg->get_bool("FsBroker.Local.DirectIO");
	}

	//m_metrics_handler = std::make_shared<MetricsHandler>(cfg, "local");
	//m_metrics_handler->start_collecting();

	/**
	 * Determine root directory
	 */
	Path root;
	if (cfg->has("DfsBroker.Local.Root")) {
		root = Path(cfg->get_str("DfsBroker.Local.Root"));

	} else if (cfg->has("FsBroker.Local.Root")) {
		root = Path(cfg->get_str("FsBroker.Local.Root"));

	} else {
		root = Path(cfg->get_str("root", ""));
	}

	if (!root.is_complete()) {
		Path data_dir = cfg->get_str("Hypertable.DataDirectory");
		root = data_dir / root;
	}

	m_rootdir = root.string();
	//m_rootdir = "/mnt/data/single/";
	//m_rootdir = "/mnt/ram";
	if (m_logpath) {
		m_rootdir = "/mnt/ssd";

	} else {
		m_rootdir = "/mnt/yfs";
	//	m_rootdir = "/mnt/data";
	}

	// ensure that root directory exists
	if (!FileUtils::mkdirs(m_rootdir)) {
		exit(EXIT_FAILURE);
	}

	m_queue = std::make_shared<LocalQueue>(thread);

	if (s_extra_read == 0) {
		Response::Parameters::Read read(0, 0);
		s_extra_read = read.encoded_length() + 4;

		Response::Parameters::Append append(0, 0);
		s_extra_append = append.encoded_length() + 4;
	}
}


LocalClient::~LocalClient() {
	if (m_queue) {
		m_queue->stop();
	}
	//m_metrics_handler->stop_collecting();
}

LocalQueue::LocalRequest*
LocalClient::Request(int type, DispatchHandler *handler)
{
	LocalQueue::LocalRequest* request = LocalQueue::LocalRequest::Create();
	request->handler = handler;
	request->client	= this;
	request->type = type;
	return request;
}

void
LocalClient::Work(LocalQueue::LocalRequest* request)
{
	switch (request->type) {

	case LocalQueue::LocalRequest::LT_read:{
		async_read(request);
	}break;

	case LocalQueue::LocalRequest::LT_pread:{
		async_pread(request);
	}break;

	case LocalQueue::LocalRequest::LT_append:{
		async_append(request);
	}break;

//	case LocalQueue::LocalRequest::LT_sync:{
//		async_sync(request);
//	}break;

	case LocalQueue::LocalRequest::LT_close:{
		async_close(request);
	}break;

	default:{
		assert(0);
	}break;

	}
}

void
LocalClient::async_read(LocalQueue::LocalRequest* request)
{
	EventPtr event = std::make_shared<Event>(Event::MESSAGE);
	event->payload_len = request->len + s_extra_read;
	event->payload = new uint8_t[event->payload_len];

	int ret = 0;
	try {
		ret = read(request->fd, (void*)(event->payload + s_extra_read), request->len, &request->offset, true);
	} catch (Exception &e) {
		HT_ERROR_OUT<< e << HT_END;
	}

	uint8_t* ptr = (uint8_t*)event->payload;
	uint8_t** bufp = &ptr;
	Serialization::encode_i32(bufp, ret == -1 ? -1 : 0);

	Response::Parameters::Read params(request->offset, ret == -1 ? 0 : ret);
	params.encode(bufp);

	request->handler->handle(event);
}

void
LocalClient::async_pread(LocalQueue::LocalRequest* request)
{
	EventPtr event = std::make_shared<Event>(Event::MESSAGE);
	event->payload_len = request->len + s_extra_read;
	event->payload = new uint8_t[event->payload_len];

	int ret = 0;
	try {
		ret = pread(request->fd, (void*)(event->payload + s_extra_read), request->len, request->offset, false, true);
	} catch (Exception &e) {
		HT_ERROR_OUT<< e << HT_END;
	}

	uint8_t* ptr = (uint8_t*)event->payload;
	uint8_t** bufp = &ptr;

	Serialization::encode_i32(bufp, ret == -1 ? -1 : 0);

	Response::Parameters::Read params(request->offset, ret == -1 ? 0 : ret);
	params.encode(bufp);

	request->handler->handle(event);
}

void
LocalClient::async_close(LocalQueue::LocalRequest* request)
{
	EventPtr event = std::make_shared<Event>(Event::MESSAGE);
	event->payload_len = 4;
	event->payload = new uint8_t[event->payload_len];

	try {
		close(request->fd, true);
	} catch (Exception &e) {
		HT_ERROR_OUT<< e << HT_END;
	}
	uint8_t* ptr = (uint8_t*)event->payload;
	uint8_t** bufp = &ptr;

	Serialization::encode_i32(bufp, 0);

	request->handler->handle(event);
}

void
LocalClient::async_append(LocalQueue::LocalRequest* request)
{
	EventPtr event = std::make_shared<Event>(Event::MESSAGE);
	event->payload_len = s_extra_append;
	event->payload = new uint8_t[event->payload_len];

	int ret = 0;
	try {
		ret = append(request->fd, request->buffer, request->flags, &request->offset, true);
	} catch (Exception &e) {
		HT_ERROR_OUT<< e << HT_END;
	}

	uint8_t* ptr = (uint8_t*)event->payload;
	uint8_t** bufp = &ptr;
	Serialization::encode_i32(bufp, ret == -1 ? -1 : 0);

	Response::Parameters::Append params(request->offset, request->len);
	params.encode(bufp);

	request->handler->handle(event);
}

int
LocalClient::open(const String &name, uint32_t flags) {

	const char* fname = name.c_str();

	String abspath;
	HT_DEBUGF("open file='%s' flags=%u", fname, flags);
	if (name[0] == '/') {
		abspath = m_rootdir + name;
	} else {
		abspath = m_rootdir + "/" + name;
	}
	int oflags = O_RDONLY;
	if (m_directio && (flags & Filesystem::OPEN_FLAG_DIRECTIO)) {
		oflags |= O_DIRECT;
	}

	int fd;
	if ((fd = ::open(abspath.c_str(), oflags)) == -1) {
		time_debug_using("open " << name << ", but failed: " << strerror(errno));
		HT_ERRORF("open failed: file='%s' - %s", abspath.c_str(), strerror(errno));
		HT_THROWF(errno, "Error opening FS file: %s", name.c_str());
		return -1;
	}
	HT_INFOF("open( %s ) = %d ", fname, fd);

	{
		struct sockaddr_in addr;
		OpenFileDataLocalPtr fdata(
			new OpenFileDataLocal(fname, fd, oflags));

		m_open_file_map.create(fd, addr, fdata);
	}

	time_trace_using("open " << name << ", flag " << oflags
		<< ", fd " << fd << ", direct " << ((oflags & O_DIRECT) ? "true" : "false"));
	return fd;
}

int
LocalClient::open_buffered(const String &name, uint32_t flags, uint32_t buf_size,
	  uint32_t outstanding, uint64_t start_offset,
	  uint64_t end_offset)
{
	try {
		HT_ASSERT((flags & Filesystem::OPEN_FLAG_DIRECTIO) == 0 ||
				(HT_IO_ALIGNED(buf_size) &&
				HT_IO_ALIGNED(start_offset) &&
				HT_IO_ALIGNED(end_offset)));
		int fd = open(name, flags | OPEN_FLAG_VERIFY_CHECKSUM);
		{
			lock_guard<mutex> lock(m_mutex);
			HT_ASSERT(
					m_buffered_reader_map.find(fd)
							== m_buffered_reader_map.end());
			m_buffered_reader_map[fd] =
					new ClientBufferedReaderHandler(this, fd, buf_size,
							outstanding,
							start_offset, end_offset);
		}
		time_trace_using("open buffered " << name << ", flag " << flags << ", fd " << fd);
		return fd;

	} catch (Exception &e) {
		HT_THROW2F(e.code(), e, "Error opening buffered FS file=%s buf_size=%u "
			"outstanding=%u start_offset=%llu end_offset=%llu",
			name.c_str(),
			buf_size, outstanding, (Llu )start_offset, (Llu )end_offset);
	}
}

int
LocalClient::create(const String &name, uint32_t flags, int32_t bufsz,
	int32_t replication, int64_t blksz)
{
	const char* fname = name.c_str();
	String abspath;
	HT_DEBUGF("create file='%s' flags=%u bufsz=%d replication=%d blksz=%lld",
		fname, flags, bufsz, (int )replication, (Lld )blksz);

	if (name[0] == '/') {
		abspath = m_rootdir + name;
	} else {
		abspath = m_rootdir + "/" + name;
	}

	int oflags = O_WRONLY | O_CREAT;
	if (flags & Filesystem::OPEN_FLAG_OVERWRITE) {
		oflags |= O_TRUNC;
	} else {
		oflags |= O_APPEND;
	}

	if (m_directio && (flags & Filesystem::OPEN_FLAG_DIRECTIO)) {
		oflags |= O_DIRECT;
	}

	int fd;
	if ((fd = ::open(abspath.c_str(), oflags, 0644)) == -1) {
		time_debug_using("create " << name << ", but failed: " << strerror(errno));
		HT_ERRORF("create failed: file='%s' - %s", abspath.c_str(), strerror(errno));
		HT_THROWF(errno, "Error creating FS file: %s", name.c_str());
		return -1;
	}

	HT_INFOF("create( %s ) = %d", fname, (int )fd);

	{
		struct sockaddr_in addr;
		OpenFileDataLocalPtr fdata(
			new OpenFileDataLocal(fname, fd, oflags));

		m_open_file_map.create(fd, addr, fdata);
	}

	time_trace_using("create " << name << ", flag " << oflags
		<< ", fd " << fd << ", direct " << ((oflags & O_DIRECT) ? "true" : "false"));
	return fd;
}

void
LocalClient::close(int32_t fd, bool async)
{
	if (0 && !m_logpath && !async) {
		//OpenFileDataLocalPtr fdata;
		//if (m_open_file_map.get(fd, fdata) && fdata->async) {
		EventPtr event;
		DispatchHandlerSynchronizer sync_handler;
		close(fd, &sync_handler);
		sync_handler.wait_for_reply(event);
		return;
		//}
	}

	time_reset();
	{
		lock_guard<mutex> lock(m_mutex);
		auto iter = m_buffered_reader_map.find(fd);
		if (iter != m_buffered_reader_map.end()) {
			ClientBufferedReaderHandler *reader_handler
				= (*iter).second;
			m_buffered_reader_map.erase(iter);
			delete reader_handler;
		}
	}

#if 0
	OpenFileDataLocalPtr fdata;
	if (m_open_file_map.get(fd, fdata)) {
		if( strstr(fdata->filename.c_str(),"cs") != NULL){
			HT_DEBUGF("file -- %s", fdata->filename.c_str());			
		}
	}
#endif

	HT_DEBUGF("close fd=%d", fd);
	m_open_file_map.remove(fd);
	
	//if (::close(fd) != 0) {
	//	HT_ERRORF("close failed: %d - %s", errno, strerror(errno));
	//}
	time_trace_using("close, fd " << fd);
	file_inc(ST_sync_time, time_last());
}

size_t
LocalClient::read(int32_t fd, void *dst, size_t len, uint64_t* _offset, bool async)
{
	time_reset();
	/** only sync read will do this */
	if (!async) {
		ClientBufferedReaderHandler *reader_handler = 0;
		{
			lock_guard<mutex> lock(m_mutex);
			auto iter = m_buffered_reader_map.find(fd);
			if (iter != m_buffered_reader_map.end())
				reader_handler = (*iter).second;
		}
		if (reader_handler) {
			return reader_handler->read(dst, len);
		}
	}

	OpenFileDataLocalPtr fdata;
	ssize_t nread;
	uint64_t offset;
	uint64_t amount = len;

	HT_DEBUGF("read fd=%d len=%d", fd, (int)len);

	if (!m_open_file_map.get(fd, fdata)) {
		time_debug_using("read fd " << fd << ", but not opened");
		HT_ERRORF("bad file handle: %d", fd);
		//m_metrics_handler->increment_error_count();
		HT_THROWF(errno, "Error reading %u bytes from FS fd %d",
					(unsigned)len, (int)fd);
		return -1;
	}
	//fdata->async = async;

	StaticBuffer buf;
	if ((fdata->flags & O_DIRECT) &&
		(!HT_IO_ALIGNED((size_t)dst) || !HT_IO_ALIGNED((size_t)len)))
	{
		StaticBuffer temp((size_t)len, (size_t)HT_DIRECT_IO_ALIGNMENT);
		buf = temp;

		amount = buf.aligned_size();
		time_trace_using("using direct io, copy " << amount << " for directo io");

	} else {
		buf.set((uint8_t*)dst, (uint32_t)len, false);
	}

	if (_offset) {
		//time_trace_using("read: try to get offset");
		if ((offset = (uint64_t) lseek(fd, 0, SEEK_CUR)) == (uint64_t) -1) {
			//int error = errno;
			//if (error != EINVAL) {
				//m_status_manager.set_read_error(error);
			//}
			time_debug_using("seek fd "<< fd << ", but failed: " << strerror(errno));
			HT_ERRORF("lseek failed: fd=%d offset=0 SEEK_CUR - %s", fd,
				strerror(errno));
			HT_THROWF(errno, "Error reading %u bytes from FS fd %d",
				(unsigned)len, (int)fd);
		}
		*_offset = offset;
	}

	if ((nread = FileUtils::read(fd, buf.base, amount)) == -1) {
		time_debug_using("read fd " << fd << ", but failed: " << strerror(errno));
		//int error = errno;
		//m_status_manager.set_read_error(error);
		HT_ERRORF("read failed: fd=%d offset=%llu len=%d - %s",
				fd, (Llu )offset, (int)len, strerror(errno));
		HT_THROWF(errno, "Error reading %u bytes from FS fd %d",
					(unsigned)len, (int)fd);
		return -1;
	}

	if (buf.own) {
		assert((ssize_t)len >= nread);
		memcpy(dst, buf.base, nread);
	}

	//m_metrics_handler->add_bytes_read(buf.size);
	//m_status_manager.clear_status();
	time_trace_using("read fd " << fd << ", len " << string_size(nread));

	file_inc(ST_read, nread);
	return nread;
}

void
LocalClient::decode_response_read(EventPtr &event, const void **buffer,
	uint64_t *offset, uint32_t *length)
{
	int error = Protocol::response_code(event);
	if (error != Error::OK) {
		HT_THROW(error, Protocol::string_format_message(event));
	}

	const uint8_t *ptr = event->payload + 4;
	size_t remain = event->payload_len - 4;

	Response::Parameters::Read params;
	params.decode(&ptr, &remain);
	*offset = params.get_offset();
	*length = params.get_amount();

	if (*length == (uint32_t) -1) {
		*length = 0;
		return;
	}

	if (remain < (size_t) *length) {
		time_debug_using("response failed");
		HT_THROWF(Error::RESPONSE_TRUNCATED, "%lu < %lu", (Lu )remain,
				(Lu )*length);
	}

	*buffer = ptr;
}

void
LocalClient::append(int32_t fd, StaticBuffer &buffer, Flags flags,
			DispatchHandler *handler)
{
#if 0
	EventPtr event = std::make_shared<Event>(Event::MESSAGE);
	event->payload_len = 4;
	event->payload = new uint8_t[event->payload_len];
	uint8_t* ptr = (uint8_t*)event->payload;
	uint8_t** bufp = &ptr;
	Serialization::encode_i32(bufp, 0);

	append(fd, buffer, flags, NULL, true);
	handler->handle(event);
	return;
#endif
	LocalQueue::LocalRequest* request = Request(
			LocalQueue::LocalRequest::LT_append, handler);

	if (m_directio && !HT_IO_ALIGNED((size_t)buffer.base)) {
		StaticBuffer temp((size_t)buffer.size, (size_t)HT_DIRECT_IO_ALIGNMENT);
		assert(buffer.size == temp.size);
		memcpy(temp.base, buffer.base, buffer.size);

		request->buffer = temp;
		time_trace_using("using direct io, copy " << buffer.size << " for directo io");

	} else {
		request->buffer = buffer;
	}

	request->Set(fd, flags);
	m_queue->add(request);
}

size_t
LocalClient::append(int32_t fd, StaticBuffer &buffer, Flags flags, uint64_t* _offset, bool async)
{
	time_reset();

	OpenFileDataLocalPtr fdata;
	ssize_t nwritten;
	uint64_t offset;
	int amount = buffer.size;
	const void *data = buffer.base;

	HT_DEBUG_OUT<< "append fd=" << fd << " amount=" << amount << " data='"
			<< format_bytes(20, data, amount) << " flags="
			<< static_cast<uint8_t>(flags) << HT_END;

	if (!m_open_file_map.get(fd, fdata)) {
		time_debug_using("append fd " << fd << ", but not opened");
		HT_ERRORF("append, but get bad file handle: %d", fd);
		//m_metrics_handler->increment_error_count();
		HT_THROWF(errno, "Error appending %u bytes to FS fd %d",
				       (unsigned)buffer.size, (int)fd);
	}
	//fdata->async = async;

	if (_offset) {
		if ((offset = (uint64_t) lseek(fd, 0, SEEK_CUR)) == (uint64_t) -1) {
			//int error = errno;
//			if (error != EINVAL) {
				//m_status_manager.set_write_error(error);
//			}
			time_debug_using("append fd "<< fd << ", but get offset failed: " << strerror(errno));
			HT_ERRORF("lseek failed: fd=%d offset=0 SEEK_CUR - %s", fd,
					strerror(errno));
			HT_THROWF(errno, "Error appending %u bytes to FS fd %d",
						   (unsigned)buffer.size, (int)fd);
		}
		*_offset = offset;
		time_trace_using("append: get offset " << offset);
	}

	if ((nwritten = FileUtils::write(fd, data, amount)) == -1) {
		time_debug_using("write fd " << fd << ", but failed: " << strerror(errno));
		//int error = errno;
		//m_status_manager.set_write_error(error);
		HT_ERRORF("write failed: fd=%d offset=%llu amount=%d data=%p- %s",
				fd, (Llu )offset, amount, data, strerror(errno));
		HT_THROWF(errno, "Error appending %u bytes to FS fd %d",
		       (unsigned)buffer.size, (int)fd);
	}

	time_trace_using("append fd " << fd << ", len " << string_size(nwritten));
	file_inc(ST_write, nwritten);

	if (flags == Filesystem::Flags::FLUSH || flags == Filesystem::Flags::SYNC) {
		//int64_t start_time = get_ts64();
		if (fsync(fd) != 0) {
			//int error = errno;
			//m_status_manager.set_write_error(error);
			time_debug_using("flush fd "<< fd << ", but failed: " << strerror(errno));
			HT_ERRORF("flush failed: fd=%d - %s", fd, strerror(errno));
			HT_THROWF(errno, "Error appending %u bytes to FS fd %d",
				(unsigned)buffer.size, (int)fd);
		}

		time_trace_using("append, sync");
		file_inc(ST_sync);
		file_inc(ST_sync_time, time_last());
		//m_metrics_handler->add_sync(get_ts64() - start_time);
	}

	//m_metrics_handler->add_bytes_written(nwritten);
	//m_status_manager.clear_status();
	return nwritten;
}

void LocalClient::decode_response_append(EventPtr &event, uint64_t *offset,
      uint32_t *length)
{
	int error = Protocol::response_code(event);
	if (error != Error::OK)
		HT_THROW(error, Protocol::string_format_message(event));

	const uint8_t *ptr = event->payload + 4;
	size_t remain = event->payload_len - 4;

	Response::Parameters::Append params;
	params.decode(&ptr, &remain);
	*offset = params.get_offset();
	*length = params.get_amount();
}

void
LocalClient::seek(int32_t fd, uint64_t offset)
{
	OpenFileDataLocalPtr fdata;

	HT_DEBUGF("seek fd=%lu offset=%llu", (Lu )fd, (Llu )offset);

	if (!m_open_file_map.get(fd, fdata)) {
		time_debug_using("seek fd " << fd << ", but not opened");
		HT_ERRORF("bad file handle: %d", fd);
		//m_metrics_handler->increment_error_count();
		return;
	}

	if ((offset = (uint64_t) lseek(fd, offset, SEEK_SET)) == (uint64_t) -1) {
		HT_ERRORF("lseek failed: fd=%d offset=%llu - %s", fd, (Llu )offset,
				strerror(errno));
	}
}

void
LocalClient::remove(const String &name, bool force)
{
	time_reset();

	String abspath;
	const char* fname = name.c_str();

	HT_INFOF("remove file='%s'", fname);
	if (fname[0] == '/') {
		abspath = m_rootdir + fname;
	} else {
		abspath = m_rootdir + "/" + fname;
	}

	if (m_no_removal) {
		String deleted_file = abspath + ".deleted";
		if (!FileUtils::rename(abspath, deleted_file)) {
			time_debug_using("rename " << abspath << " to " << deleted_file << ", but failed: " << strerror(errno));
			HT_THROWF(errno, "Error removing FS file: %s", name.c_str());
			return;
		}
	} else {
		if (unlink(abspath.c_str()) == -1) {
			time_debug_using("unlink " << abspath << ", but failed: " << strerror(errno));
			HT_ERRORF("unlink failed: file='%s' - %s", abspath.c_str(),
					strerror(errno));
			HT_THROWF(errno, "Error removing FS file: %s", name.c_str());
			return;
		}
	}
	time_trace_using("remove " << name);

	file_inc(ST_remove);
	file_inc(ST_remove_time, time_last());
}

void LocalClient::status(Status &status, Timer *timer) {
	Status::Code code = Hypertable::Status::Code::OK;
	std::string text;
	//m_status_manager.get().get(&code, text);
    status.set(code, text);
}

int64_t LocalClient::length(const String &name, bool accurate)
{
	String abspath;
	uint64_t length;
	const char* fname = name.c_str();

	HT_DEBUGF("length file='%s' (accurate=%s)", fname,
			accurate ? "true" : "false");

	if (fname[0] == '/') {
		abspath = m_rootdir + fname;
	} else {
		abspath = m_rootdir + "/" + fname;
	}

	if ((length = FileUtils::length(abspath)) == (uint64_t) -1) {
		time_debug_using("get length " << abspath << ", but failed: " << strerror(errno));
		HT_ERRORF("length (stat) failed: file='%s' - %s", abspath.c_str(),
				strerror(errno));
		HT_THROWF(errno, "Error getting length of FS file: %s",
				name.c_str());
		return -1;
	}
	return length;
}

size_t
LocalClient::pread(int32_t fd, void *dst, size_t len, uint64_t offset, bool verify_checksum, bool async)
{
	time_reset();

	OpenFileDataLocalPtr fdata;
	ssize_t nread;
	int amount = len;

	HT_DEBUGF("pread fd=%d offset=%llu amount=%d", fd, (Llu )offset, amount);

	if (!m_open_file_map.get(fd, fdata)) {
		time_debug_using("pread fd " << fd << ", but not opened");

		HT_ERRORF("bad file handle: %d", fd);
		//m_metrics_handler->increment_error_count();
		HT_THROWF(errno, "Error preading at byte %llu on FS fd %d",
			(Llu)offset, (int)fd);
	}
	//fdata->async = async;

	StaticBuffer buf;
	if ((fdata->flags & O_DIRECT) &&
		(!HT_IO_ALIGNED((size_t)dst) || !HT_IO_ALIGNED((size_t)len)))
	{
		StaticBuffer temp((size_t)len, (size_t)HT_DIRECT_IO_ALIGNMENT);
		buf = temp;
		amount = buf.aligned_size();

		time_trace_using("alloc " << amount << " for directo io");

	} else {
		buf.set((uint8_t*)dst, (uint32_t)len, false);
	}

	nread = FileUtils::pread(fd, buf.base, amount,
			(off_t) offset);
	if (nread != (ssize_t) amount) {
		time_debug_using("pread fd "<< fd << ", but length not match, failed: " << strerror(errno));
		//int error = errno;
		//m_status_manager.set_read_error(error);
		HT_ERRORF(
				"pread failed: fd=%d amount=%d aligned_size=%d offset=%llu - %s",
				fd, (int )len, (int )buf.aligned_size(), (Llu )offset,
				strerror(errno));
		HT_THROWF(errno, "Error preading at byte %llu on FS fd %d",
				(Llu)offset, (int)fd);
		return -1;
	}

	if (buf.own) {
		assert((ssize_t)len >= nread);
		memcpy(dst, buf.base, nread);
	}

	//m_metrics_handler->add_bytes_read(nread);
	//m_status_manager.clear_status();

	time_trace_using("pread fd " << fd << ", len " << string_size(nread));

	file_inc(ST_read, nread);
	return nread;
}

void
LocalClient::mkdirs(const String &name) {
	String absdir;
	const char* dname = name.c_str();

	HT_DEBUGF("mkdirs dir='%s'", dname);

	if (dname[0] == '/') {
		absdir = m_rootdir + dname;
	} else {
		absdir = m_rootdir + "/" + dname;
	}

	if (!FileUtils::mkdirs(absdir)) {
		HT_ERRORF("mkdirs failed: dname='%s' - %s", absdir.c_str(),
				strerror(errno));
		return;
	}

	time_debug_using("mkdir " << name);
}


void
LocalClient::flush(int32_t fd) {
	sync(fd);
}

void
LocalClient::sync(int32_t fd) {

	time_reset();

	OpenFileDataLocalPtr fdata;

	HT_DEBUGF("sync fd=%d", fd);

	if (!m_open_file_map.get(fd, fdata)) {
		time_debug_using("sync fd " << fd << ", but not opened");
		HT_ERRORF("bad file handle: %d", fd);
		//m_metrics_handler->increment_error_count();
		return;
	}
	////fdata->async = async;

	//int64_t start_time = get_ts64();
	if (fsync(fd) != 0) {
		//m_status_manager.set_write_error(errno);
		HT_ERRORF("sync failed: fd=%d - %s", fd, strerror(errno));
		return;
	}

	//m_metrics_handler->add_sync(get_ts64() - start_time);
	//m_status_manager.clear_status();

	time_trace_using("sync fd " << fd);

	file_inc(ST_sync);
	file_inc(ST_sync_time, time_last());
}

void
LocalClient::rmdir(const String &name, bool force)
{
	time_reset();

	String absdir;
	String cmd_str;
	const char* dname = name.c_str();

	if (m_verbose) {
		HT_INFOF("rmdir dir='%s'", dname);
	}

	if (dname[0] == '/') {
		absdir = m_rootdir + dname;
	} else {
		absdir = m_rootdir + "/" + dname;
	}

	if (FileUtils::exists(absdir)) {
		if (m_no_removal) {
			String deleted_file = absdir + ".deleted";
			if (!FileUtils::rename(absdir, deleted_file)) {
				time_debug_using("rename " << absdir << " to " << deleted_file << ", but failed: " << strerror(errno));
				HT_THROWF(errno, "Error removing FS directory: %s", name.c_str());
				return;
			}
		}
		else {
			cmd_str = (String) "/bin/rm -rf " + absdir;
			time_trace_using("try to rmdir " << absdir);

			if (system(cmd_str.c_str()) != 0) {
				time_debug_using("rmdir " << absdir << ", but failed: " << strerror(errno));
				HT_ERRORF("%s failed.", cmd_str.c_str());
				//m_metrics_handler->increment_error_count();
				HT_THROWF(errno, "Error removing FS directory: %s", name.c_str());
				return;
			}
		}
	} else {
		if (!force) {
			time_debug_using("rm " << absdir << ", but not exist");
			HT_THROWF(errno, "Error removing FS directory: %s", name.c_str());
		}
	}

#if 0
	if (rmdir(absdir.c_str()) != 0) {
		report_error(cb);
		HT_ERRORF("rmdir failed: dname='%s' - %s", absdir.c_str(), strerror(errno));
		return;
	}
#endif

	time_debug_using("rmdir " << absdir);

	file_inc(ST_rmdir);
	file_inc(ST_rmdir_time, time_last());
}

void LocalClient::readdir(const String &name, std::vector<Dirent> &listing) {
	Filesystem::Dirent entry;
	String absdir;
	const char* dname = name.c_str();

	HT_DEBUGF("Readdir dir='%s'", dname);

	if (dname[0] == '/') {
		absdir = m_rootdir + dname;
	} else {
		absdir = m_rootdir + "/" + dname;
	}

	DIR *dirp = opendir(absdir.c_str());
	if (dirp == 0) {
		time_debug_using("opendir " << absdir << ", but failed: " << strerror(errno));
		HT_ERRORF("opendir('%s') failed - %s", absdir.c_str(), strerror(errno));
		HT_THROWF(errno, "Error reading directory entries for FS "
						"directory: %s", name.c_str());
		return;
	}

	struct dirent *dp = (struct dirent *) new uint8_t[sizeof(struct dirent)
			+ 1025];
	struct dirent *result;

	if (readdir_r(dirp, dp, &result) != 0) {
		time_debug_using("readdir " << absdir << ", but failed: " << strerror(errno));
		HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(), strerror(errno));
		(void) closedir(dirp);
		delete[] (uint8_t *) dp;
		HT_THROWF(errno, "Error reading directory entries for FS "
				"directory: %s", name.c_str());
		return;
	}

	String full_entry_path;
	struct stat statbuf;
	while (result != 0) {

		if (result->d_name[0] != '.' && result->d_name[0] != 0) {
			if (m_no_removal) {
				size_t len = strlen(result->d_name);
				if (len <= 8 || strcmp(&result->d_name[len - 8], ".deleted")) {
					entry.name.clear();
					entry.name.append(result->d_name);
					entry.is_dir = result->d_type == DT_DIR;
					full_entry_path.clear();
					full_entry_path.append(absdir);
					full_entry_path.append("/");
					full_entry_path.append(entry.name);
					if (stat(full_entry_path.c_str(), &statbuf) == -1) {
						if (errno != ENOENT) {
							time_debug_using("readdir " << absdir << ", but failed: " << strerror(errno));
							HT_ERRORF("readdir('%s') failed - %s",
									absdir.c_str(), strerror(errno));
							delete[] (uint8_t *) dp;
							HT_THROWF(errno, "Error reading directory entries for FS "
											"directory: %s", name.c_str());
							return;
						}
					}
					else {
						entry.length = (uint64_t) statbuf.st_size;
						entry.last_modification_time = statbuf.st_mtime;
						listing.push_back(entry);
					}
				}
			}
			else {
				entry.name.clear();
				entry.name.append(result->d_name);
				entry.is_dir = result->d_type == DT_DIR;
				full_entry_path.clear();
				full_entry_path.append(absdir);
				full_entry_path.append("/");
				full_entry_path.append(entry.name);
				if (stat(full_entry_path.c_str(), &statbuf) == -1) {
					time_debug_using("readdir " << absdir << ", but failed: " << strerror(errno));
					HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(),
							strerror(errno));
					delete[] (uint8_t *) dp;
					HT_THROWF(errno, "Error reading directory entries for FS "
							"directory: %s", name.c_str());
					return;
				}
				entry.length = (uint64_t) statbuf.st_size;
				entry.last_modification_time = statbuf.st_mtime;
				listing.push_back(entry);
			}
			//HT_INFOF("readdir Adding listing '%s'", result->d_name);
		}

		if (readdir_r(dirp, dp, &result) != 0) {
			time_debug_using("readdir " << absdir << ", but failed: " << strerror(errno));
			HT_ERRORF("readdir('%s') failed - %s", absdir.c_str(),
					strerror(errno));
			delete[] (uint8_t *) dp;
			HT_THROWF(errno, "Error reading directory entries for FS "
					"directory: %s", name.c_str());
			return;
		}
	}
	(void) closedir(dirp);

	delete[] (uint8_t *) dp;

	HT_DEBUGF("Sending back %d listings", (int )listing.size());
}

bool LocalClient::exists(const String &name) {
	String abspath;
	const char* fname = name.c_str();

	if (fname[0] == '/') {
		abspath = m_rootdir + fname;
	} else {
		abspath = m_rootdir + "/" + fname;
	}
	if (!FileUtils::exists(abspath)) {
		HT_DEBUGF("not exist file='%s'", fname);
		return false;
	}

	HT_DEBUGF("exists file='%s'", fname);
	time_trace_using("exists " << fname);
	return true;
}

void
LocalClient::rename(const String &_src, const String &_dst)
{
	time_reset();

	const char* src = _src.c_str();
	const char* dst = _dst.c_str();

	HT_INFOF("rename %s -> %s", src, dst);

	String asrc =
			format("%s%s%s", m_rootdir.c_str(), *src == '/' ? "" : "/", src);
	String adst =
			format("%s%s%s", m_rootdir.c_str(), *dst == '/' ? "" : "/", dst);

	if (std::rename(asrc.c_str(), adst.c_str()) != 0) {
		return;
	}

	time_debug_using("rename " << asrc << " to " << adst);
}



