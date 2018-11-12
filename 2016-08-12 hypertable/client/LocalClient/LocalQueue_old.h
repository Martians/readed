#ifndef Hypertable_Local_Queue_h
#define Hypertable_Local_Queue_h

#include <Common/Logger.h>
#include <Common/StringExt.h>
#include <Common/Thread.h>

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace Hypertable {
namespace FsBroker {
namespace Lib {

class LocalClient;


class LocalRequest
{
public:
	LocalRequest(int t = LT_null, DispatchHandler *h = NULL, LocalClient* c = NULL)
		: type(t), handler(h), client(c)
	{
	}

	~LocalRequest() { }

	enum {
		LT_null = 0,
		LT_read,
		LT_pread,
		LT_append,
	};

public:

	static LocalRequest* Create() { return new LocalRequest(); }

	static void Remove(LocalRequest *request) { delete request; }

public:
	void	Work();

	void	Set(int32_t _fd, uint64_t _len, uint64_t _offset = 0) {
		fd 	= _fd;
		len = _len;
		offset = _offset;
	}

	void	Set(int32_t _fd, StaticBuffer* _buffer, Filesystem::Flags _flags) {
		fd 		= _fd;
		buffer 	= _buffer;
		flags 	= _flags;
	}

public:
	int		type;
	DispatchHandler* handler;
	LocalClient* client;
	std::string	name;

	int		fd;
	Filesystem::Flags flags;
	StaticBuffer* buffer;
	uint64_t len;
	uint64_t offset;
};

class LocalQueue
{
public:
	LocalQueue(int worker_count)
		: joined(false)
	{
		m_state.threads_total = worker_count;
		Worker Worker(m_state);
		assert(worker_count > 0);
		for (int i = 0; i < worker_count; ++i) {
			m_thread_ids.push_back(m_threads.create_thread(Worker)->get_id());
		}
	}

	/** Destructor.
	 */
	virtual ~LocalQueue()
	{
		if (!joined)
		{
			shutdown();
			join();
		}
	}

	/** Individual request queue
	 */
	typedef std::list<LocalRequest *> RequestQueue;

	/** Application queue state shared among worker threads.
	 */
	class LocalQueueState
	{
	public:
		LocalQueueState()
			: threads_available(0), threads_total(0), shutdown(false), paused(false)
		{
		}

		/// Normal request queue
		RequestQueue queue;

		/// %Mutex for serializing concurrent access
		std::mutex mutex;

		/// Condition variable to signal pending handlers
		std::condition_variable cond;

		/// Condition variable used to signal <i>quiesced</i> queue
		std::condition_variable quiesce_cond;

		/// Idle thread count
		size_t threads_available;

		/// Total initial threads
		size_t threads_total;

		/// Flag indicating if shutdown is in progress
		bool shutdown;

		/// Flag indicating if queue has been paused
		bool paused;
	};

	/** Application queue worker thread function (functor)
	 */
	class Worker
	{

	public:
		Worker(LocalQueueState &qstate)
			: m_state(qstate)
		{
		}

		/** Thread run method
		 */
		void operator()() {
			LocalRequest *request = 0;
			RequestQueue::iterator iter;

			while (true)
			{
				{
					std::unique_lock<std::mutex> lock(m_state.mutex);

					m_state.threads_available++;
					while (m_state.paused || m_state.queue.empty()) {
						if (m_state.shutdown) {
							m_state.threads_available--;
							return;
						}
						if (m_state.threads_available == m_state.threads_total) {
							m_state.quiesce_cond.notify_all();
						}
						m_state.cond.wait(lock);
					}

					if (m_state.shutdown) {
						m_state.threads_available--;
						return;
					}

					request = 0;

					if (!m_state.paused) {
						iter = m_state.queue.begin();
						while (iter != m_state.queue.end()) {
							request = (*iter);
							m_state.queue.erase(iter);
							break;
						}
					}

					if (request == 0) {
						if (m_state.shutdown) {
							m_state.threads_available--;
							return;
						}
						m_state.cond.wait(lock);
						if (m_state.shutdown) {
							m_state.threads_available--;
							return;
						}
					}

					m_state.threads_available--;
				}

				if (request) {
					request->Work();
					LocalRequest::Remove(request);
				}
			}

			HT_INFO("thread exit");
		}

	private:

		/// Shared application queue state object
		LocalQueueState &m_state;
	};

public:


	std::vector<Thread::id> get_thread_ids() const {
		return m_thread_ids;
	}

	void shutdown() {
		m_state.shutdown = true;
		m_state.cond.notify_all();
	}

	bool wait_for_idle( std::chrono::time_point<std::chrono::steady_clock> deadline,
			int reserve_threads = 0)
	{
		std::unique_lock<std::mutex> lock(m_state.mutex);
		return m_state.quiesce_cond.wait_until(lock, deadline,
				[this, reserve_threads]()
				{	return m_state.threads_available >= (m_state.threads_total-reserve_threads);});
	}

	void join() {
		if (!joined) {
			m_threads.join_all();
			joined = true;
		}
	}

	/** Starts application queue.
	 */
	void start() {
		std::lock_guard<std::mutex> lock(m_state.mutex);
		m_state.paused = false;
		m_state.cond.notify_all();
	}

	void stop() {
		std::lock_guard<std::mutex> lock(m_state.mutex);
		m_state.paused = true;
	}

	void add(LocalRequest *request) {

		std::lock_guard<std::mutex> lock(m_state.mutex);
		m_state.queue.push_back(request);
		m_state.cond.notify_one();
	}

	size_t backlog() {
		std::lock_guard<std::mutex> lock(m_state.mutex);
		return m_state.queue.size();
	}

protected:
	/// Application queue state object
	LocalQueueState m_state;

	/// Boost thread group for managing threads
	ThreadGroup m_threads;

	/// Vector of thread IDs
	std::vector<Thread::id> m_thread_ids;

	/// Flag indicating if threads have joined after a shutdown
	bool joined;
};

/// Shared smart pointer to LocalQueue object
typedef std::shared_ptr<LocalQueue> LocalQueuePtr;
}
}
}

#endif // Hypertable_Local_Queue_h
