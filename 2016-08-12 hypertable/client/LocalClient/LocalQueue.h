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

#include <Common/StaticBuffer.h>
#include "Common/Debug.hpp"

namespace Hypertable
{
namespace FsBroker
{
namespace Lib
{
class LocalClient;
class LocalQueue
{
public:
	/** Tracks group execution state.
	 * A GroupState object is created for each unique group ID to track the
	 * queue execution state of requests in the group.
	 */
	class GroupState
	{
	public:
		GroupState()
				: group_id(0), running(false), outstanding(1) {
			return;
		}
		uint64_t group_id;    //!< Group ID
		/** <i>true</i> if a request from this group is being executed */
		bool running;
		/** Number of outstanding (uncompleted) requests in queue for this group*/
		int outstanding;
	};

	class LocalRequest
	{
	public:
		LocalRequest(int t = LT_null, DispatchHandler *h = NULL, LocalClient* c = NULL)
			: group_state(0), fd(0), type(t), handler(h), client(c)
		{
		}

		~LocalRequest() { }

		enum {
			LT_null = 0,
			LT_read,
			LT_pread,
			LT_append,
			LT_close,
			LT_sync,
		};

	public:

		static LocalRequest* Create() { return new LocalRequest(); }

		static void Remove(LocalRequest *request) { delete request; }

		bool is_urgent() { return false; }

	public:
		void	Work();

		void	Set(int32_t _fd, uint64_t _len, uint64_t _offset = 0) {
			fd 	= _fd;
			len = _len;
			offset = _offset;
		}

		void	Set(int32_t _fd, Filesystem::Flags _flags) {
			fd 		= _fd;
			flags 	= _flags;
		}

		void	Set(int32_t _fd) {
			fd		= _fd;
		}

	public:
		GroupState *group_state; //!< Pointer to GroupState to which request belongs
		int		fd;
		int		type;
		DispatchHandler* handler;
		LocalClient* client;
		std::string	name;

		Filesystem::Flags flags;
		StaticBuffer buffer;
		uint64_t len;
		uint64_t offset;
	};

	/** Hash map of thread group ID to GroupState
	 */
	typedef std::unordered_map<uint64_t, GroupState *> GroupStateMap;

	/** Individual request queue
	 */
	typedef std::list<LocalRequest *> RequestQueue;

	/** Application queue state shared among worker threads.
	 */
	class LocalQueueState
	{
	public:
		LocalQueueState()
				: threads_available(0), shutdown(false),
						paused(false) {
		}

		/// Normal request queue
		RequestQueue queue;

		/// Urgent request queue
		RequestQueue urgent_queue;

		/// Group ID to group state map
		GroupStateMap group_state_map;

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
		Worker(LocalQueueState &qstate, bool one_shot = false)
				: m_state(qstate), m_one_shot(one_shot) {
			return;
		}

		/** Thread run method
		 */
		void operator()() {
			LocalRequest *request = 0;
			RequestQueue::iterator iter;

			while (true) {
				{
					std::unique_lock<std::mutex> lock(m_state.mutex);

					m_state.threads_available++;
					while ((m_state.paused || m_state.queue.empty()) &&
							m_state.urgent_queue.empty()) {
						if (m_state.shutdown) {
							m_state.threads_available--;
							return;
						}
						if (m_state.threads_available == m_state.threads_total)
							m_state.quiesce_cond.notify_all();
						m_state.cond.wait(lock);
					}

					if (m_state.shutdown) {
						m_state.threads_available--;
						return;
					}

					request = 0;

					iter = m_state.urgent_queue.begin();
					while (iter != m_state.urgent_queue.end()) {
						request = (*iter);
						if (request->group_state == 0
								|| !request->group_state->running) {
							if (request->group_state)
								request->group_state->running = true;
							m_state.urgent_queue.erase(iter);
							break;
						}
						request = 0;
						iter++;
					}

					if (request == 0 && !m_state.paused) {
						iter = m_state.queue.begin();
						while (iter != m_state.queue.end()) {
							request = (*iter);
							if (request->group_state == 0
									|| !request->group_state->running) {
								if (request->group_state)
									request->group_state->running = true;
								m_state.queue.erase(iter);
								break;
							}
							request = 0;
							iter++;
						}
					}

					if (request == 0 && !m_one_shot) {
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
					remove(request);
					if (m_one_shot)
						return;
				}
				else if (m_one_shot)
					return;
			}

			HT_INFO("thread exit");
		}

	private:

		/** Removes and deletes a request.  This method updates the group
		 * state associated with <code>request</code> by setting the running flag to
		 * <i>false</i> and decrementing the outstanding count.  If the
		 * outstanding count for the group drops to 0, the group state requestord
		 * is removed from LocalQueueState::group_state_map and is deleted.
		 * @param request Request requestord to remove
		 */
		void remove(LocalRequest *request) {
			if (request->group_state) {
				std::lock_guard<std::mutex> ulock(m_state.mutex);
				request->group_state->running = false;
				request->group_state->outstanding--;
				if (request->group_state->outstanding == 0) {
					m_state.group_state_map.erase(request->group_state->group_id);
					delete request->group_state;
				}
			}
			delete request;
		}

		/** Removes and deletes an expired request.  This method updates the group
		 * state associated with <code>request</code> by decrementing the outstanding
		 * count.  If the outstanding count for the group drops to 0, the group
		 * state requestord is removed from LocalQueueState::group_state_map and
		 * is deleted.
		 * @param request Request requestord to remove
		 */
		void remove_expired(LocalRequest *request) {
			if (request->group_state) {
				request->group_state->outstanding--;
				if (request->group_state->outstanding == 0) {
					m_state.group_state_map.erase(request->group_state->group_id);
					delete request->group_state;
				}
			}
			delete request;
		}

		/// Shared application queue state object
		LocalQueueState &m_state;

		/// Set to <i>true</i> if thread should exit after executing request
		bool m_one_shot;
	};

	/// Application queue state object
	LocalQueueState m_state;

	/// Boost thread group for managing threads
	ThreadGroup m_threads;

	/// Vector of thread IDs
	std::vector<Thread::id> m_thread_ids;

	/// Flag indicating if threads have joined after a shutdown
	bool joined;

	/** Set to <i>true</i> if queue is configured to allow dynamic thread
	 * creation
	 */
	bool m_dynamic_threads;

public:

	/** Default constructor used by derived classes only
	 */
	LocalQueue()
			: joined(true) {
	}

	/**
	 * Constructor initialized with worker thread count.
	 * This constructor sets up the application queue with a number of worker
	 * threads specified by <code>worker_count</code>.
	 * @param worker_count Number of worker threads to create
	 * @param dynamic_threads Dynamically create temporary thread to carry out
	 * requests if none available.
	 */
	LocalQueue(int worker_count, bool dynamic_threads = false)
			: joined(false), m_dynamic_threads(dynamic_threads) {
		m_state.threads_total = worker_count;
		Worker Worker(m_state);
	//	assert(worker_count > 0);
		for (int i = 0; i < worker_count; ++i) {
			m_thread_ids.push_back(m_threads.create_thread(Worker)->get_id());
		}
		//threads
	}

	/** Destructor.
	 */
	virtual ~LocalQueue() {
		if (!joined) {
			shutdown();
			join();
		}
	}

	/**
	 * Returns all the thread IDs for this threadgroup
	 * @return vector of Thread::id
	 */
	std::vector<Thread::id> get_thread_ids() const {
		return m_thread_ids;
	}

	/**
	 * Shuts down the application queue.  All outstanding requests are carried
	 * out and then all threads exit.  #join can be called to wait for
	 * completion of the shutdown.
	 */
	void shutdown() {
		m_state.shutdown = true;
		m_state.cond.notify_all();
	}

	/** Wait for queue to become idle (with timeout).
	 * @param deadline Return by this time if queue does not become idle
	 * @param reserve_threads Number of threads that can be active when queue is
	 * idle
	 * @return <i>false</i> if <code>deadline</code> was reached before queue
	 * became idle, <i>true</i> otherwise
	 */
	bool wait_for_idle(
			std::chrono::time_point<std::chrono::steady_clock> deadline,
			int reserve_threads = 0) {
		std::unique_lock<std::mutex> lock(m_state.mutex);
		return m_state.quiesce_cond.wait_until(lock, deadline,
				[this, reserve_threads]() {return m_state.threads_available >= (m_state.threads_total-reserve_threads);});
	}

	/**
	 * Waits for a shutdown to complete.  This method returns when all
	 * application queue threads exit.
	 */
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

	/** Stops (pauses) application queue, preventing non-urgent requests from
	 * being executed.  Any requests that are being executed at the time of the
	 * call are allowed to complete.
	 */
	void stop() {
		std::lock_guard<std::mutex> lock(m_state.mutex);
		m_state.paused = true;
	}

	/** Adds a request (application request handler) to the application queue.
	 * The request queue is designed to support the serialization of related
	 * requests.  Requests are related by the thread group ID value in the
	 * LocalRequest.  This thread group ID is constructed in the
	 * Event object.
	 * @param request Pointer to request to add
	 */
	virtual void add(LocalRequest *request) {
		GroupStateMap::iterator uiter;
		uint64_t group_id = request->fd;
		request->group_state = 0;

		HT_ASSERT(request);

		if (group_id != 0) {
			std::lock_guard<std::mutex> ulock(m_state.mutex);
			if ((uiter = m_state.group_state_map.find(group_id))
					!= m_state.group_state_map.end()) {
				request->group_state = (*uiter).second;
				request->group_state->outstanding++;
			}
			else {
				request->group_state = new GroupState();
				request->group_state->group_id = group_id;
				m_state.group_state_map[group_id] = request->group_state;
			}
		}

		{
			std::lock_guard<std::mutex> lock(m_state.mutex);
			if (request->is_urgent()) {
				m_state.urgent_queue.push_back(request);
				if (m_dynamic_threads && m_state.threads_available == 0) {
					Worker worker(m_state, true);
					Thread t(worker);
				}
			}
			else
			m_state.queue.push_back(request);
			m_state.cond.notify_one();
		}
	}

	/** Adds a request (application request handler) to the application queue.
	 * The request queue is designed to support the serialization of related
	 * requests.  Requests are related by the thread group ID value in the
	 * LocalRequest.  This thread group ID is constructed in the
	 * Event object.
	 * @note This method is defined for symmetry and just calls #add
	 * @param request Pointer to request to add
	 */
	virtual void add_unlocked(LocalRequest *request) {
		add(request);
	}

	/// Returns the request backlog
	/// Returns the request backlog, which is the number of requests waiting on
	/// the request queues for a thread to become available
	/// @return Request backlog
	size_t backlog() {
		std::lock_guard<std::mutex> lock(m_state.mutex);
		return m_state.queue.size() + m_state.urgent_queue.size();
	}
};

/// Shared smart pointer to LocalQueue object
typedef std::shared_ptr<LocalQueue> LocalQueuePtr;
/** @}*/
}
}
}
#endif // Hypertable_Local_Queue_h
