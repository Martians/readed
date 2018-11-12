
#pragma once

#include <vector>
#include <deque>

namespace common {

/**
 * @brief type vector
 */
template<class Type>
class TypeVector
{
public:
	TypeVector() {}
	virtual ~TypeVector() {}

public:
	/**
	 * get set count
	 **/
	size_t	count() { return m_local.size(); }

	/**
	 * @brief get queue count no lock
	 */
	bool	empty() { return m_local.empty(); }

	/**
	 * clear map
	 **/
	void	clear() { m_local.clear(); }

	/**
	 * resize map
	 **/
	void	resize(unsigned int size = 0) { m_local.resize(size); }

	/**
	 * add new type
	 **/
	void	add(Type* v){
		m_local.push_back(*v);
	}

	/**
	 * add new type
	 **/
	void	add(Type& v){
		m_local.push_back(v);
	}

	/**
	 * try to traverse container
	 *
	 * @return false if no element
	 **/
	bool	init() {
		if (m_local.empty()) {
			m_iter = m_local.end();
			return false;
		}
		m_iter = m_local.begin();
		return true;
	}

	/**
	 * traverse next type
	 *
	 * @param key [out] the key correspond to the type
	 * @return type ptr
	 **/
	Type*	next(){
		return m_iter == m_local.end() ? NULL : &(*m_iter++);
	}

	/**
	 * min value
	 **/
    Type*	minv() {
        if (m_local.empty()) return NULL;
        return const_cast<Type*>(&(*m_local.begin()));
    }

    /**
     * max value
     **/
    Type*	maxv() {
        if (m_local.empty()) return NULL;
        return const_cast<Type*>(&(*m_local.rbegin()));
    }
protected:
	typedef typename std::vector<Type> 	 curr_type;
	typedef typename curr_type::iterator curr_iter;

	/** set container */
	curr_type	m_local;
	/** set iterator */
	curr_iter	m_iter;
};

/**
 * @brief pt type ptr queue
 * @note use condition
 */
template<class Type>
class TypeQueue
{
public:
	/**
	 * @brief get queue count
	 */
	size_t	count() { return m_local.size(); }

	/**
	 * @brief get queue count no lock
	 */
	bool	empty() { return m_local.empty(); }

	/**
	 * @brief clear queue
	 */
	void    clear(bool check = false) {
		if(check && !m_local.empty()) return;
		m_local.clear();
	}

	/**
	 * @brief resize queue
	 */
	void	resize(uint32_t size = 0) { m_local.resize(size); }

	/**
	 * @brief front type
	 */
	Type	front() { return m_local.empty() ? NULL : m_local.front(); }

	/**
	 * @brief back type
	 */
	Type	back() { return m_local.empty() ? NULL : m_local.back(); }

	/**
	 * @brief push type back
	 */
	void	enque(Type* type) { m_local.push_back(*type); }

	/**
	 * @brief push type back
	 */
	void	enque(Type& type) { m_local.push_back(type); }

	/**
	 * @brief pop type from
	 */
	Type	deque() {
		if (m_local.empty()) return Type();
		Type type = m_local.front();
		m_local.pop_front();
		return type;
	}

	/**
	 * @brief pop type from
	 */
	void	deque(Type* type) {
		if (!m_local.empty()) {
			if (type) {
				*type = m_local.front();
			}
			m_local.pop_front();
		}
	}

	/**
	 * @brief enque to front
	 */
	void	enque_front(Type* type) { m_local.push_front(*type); }

	/**
	 * @brief enque to front
	 */
	void	enque_front(Type& type) { m_local.push_front(type); }

	/**
	 * @brief deque back
	 */
	Type	deque_back() {
		if (m_local.empty()) return Type();
		Type type = m_local.back();
		m_local.pop_back();
		return type;
	}

	/**
	 * @brief deque back
	 */
	void	deque_back(Type* type) {
		if (!m_local.empty()) {
			if (type) {
				*type = m_local.back();
			}
			m_local.pop_back();
		}
	}

	/**
	 * @brief try to traverse container
	 * @return false if no element
	 */
	bool	init() {
		if( m_local.empty() ){
			m_iter = m_local.end();
			return false;
		}
		m_iter = m_local.begin();
		return true;
	}

	/**
	 * @brief traverse next type
	 */
	Type*	next() { return m_iter == m_local.end() ? NULL : &*m_iter++; }

public:
	/**
	 * @brief append another deq into local and erase old one
	 * @param deq src deq
	 * @param tail append to tail or head
	 */
	void	equeue(TypeQueue<Type>& queue, bool tail = true){
		m_local.insert((!tail ? m_local.begin() : m_local.end()),
			queue.m_local.begin(), queue.m_local.end());
		queue.m_local.resize(0);
	}

	/**
	 * @brief enque type contained in vector
	 */
	void	enque(std::vector<Type>& vec){
		for(size_t i = 0; i < vec.size(); i++){
			m_local.push_back(vec[i]);
		}
		vec.clear();
	}

	/**
	 * @brief deque type into vector
	 * @param count deque count
	 */
	bool	deque(std::vector<Type*>& vec, uint32_t count = -1){
		uint32_t limit = 0;
		while( limit++ <= count ){
			if( m_local.empty() ) break;
			vec.push_back(m_local.front());
			m_local.pop_front();
		}
		return !vec.empty();
	}

	/**
	 * @brief insert type to queue
	 * @param type type ptr
	 * @note check src existence first
	 */
	bool   insert(Type* type){
		curr_iter it = std::find(m_local.begin(), m_local.end(), type);
		if( it != m_local.end() ) return false;
		m_local.push_back(type);
		return true;
	}

	/**
	 * @brief erase type from queue
	 */
	Type*   find(Type* type){
		curr_iter it = std::find(m_local.begin(), m_local.end(), type);
		return it == m_local.end() ? NULL : *it;
	}

	/**
	 * @brief erase type from queue
	 */
	bool    erase(Type* type){
		curr_iter it = std::find(m_local.begin(), m_local.end(), type);
		if( it == m_local.end() ) return false;
		m_local.erase(it);
		return true;
	}

protected:
	typedef typename std::deque<Type>	 curr_type;
	typedef typename curr_type::iterator curr_iter;
	/** queue container */
	curr_type	m_local;
	/** traverse iterator */
	curr_iter	m_iter;
};

}

using common::TypeVector;
using common::TypeQueue;
