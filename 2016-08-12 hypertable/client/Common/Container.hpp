
#pragma once

#include <map>
#include <set>
#include <string>

namespace common {

/** loop container */
#define LOOP_FOR_EACH(iterator, container)          \
    for(container.init(), iterator = container.next(); iterator != NULL; iterator = container.next())

#define	LOOP_KEY_FOR_EACH(key, value, container)	\
	for(container.init(), value = container.next(&key); value != NULL; value = container.next(&key))

#define	LOOP_PTR_FOR_EACH(Type, value, container)	\
	Type* value = NULL;  Type** ptr = NULL;			\
	for(container.init(), ptr = container.next(); (ptr != NULL && (value = *ptr)); ptr = container.next())


/**
 * @brief type set
 */
template<class Type, class Comp = std::less<Type> >
class TypeSet
{
public:
	TypeSet() {}
	virtual ~TypeSet() {}

public:
	/**
	 * get set size
	 **/
	size_t	size() { return m_local.size(); }

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
	Type*	add(Type* v){
		return m_local.find(*v) != m_local.end() ? NULL :
			const_cast<Type*>(&(*m_local.insert(*v).first));
	}

	/**
	 * add new type
	 **/
	Type*	add(Type& v){
		return m_local.find(v) != m_local.end() ? NULL :
			const_cast<Type*>(&(*m_local.insert(v).first));
	}

	/**
	 * del type
	 **/
	bool	del(Type* v) { return m_local.erase(*v) != 0; }

	/**
	 * del type
	 **/
	bool	del(Type& v) { return m_local.erase(v) != 0; }

	/**
	 * get type
	 **/
	Type*	get(Type* v){
		curr_iter it = m_local.find(*v);
		return it == m_local.end() ? NULL : const_cast<Type*>(&(*it));
	}

	/**
	 * get type
	 **/
	Type*	get(Type& v){
		curr_iter it = m_local.find(v);
		return it == m_local.end() ? NULL : const_cast<Type*>(&(*it));
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
		return m_iter == m_local.end() ? NULL :
			const_cast<Type*>(&(*m_iter++));
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
	typedef typename std::set<Type, Comp> curr_type;
	typedef typename curr_type::iterator  curr_iter;

	/** set container */
	curr_type	m_local;
	/** set iterator */
	curr_iter	m_iter;
};

/**
 * type map
 **/
template<class Key, class Type,	class Comp = std::less<Key> >
class TypeMap
{
public:
	/**
	 * get map size
	 **/
    size_t	size() { return m_local.size(); }

    /**
     * @brief clear map
     */
    void	clear() { m_local.clear(); }

    /**
     * add new pair
     *
     * @param key key value
     * @param type type ptr
     * @return new add type
     **/
    Type*	add(const Key& key, Type* type = NULL) {
        std::pair<curr_iter,bool> ret = m_local.insert(curr_value(key, type == NULL ? Type() : *type));
        return ret.second  ? &(*ret.first).second : NULL;
    }

    /**
     * add new pair
     *
     * @param key key value
     * @param type type ptr
     * @return new add type
     **/
    Type*	add(const Key& key, Type& type) {
        std::pair<curr_iter,bool> ret = m_local.insert(curr_value(key, type));
        return ret.second  ? &(*ret.first).second : NULL;
    }

    /**
     * del key entry
     *
     * @param key key value
     **/
    bool	del(const Key& key) { return m_local.erase(key) != 0; }

    /** get type
     *
     * @param key type key
     **/
    Type*	get(const Key& key) {
        curr_iter it = m_local.find(key);
        return it == m_local.end() ? NULL : &(*it).second;
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
    Type*	next(Key* key = NULL) {
        if (m_iter == m_local.end()) return NULL;
        if (key) *key = (*m_iter).first;
        return &(*m_iter++).second;
    }

    /**
     * min value
     **/
	Type*	minv(Key* key = NULL) {
		if (m_local.empty()) return NULL;
		if (key) *key = (*m_local.begin()).first;
		return &(*m_local.begin()).second;
	}

	/**
	 * max value
	 **/
    Type*	maxv(Key* key = NULL) {
        if (m_local.empty()) return NULL;
        if (key) *key = (*m_local.rbegin()).first;
        return &(*m_local.rbegin()).second;
    }

protected:
    typedef typename std::map<Key,Type, Comp> curr_type;
    typedef typename curr_type::iterator	curr_iter;
    typedef typename curr_type::value_type 	curr_value;

    /** map container */
    curr_type	m_local;
    /**  traverse iterator */
    curr_iter	m_iter;
};

}

using common::TypeSet;
using common::TypeMap;

/* vim: set ts=4 sw=4 expandtab */
