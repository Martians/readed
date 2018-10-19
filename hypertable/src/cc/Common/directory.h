/* -*- c++ -*-
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3
 * of the License.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

/// @file
/// Declarations for directory.
/// This file contains type declarations for directory, a C++ contianer class
/// for holding data in a directory hierarchy.

#ifndef Common_directory_h
#define Common_directory_h

// remove me!
#include <iostream>

#include <cassert>
#include <iomanip>
#include <iterator>
#include <memory>
#include <set>
#include <stack>
#include <type_traits>
#include <vector>

namespace Hypertable {

  namespace __detail {

    struct __any {
      __any(...);
    };

    struct __nat { };

    __nat swap(__any, __any);

    template <class _Tp>
    struct __swappable
    {
      typedef decltype(swap(std::declval<_Tp&>(), std::declval<_Tp&>())) type;
      static const bool value = !std::is_same<type, __nat>::value;
    };

  }

  template <class _Tp>
  struct __is_swappable
    : public std::integral_constant<bool, __detail::__swappable<_Tp>::value>
  {
  };

  template <bool, class _Tp>
  struct __is_nothrow_swappable_imp
    : public std::integral_constant<bool, noexcept(swap(std::declval<_Tp&>(),
                                                        std::declval<_Tp&>()))>
  {
  };

  template <class _Tp>
  struct __is_nothrow_swappable_imp<false, _Tp>
    : public std::false_type
  {
  };

  template <class _Tp>
  struct __is_nothrow_swappable
    : public __is_nothrow_swappable_imp<__is_swappable<_Tp>::value, _Tp>
  {
  };


  /// @addtogroup Common
  /// @{

  template <class _Key, class _Tp>
  class directory_entry {
  public:
    directory_entry() {}
    directory_entry(int level, _Key key) : level(level), key(key) {}
    directory_entry(int level, _Key key, _Tp value) : level(level), key(key), value(value), isset_value(true) {}
    void set_value(_Tp val) { value = val; isset_value = true; }
    friend inline std::ostream &operator<<(std::ostream &os, const directory_entry &de) {
      os << std::setw(2) << de.level << " " << de.key;
      if (de.isset_value)
        os << "=" << de.value;
      return os;
    }
    size_t level {};
    _Key key {};
    _Tp value {};
    bool isset_value {};
  };

  template <class _Key, class _Tp>
  inline bool operator== (const directory_entry<_Key, _Tp>& lhs,
                         const directory_entry<_Key, _Tp>& rhs) {
    if (lhs.level == rhs.level &&
        lhs.key == rhs.key &&
        lhs.isset_value == rhs.isset_value)
      return !lhs.isset_value || lhs.value == rhs.value;
    return false;
  }

  template <class _Key, class _Tp>
  inline bool operator!= (const directory_entry<_Key, _Tp>& lhs,
                          const directory_entry<_Key, _Tp>& rhs) {
    return !(lhs == rhs);
  }

  template <class _Key, class _Tp>
  inline bool operator< (const directory_entry<_Key, _Tp>& lhs,
                         const directory_entry<_Key, _Tp>& rhs) {
    if (lhs.level != rhs.level)
      return lhs.level < rhs.level;
    if (lhs.key != rhs.key)
      return lhs.key < rhs.key;
    if (lhs.isset_value != rhs.isset_value)
      return !lhs.isset_value;
    if (lhs.isset_value)
      return lhs.value < rhs.value;
    return false;
  }

  template <class _Value, class _Compare, class _Allocator>
  class __directory_node_internal {
  public:

    typedef __directory_node_internal *pointer;
    typedef std::allocator_traits<_Allocator> __alloc_traits;

    typedef typename __alloc_traits::template rebind_alloc<__directory_node_internal> __node_allocator;
    typedef std::allocator_traits<__node_allocator> __node_traits;


    class pointer_compare
      : public std::binary_function<pointer, pointer, bool> {
    public:
      pointer_compare() : comp() {}
      pointer_compare(_Compare c) : comp(c) {}
      bool operator()(const pointer __x, const pointer __y) const
      {return comp(__x->entry, __y->entry);}
    protected:
      _Compare comp;
    };

    using children_set = std::set<pointer, pointer_compare, _Allocator>;

    __directory_node_internal() {}
    __directory_node_internal(_Compare &comp, _Allocator &alloc) 
      : m_value_allocator(alloc), children(pointer_compare(comp), alloc) {}
    __directory_node_internal(const __directory_node_internal &other)
      :  m_value_allocator(__alloc_traits::select_on_container_copy_construction(other.m_value_allocator)),
         entry(other.entry), children(other.children.key_comp(), other.m_value_allocator) {}
    __directory_node_internal(_Value value, _Compare &comp, _Allocator &alloc)
      : m_value_allocator(alloc), entry(value), children(pointer_compare(comp), alloc) {}

    ~__directory_node_internal() {
      for (auto &np : children)
        __destroy_node(np);
    }

    std::pair<typename children_set::iterator, bool> insert_child(pointer child) {
      child->parent = this;
      return children.insert(child);
    }

    size_t size() const noexcept {
      size_t sz {1};
      for (const auto &child : children)
        sz += child->size();
      return sz;
    }

  private:

    void __destroy_node(pointer np) {
      __node_allocator __na(m_value_allocator);
      __node_traits::destroy(__na, np);
      __node_traits::deallocate(__na, np, 1);
    }

    _Allocator m_value_allocator;

  public:
    pointer parent {};
    _Value entry;
    children_set children;
  };

  template <class _Tp, class _NodePtr, class _DiffType>
  class __directory_iterator {

    typedef _NodePtr __node_pointer;
    typedef typename std::pointer_traits<_NodePtr>::element_type __node;

  public:
    typedef std::bidirectional_iterator_tag iterator_category;
    typedef _Tp value_type;
    typedef _DiffType difference_type;
    typedef value_type& reference;
    typedef typename std::pointer_traits<__node_pointer>::template rebind<value_type> pointer;

    __directory_iterator() noexcept { }

    __directory_iterator(__node_pointer root) noexcept : __root_(root) { }

    __directory_iterator(__node_pointer root, __node_pointer np) : __root_(root) {
      if (np)
        __iters_.push_back(__subdirectory_iterator(np, np->children.begin()));
    }

    reference operator*() const {return __iters_.back().np->entry;}

    pointer operator->() const {
      return std::pointer_traits<pointer>::pointer_to(__iters_.back().np->entry);
    }

    __directory_iterator& operator++() {
      if (__iters_.back().iter != __iters_.back().np->children.end()) {
        __node_pointer np = *__iters_.back().iter;
        __iters_.push_back(__subdirectory_iterator(np, np->children.begin()));
      }
      else {
        while (!__iters_.empty()) {
          if (__iters_.back().iter == __iters_.back().np->children.end())
            __iters_.pop_back();
          else {
            ++__iters_.back().iter;
            if (__iters_.back().iter == __iters_.back().np->children.end())
              continue;
            __node_pointer np = *__iters_.back().iter;          
            __iters_.push_back(__subdirectory_iterator(np, np->children.begin()));
            break;
          }
        }
      }
      return *this;
    }

    __directory_iterator operator++(int) {
      __directory_iterator __before(*this); ++(*this); return __before;
    }

    __directory_iterator& operator--() {
      if (__iters_.empty())
        __iters_.push_back(__subdirectory_iterator(__root_, __root_->children.end()));
      else if (__iters_.back().iter == __iters_.back().np->children.begin())
        __iters_.pop_back();

      if (__iters_.back().iter != __iters_.back().np->children.begin()) {
        __node_pointer np;
        do {
          --__iters_.back().iter;
          np = *__iters_.back().iter;
          __iters_.push_back(__subdirectory_iterator(np, np->children.end()));
        } while (!np->children.empty());
      }
      return *this;
    }

    __directory_iterator& operator--(int) {
      __directory_iterator __before(*this); --(*this); return __before;
    }

    friend bool operator==(const __directory_iterator& __x, const __directory_iterator& __y) {
      return __x.__iters_ == __y.__iters_;
    }

    friend bool operator!=(const __directory_iterator& __x, const __directory_iterator& __y) {
      return !(__x == __y);
    }

    void push(__node_pointer np,
              typename __node::children_set::iterator iter) {
      __iters_.push_back(__subdirectory_iterator(np, iter));
    }

    __node_pointer node() {
      return __iters_.back().np;
    }

    operator bool() const {
      return !__iters_.empty();
    }

    friend inline std::ostream &operator<<(std::ostream &os, const __directory_iterator &di) {
      os << "[directory_iterator]" << std::endl;
      for (const auto &si : di.__iters_)
        os << si.np->entry << std::endl;
      return os;
    }

  private:

    class __subdirectory_iterator {
    public:
      __subdirectory_iterator(__node_pointer np,
                              typename __node::children_set::iterator iter)
        : np(np), iter(iter) {}
      __node_pointer np;
      typename __node::children_set::iterator iter;

      friend bool operator==(const __subdirectory_iterator& __x, const __subdirectory_iterator& __y) {
        return __x.np == __y.np && __x.iter == __y.iter;
      }

      friend bool operator!=(const __subdirectory_iterator& __x, const __subdirectory_iterator& __y) {
        return !(__x == __y);
      }

    };

    __node_pointer __root_ {};
    std::vector<__subdirectory_iterator> __iters_;
  };


  /** Directory container class.
   * @tparam _Key Key type
   * @tparam _Tp Value type
   * @tparam _Compare Key comparison type
   * @tparam _Allocator Memory allocator
   */
  template <class _Key, class _Tp, class _Compare = std::less<_Key>,
            class _Allocator = std::allocator<directory_entry<const _Key, _Tp> > >
  class directory {

  public:

    /// Key type
    typedef _Key key_type;

    /// Mapped value type
    typedef _Tp mapped_type;

    /// Value type
    typedef directory_entry<const key_type, mapped_type> value_type;

    /// %Key comparison type
    typedef _Compare key_compare;

    /// Memory allocator type
    typedef _Allocator allocator_type;

    typedef std::allocator_traits<allocator_type> __alloc_traits;

    typedef typename __alloc_traits::pointer pointer;
    typedef typename __alloc_traits::const_pointer const_pointer;
    typedef typename __alloc_traits::size_type size_type;
    typedef typename __alloc_traits::difference_type difference_type;

    /// Reference to value type
    typedef value_type& reference;

    /// Const reference to value type
    typedef const value_type& const_reference;


    /// Ordering relation on #value_type.
    class value_compare
      : public std::binary_function<value_type, value_type, bool> {
      friend class directory;
    protected:
      key_compare key_comp;

      value_compare(key_compare c) : key_comp(c) {}
    public:
      bool operator()(const value_type& __x, const value_type& __y) const
      {return key_comp(__x.key, __y.key);}
    };

    typedef __directory_node_internal<value_type, value_compare, allocator_type> __node;
    typedef typename __alloc_traits::template rebind_alloc<__node> __node_allocator;
    typedef std::allocator_traits<__node_allocator> __node_traits;
    typedef typename __node_traits::pointer __node_pointer;
    typedef typename __node_traits::pointer __node_const_pointer;

    using iterator = __directory_iterator<value_type, __node_pointer, difference_type>;
    // fix me!
    using const_iterator = __directory_iterator<value_type, __node_pointer, difference_type>;

    typedef std::reverse_iterator<iterator>       reverse_iterator;
    typedef std::reverse_iterator<const_iterator> const_reverse_iterator;

    /** Default constructor.
     */
    directory()
    noexcept(std::is_nothrow_default_constructible<allocator_type>::value &&
             std::is_nothrow_default_constructible<key_compare>::value &&
             std::is_nothrow_copy_constructible<key_compare>::value) 
      : m_value_compare(key_compare()) {
    }

    explicit directory(const key_compare& __comp)
      noexcept(std::is_nothrow_default_constructible<allocator_type>::value &&
               std::is_nothrow_copy_constructible<key_compare>::value);

    explicit directory(const allocator_type& __a);

    directory(const key_compare& __comp, const allocator_type& __a);

    template <class _InputIterator>
    directory(_InputIterator __f, _InputIterator __l,
        const key_compare& __comp = key_compare())
      : m_value_compare(__comp) {
      insert(__f, __l);
    }

    template <class _InputIterator>
    directory(_InputIterator __f, _InputIterator __l,
        const key_compare& __comp, const allocator_type& __a)
      : m_value_allocator(__a), m_value_compare(__comp) {
      insert(__f, __l);
    }

    template <class _InputIterator>
    directory(_InputIterator __f, _InputIterator __l, const allocator_type& __a)
      : directory(__f, __l, key_compare(), __a) {}

    directory(const directory &__d);

    directory(directory &&__d)
    noexcept(std::is_nothrow_move_constructible<allocator_type>::value &&
             std::is_nothrow_move_constructible<value_compare>::value);

    virtual ~directory() {
      __destroy_node(m_root);
    }

    directory& operator=(const directory& __d) {
      if (this != &__d) {
        clear();
        m_value_compare = __d.value_comp();
        __copy_assign_alloc(__d);
        insert(__d.begin(), __d.end());
      }
      return *this;
    }

    directory& operator=(directory&& __d)
    noexcept(__node_traits::propagate_on_container_move_assignment::value &&
             std::is_nothrow_move_assignable<value_compare>::value &&
             std::is_nothrow_move_assignable<allocator_type>::value) {
      
      __move_assign(__d, std::integral_constant<bool,
                    __node_traits::propagate_on_container_move_assignment::value>());
      return *this;
    }


    /**
     * Returns a copy of the key comparison object from which the container was
     * constructed.
     * @return Key comparison object
     */
    key_compare key_comp() const {
      return m_value_compare.key_comp;
    }

    /**
     * Returns an object of type #value_compare constructed from the key
     * comparison object.  This object can be used to compare two values
     * (of type #value_type) to determine if the key of the first value comes
     * before the key of the second value.
     * @return Value comparison object
     */
    value_compare value_comp() const {
      return value_compare(m_value_compare.key_comp);
    }

    iterator begin() noexcept { return iterator(m_root, m_root); }
    const_iterator begin() const noexcept { return const_iterator(m_root, m_root); }
    iterator end() noexcept { return iterator(m_root); }
    const_iterator end() const noexcept { return const_iterator(m_root); }

    reverse_iterator rbegin() noexcept {return reverse_iterator(end());}
    const_reverse_iterator rbegin() const noexcept
    {return const_reverse_iterator(end());}
    reverse_iterator rend() noexcept {return reverse_iterator(begin());}
    const_reverse_iterator rend() const noexcept
    {return const_reverse_iterator(begin());}

    bool empty() const noexcept { return m_root == nullptr; }

    size_type size() const noexcept {
      return m_root ? m_root->size() : 0;
    }

    size_type max_size() const noexcept
    {return __node_traits::max_size(m_value_allocator); }

    std::pair<iterator, bool>
    insert(const std::vector<key_type> &keys, const mapped_type &val) {
      assert(!keys.empty());
      std::pair<iterator, bool> ret;
      ret.second = false;
      if (!m_root)
        m_root = __create_node();
      __node *node = m_root;
      for (size_t i=0; i<keys.size(); ++i) {
        __node child(value_type(i+1, keys[i]), m_value_compare, m_value_allocator);
        auto child_iter = node->children.find(&child);
        if (child_iter == node->children.end()) {
          if (i == keys.size()-1) {
            ret.second = true;
            child.entry.set_value(val);
          }
          __node *new_node = __create_node(child);

          ret.first.push(node, node->insert_child(new_node).first);
          node = new_node;
        }
        else {
          ret.first.push(node, child_iter);
          node = *child_iter;
        }
      }
      return ret;
    }

    template <class InputIterator>
    void insert(const_iterator pos, InputIterator first, InputIterator last);

    template <class InputIterator>
    void insert(InputIterator first, InputIterator last);

    void clear() noexcept {
      if (m_root) {
        __destroy_node(m_root);
        m_root = nullptr;
      }
    }

    void swap(directory& __d)
      noexcept(__is_nothrow_swappable<value_compare>::value &&
               (!__node_traits::propagate_on_container_swap::value ||
                __is_nothrow_swappable<__node_allocator>::value));

    iterator find(const std::vector<key_type> &keys) {
      if (keys.empty() || !m_root)
        return end();
      iterator iter;
      auto np = m_root;
      for (auto key_iter = keys.begin(); key_iter != keys.end(); ++key_iter) {
        __node child(value_type(0, *key_iter), m_value_compare, m_value_allocator);
        auto child_iter = np->children.find(&child);
        if (child_iter == np->children.end())
          return end();
        iter.push(np, child_iter);
        np = *child_iter;
      }
      iter.push(np, np->children.begin());
      return iter;
    }

    const_iterator find(const std::vector<key_type> &keys) const {
      if (keys.empty() || !m_root)
        return end();
      const_iterator iter;
      auto np = m_root;
      for (auto key_iter = keys.begin(); key_iter != keys.end(); ++key_iter) {
        __node child(value_type(0, *key_iter), m_value_compare, m_value_allocator);
        auto child_iter = np->children.find(&child);
        if (child_iter == np->children.end())
          return end();
        iter.push(np, child_iter);
        np = *child_iter;
      }
      iter.push(np, np->children.begin());
      return iter;
    }

  private:

    typename __node::pointer release_root() {
      typename __node::pointer tmp = m_root;
      m_root = nullptr;
      return tmp;
    }

    __node_pointer __create_node();

    __node_pointer __create_node(__node &other);

    __node_pointer __create_node(value_type &value);

    template <class ..._Args>
    __node_pointer __create_node(_Args&& ...__args);

    void __destroy_node(__node *np);

    void __copy_assign_alloc(const directory& __d)
    { __copy_assign_alloc(__d, std::integral_constant<bool,
                          __node_traits::propagate_on_container_copy_assignment::value>());}
    void __copy_assign_alloc(const directory& __d, std::true_type)
    { m_value_allocator = __d.m_value_allocator; }
    void __copy_assign_alloc(const directory& __d, std::false_type) {}


    void __move_assign_alloc(directory& __d)
      noexcept(!__node_traits::propagate_on_container_move_assignment::value ||
               std::is_nothrow_move_assignable<__node_allocator>::value) {
      __move_assign_alloc(__d, std::integral_constant<bool,
                          __node_traits::propagate_on_container_move_assignment::value>());
    }

    void __move_assign_alloc(directory& __d, std::true_type)
      noexcept(std::is_nothrow_move_assignable<allocator_type>::value)
    { m_value_allocator = std::move(__d.m_value_allocator); }

    void __move_assign_alloc(directory& __d, std::false_type) noexcept {}

    void __move_assign(directory& __d, std::true_type)
      noexcept(std::is_nothrow_move_assignable<value_compare>::value &&
               std::is_nothrow_move_assignable<allocator_type>::value) {
      __destroy_node(m_root);
      m_root = __d.release_root();
      __move_assign_alloc(__d);
      m_value_compare = std::move(m_value_compare);
    }

    void __move_assign(directory& __d, std::false_type) {
      if (m_value_allocator == __d.m_value_allocator)
        __move_assign(__d, std::true_type());
      else {
        m_value_compare = std::move(m_value_compare);
        clear();
        insert(__d.begin(), __d.end());
        __d.clear();
      }
    }

    typename __node::pointer m_root {};

    allocator_type m_value_allocator;
    value_compare m_value_compare;
  };

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  directory<_Key, _Tp, _Compare, _Allocator>::directory(const key_compare& __comp)
    noexcept(std::is_nothrow_default_constructible<allocator_type>::value &&
             std::is_nothrow_copy_constructible<key_compare>::value) 
    : m_value_compare(__comp) {
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  directory<_Key, _Tp, _Compare, _Allocator>::directory(const allocator_type& __a)
    : m_value_allocator(__a), m_value_compare(key_compare()) {
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  directory<_Key, _Tp, _Compare, _Allocator>::directory(const key_compare& __comp,
                                                        const allocator_type& __a)
    : m_value_allocator(__a), m_value_compare(__comp) {
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  directory<_Key, _Tp, _Compare, _Allocator>::directory(const directory &__d)
    : m_value_allocator(__alloc_traits::select_on_container_copy_construction(__d.m_value_allocator)),
      m_value_compare(__d.value_comp()) {
    insert(__d.begin(), __d.end());
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  directory<_Key, _Tp, _Compare, _Allocator>::directory(directory &&__d)
    noexcept(std::is_nothrow_move_constructible<allocator_type>::value &&
             std::is_nothrow_move_constructible<value_compare>::value)
    : m_root(__d.release_root()),
    m_value_allocator(std::move(__d.m_value_allocator)),
    m_value_compare(std::move(__d.m_value_compare)) {
  }


  template <class _Key, class _Tp, class _Compare, class _Allocator>
  template <class _InputIterator>
  void directory<_Key, _Tp, _Compare, _Allocator>::insert(const_iterator pos, _InputIterator __f, _InputIterator __l) {
    __node_pointer parent;

    if (__f == __l)
      return;


    if (!pos) {
      if (!m_root)
        m_root = __create_node();
      parent = m_root;
    }
    else
      parent = pos.node();

    if (__f->level == 0 && parent->entry.level == 0)
      ++__f;

    size_t previous_level = __f->level;
    __node_pointer previous_node {};

    for ( ; __f != __l; ++__f) {

      if (__f->level < previous_level) {
        parent = parent->parent;
        if (parent == 0)
          return;
      }
      else if (__f->level > previous_level && previous_node)
        parent = previous_node;

      __node child(*__f, m_value_compare, m_value_allocator);
      auto child_iter = parent->children.find(&child);
      if (child_iter == parent->children.end()) {
        __node *new_node = __create_node(child);
        new_node->entry.level = parent->entry.level + 1;
        parent->insert_child(new_node);
        previous_node = new_node;
      }
      else {
        (*child_iter)->entry.value = __f->value;
        previous_node = *child_iter;
      }

      previous_level = __f->level;
    }
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  void directory<_Key, _Tp, _Compare, _Allocator>::swap(directory& __d)
    noexcept(__is_nothrow_swappable<value_compare>::value &&
             (!__node_traits::propagate_on_container_swap::value ||
              __is_nothrow_swappable<__node_allocator>::value))
  {
    using std::swap;
    swap(m_root, __d.m_root);
    swap(m_value_compare, __d.m_value_compare);
    swap(m_value_allocator, __d.m_value_allocator);
  }


  template <class _Key, class _Tp, class _Compare, class _Allocator>
  template <class _InputIterator>
  void directory<_Key, _Tp, _Compare, _Allocator>::insert(_InputIterator __f, _InputIterator __l) {
    insert(const_iterator(m_root, m_root), __f, __l);
  }

  /*
   * PRIVATE
   */

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer
  directory<_Key, _Tp, _Compare, _Allocator>::__create_node() {
    __node_allocator __na(m_value_allocator);
    typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer node
      = __node_traits::allocate(__na, 1);
    __node_traits::construct(__na, node, m_value_compare, m_value_allocator);
    return node;
  }


  template <class _Key, class _Tp, class _Compare, class _Allocator>
  typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer
  directory<_Key, _Tp, _Compare, _Allocator>::__create_node(typename directory<_Key, _Tp, _Compare, _Allocator>::__node &other) {
    __node_allocator __na(m_value_allocator);
    typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer node
      = __node_traits::allocate(__na, 1);
    __node_traits::construct(__na, node, other);
    return node;
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer
  directory<_Key, _Tp, _Compare, _Allocator>::__create_node(typename directory<_Key, _Tp, _Compare, _Allocator>::value_type &value) {
    __node_allocator __na(m_value_allocator);
    typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer node
      = __node_traits::allocate(__na, 1);
    __node_traits::construct(__na, node, value, m_value_compare, m_value_allocator);
    return node;
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  template <class ..._Args>
  typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer
  directory<_Key, _Tp, _Compare, _Allocator>::__create_node(_Args&& ...__args) {
    __node_allocator __na(m_value_allocator);
    typename directory<_Key, _Tp, _Compare, _Allocator>::__node_pointer node
      = __node_traits::allocate(__na, 1);
    __node_traits::construct(__na, node, std::forward<_Args>(__args)...);
    return node;
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  void directory<_Key, _Tp, _Compare, _Allocator>::__destroy_node(__node *np) {
    if (np) {
      __node_allocator __na(m_value_allocator);
      __node_traits::destroy(__na, np);
      __node_traits::deallocate(__na, np, 1);
    }
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  bool operator==(const directory<_Key, _Tp, _Compare, _Allocator>& __x,
                  const directory<_Key, _Tp, _Compare, _Allocator>& __y)
  {
    return __x.size() == __y.size() && std::equal(__x.begin(), __x.end(), __y.begin());
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  bool operator!=(const directory<_Key, _Tp, _Compare, _Allocator>& __x,
                  const directory<_Key, _Tp, _Compare, _Allocator>& __y)
  {
    return !(__x == __y);
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  bool
  operator< (const directory<_Key, _Tp, _Compare, _Allocator>& __x,
             const directory<_Key, _Tp, _Compare, _Allocator>& __y)
  {
    auto iter_x = __x.begin();
    auto iter_y = __y.begin();
    while (iter_x != __x.end() && iter_y != __y.end()) {
      if (*iter_x != *iter_y)
        return *iter_x < *iter_y;
      ++iter_x;
      ++iter_y;
    }
    if (iter_x == __x.end())
      return iter_y != __y.end();
    return false;
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  bool operator> (const directory<_Key, _Tp, _Compare, _Allocator>& __x,
                  const directory<_Key, _Tp, _Compare, _Allocator>& __y)
  {
    return __y < __x;
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  bool operator>=(const directory<_Key, _Tp, _Compare, _Allocator>& __x,
                  const directory<_Key, _Tp, _Compare, _Allocator>& __y)
  {
    return !(__x < __y);
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  bool operator<=(const directory<_Key, _Tp, _Compare, _Allocator>& __x,
                  const directory<_Key, _Tp, _Compare, _Allocator>& __y)
  {
    return !(__y < __x);
  }

  template <class _Key, class _Tp, class _Compare, class _Allocator>
  inline
  void swap(directory<_Key, _Tp, _Compare, _Allocator>& __x,
            directory<_Key, _Tp, _Compare, _Allocator>& __y)
    noexcept(noexcept(__x.swap(__y)))
  {
    __x.swap(__y);
  }

  /// @}

}

#endif // Common_directory_h
