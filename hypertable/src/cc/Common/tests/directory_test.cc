/*
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

#include <Common/Compat.h>
#include <Common/directory.h>
#include <Common/Logger.h>
#include <Common/PageArenaAllocator.h>

#include <cstdlib>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <stack>
#include <string>
#include <vector>

using namespace Hypertable;
using namespace std;

ofstream out;

template <typename K, typename V, class C = std::less<K>>
void display_directory(typename directory<K,V,C>::value_type &entry) {
  static vector<K> comps;  
  if (entry.level == 0)
    comps.clear();
  else {
    if (comps.size() >= entry.level)
      comps.resize(entry.level-1);
    comps.push_back(entry.key);
  }
  stringstream ss;
  for (const auto &key : comps)
    ss << "/" << key;
  if (entry.isset_value)
    ss << "=" << entry.value;
  else {
    if (ss.str().length() > 1)
      ss << "/";
  }
  if (entry.level == 0)
    ss << "/";
  out << ss.str() << endl;
}


template <class D>
void load_directory(D &dir) {
  vector<string> path;
  path.push_back("a");
  path.push_back("b");
  path.push_back("c");
  path.push_back("1");
  path.push_back("a");
  dir.insert(path, "foo");
  path.pop_back();
  path.push_back("b");
  dir.insert(path, "bar");
  path.pop_back();
  path.pop_back();
  path.push_back("2");
  path.push_back("a");
  dir.insert(path, "how");
  path.pop_back();
  path.push_back("b");
  dir.insert(path, "now");
  path.pop_back();
  path.pop_back();
  path.push_back("3");
  path.push_back("a");
  dir.insert(path, "brown");
  path.pop_back();
  path.push_back("b");
  dir.insert(path, "cow");
}


void test_default_constructor() {
  out << "[default_constructor]" << endl;
  directory<string, string> dir;
  for_each(dir.begin(), dir.end(), display_directory<string, string>);
  HT_ASSERT(dir.empty());
}

void test_copy_constructor() {
  out << "[copy_constructor]" << endl;
  directory<string, string> dir;
  load_directory(dir);
  directory<string, string> dir2(dir);
  for_each(dir2.begin(), dir2.end(), display_directory<string, string>);
  out << "dir is " << (dir.empty() ? "empty" : "not empty") << endl;
  out << "dir2 is " << (dir2.empty() ? "empty" : "not empty") << endl;
  HT_ASSERT(dir.size() == dir2.size());
  HT_ASSERT(dir.max_size() == dir2.max_size());
}

void test_move_constructor() {
  out << "[move_constructor]" << endl;
  directory<string, string> dir;
  load_directory(dir);
  directory<string, string> dir2(std::move(dir));
  for_each(dir2.begin(), dir2.end(), display_directory<string, string>);
  out << "dir is " << (dir.empty() ? "empty" : "not empty") << endl;
  out << "dir2 is " << (dir2.empty() ? "empty" : "not empty") << endl;
  out << "dir.size() == " << dir.size() << endl;
  HT_ASSERT(dir.max_size() == dir2.max_size());
}

void test_range_constructor() {
  out << "[range_constructor]" << endl;
  directory<string, string> dir;
  load_directory(dir);
  for_each(dir.begin(), dir.end(), display_directory<string, string>);

  directory<string, string> dir2(dir.begin(), dir.end());
  for_each(dir2.begin(), dir2.end(), display_directory<string, string>);

  vector<string> path;
  path.push_back("a");
  path.push_back("b");
  path.push_back("c");
  path.push_back("1");
  auto begin_iter = dir.find(path);
  path.pop_back();
  path.push_back("2");
  auto end_iter = dir.find(path);

  directory<string, string> dir3(begin_iter, end_iter);
  for_each(dir3.begin(), dir3.end(), display_directory<string, string>);

  begin_iter = dir.begin();
  end_iter = dir.begin();
  while (end_iter != dir.end()) {
    ++end_iter;
    directory<string, string> dir4(begin_iter, end_iter);
    for_each(dir4.begin(), dir4.end(), display_directory<string, string>);
  }

}

void test_reverse_iteration() {
  out << "[reverse_iteration]" << endl;
  directory<string, string> dir;
  load_directory(dir);
  for (auto riter = dir.rbegin(); riter != dir.rend(); ++riter)
    out << *riter << endl;
}

void test_reverse_insert() {
  out << "[reverse_insert]" << endl;
  vector<string> path;

  directory<string, string> dir;
  path.push_back("a");
  path.push_back("a");
  path.push_back("a");
  path.push_back("a");
  dir.insert(path, "foo");
  auto iter = dir.find(path);

  path.clear();
  directory<string, string> dir2;
  path.push_back("b");
  path.push_back("b");
  path.push_back("b");
  path.push_back("b");
  dir2.insert(path, "bar");

  dir.insert(iter, dir2.rbegin(), dir2.rend());

  for_each(dir.begin(), dir.end(), display_directory<string, string>);

}

void test_copy_assignment() {
  out << "[copy_assignment]" << endl;
  directory<string, string> dir;
  load_directory(dir);
  auto dir2 = dir;
  for_each(dir2.begin(), dir2.end(), display_directory<string, string>);
}

void test_move_assignment() {
  out << "[move_assignment]" << endl;
  directory<string, string> dir;
  load_directory(dir);
  auto dir2 = std::move(dir);
  for_each(dir2.begin(), dir2.end(), display_directory<string, string>);
}


void test_swap() {
  out << "[swap]" << endl;
  directory<string, string> dir, dir2;
  load_directory(dir);
  size_t before_size = dir.size();
  dir.swap(dir2);
  HT_ASSERT(dir.empty());
  swap(dir, dir2);
  HT_ASSERT(dir2.empty());
  HT_ASSERT(dir.size() == before_size);
}


void test_reverse_comparator() {
  out << "[reverse_comparator]" << endl;
  auto cmp = [](const string &lhs, const string &rhs) -> bool { return rhs.compare(lhs) < 0; };
  directory<string, string, decltype(cmp)> dir(cmp);
  load_directory(dir);
  for_each(dir.begin(), dir.end(), display_directory<string, string, decltype(cmp)>);
}


void test_int_keys() {
  out << "[int_keys]" << endl;
  directory<int, string> dir;

  vector<int> path;
  path.push_back(1);
  path.push_back(2);
  path.push_back(3);
  path.push_back(4);
  path.push_back(5);
  dir.insert(path, "foo");
  path.pop_back();
  path.push_back(6);
  dir.insert(path, "bar");
  path.pop_back();
  path.pop_back();
  path.push_back(16);
  path.push_back(38);
  dir.insert(path, "how");
  path.pop_back();
  path.push_back(44);
  dir.insert(path, "now");
  path.pop_back();
  path.pop_back();
  path.push_back(81);
  path.push_back(27);
  dir.insert(path, "brown");
  path.pop_back();
  path.push_back(29);
  dir.insert(path, "cow");
  for_each(dir.begin(), dir.end(), display_directory<int, string>);
}

void test_equals() {
  out << "[equals]" << endl;
  directory<string, string> dir, dir2;
  load_directory(dir);
  load_directory(dir2);
  HT_ASSERT(dir == dir2);
  vector<string> path;
  path.push_back("a");
  path.push_back("b");
  path.push_back("c");
  path.push_back("4");
  path.push_back("a");
  dir.insert(path, "Hello");
  HT_ASSERT(dir != dir2);
  dir2.insert(path, "World");
  HT_ASSERT(dir != dir2);
}

void test_relational() {
  out << "[relational]" << endl;
  vector<string> path;
  {
    directory<string, string> dir, dir2;
    path.push_back("a");
    dir.insert(path, "foo");
    path.clear();
    path.push_back("b");
    dir2.insert(path, "foo");
    HT_ASSERT(dir < dir2);
  }
  {
    directory<string, string> dir, dir2;
    path.clear();
    path.push_back("a");
    dir.insert(path, "bar");
    path.clear();
    path.push_back("a");
    dir2.insert(path, "foo");
    HT_ASSERT(dir < dir2);
    HT_ASSERT(dir <= dir2);
    HT_ASSERT(dir2 > dir);
    HT_ASSERT(dir2 >= dir);
  }
  {
    directory<string, string> dir, dir2;
    path.clear();
    path.push_back("a");
    dir.insert(path, "");
    path.clear();
    path.push_back("b");
    dir.insert(path, "");
    path.push_back("a");
    path.push_back("b");
    dir2.insert(path, "");
    HT_ASSERT(dir < dir2);
    HT_ASSERT(dir <= dir2);
    HT_ASSERT(dir2 > dir);
    HT_ASSERT(dir2 >= dir);
  }
  {
    directory<string, string> dir, dir2;
    path.clear();
    path.push_back("a");
    dir.insert(path, "foo");
    dir2.insert(path, "foo");
    HT_ASSERT(!(dir < dir2));
    HT_ASSERT(dir >= dir2);
    HT_ASSERT(dir <= dir2);
  }
}


void test_alternate_allocator() {
  out << "[alternate_allocator]" << endl;
  ByteArena arena;
  PageArenaAllocator<directory_entry<string, string>, decltype(arena)> alloc(arena);
  directory<string, string, less<string>, decltype(alloc)> dir(alloc);
  load_directory(dir);
  for_each(dir.begin(), dir.end(), display_directory<string, string>);
}


int main(int argc, char **argv) {
  string golden;

  if (argc == 2) {
    if (strcmp(argv[1], "--golden"))
      golden.append(argv[1]);
  }
  else if (argc == 1) {
    cout << "usage: directory_test <golden-file>" << endl;
    cout << " or" << endl;
    cout << "usage: directory_test --golden" << endl;
    quick_exit(EXIT_SUCCESS);
  }

  out.open("directory_test.output", std::ofstream::out);

  test_default_constructor();
  test_copy_constructor();
  test_move_constructor();
  test_range_constructor();
  test_reverse_iteration();
  test_reverse_insert();
  test_copy_assignment();
  test_move_assignment();
  test_swap();
  test_reverse_comparator();
  test_int_keys();
  test_equals();
  test_relational();
  test_alternate_allocator();

  out.close();

  if (golden.empty()) {
    string cmd("cat directory_test.output");
    if (system(cmd.c_str()))
      quick_exit(EXIT_FAILURE);
  }
  else {
    string cmd("diff directory_test.output ");
    cmd.append(golden);
    if (system(cmd.c_str()))
      quick_exit(EXIT_FAILURE);
  }

  quick_exit(EXIT_SUCCESS);
    
}
