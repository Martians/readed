/*
 * Copyright (C) 2007-2015 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License, or any later version.
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
/// Definitions for Range.
/// This file contains the variable and method definitions for Range, a class
/// used to access and manage a range of table data.

#include <Common/Compat.h>
#include "Range.h"

#include <Hypertable/RangeServer/CellList.h>
#include <Hypertable/RangeServer/CellStoreFactory.h>
#include <Hypertable/RangeServer/Global.h>
#include <Hypertable/RangeServer/MergeScannerRange.h>
#include <Hypertable/RangeServer/MetaLogEntityTaskAcknowledgeRelinquish.h>
#include <Hypertable/RangeServer/MetadataNormal.h>
#include <Hypertable/RangeServer/MetadataRoot.h>
#include <Hypertable/RangeServer/TransferLog.h>

#include <Hypertable/Lib/CommitLog.h>
#include <Hypertable/Lib/CommitLogReader.h>
#include <Hypertable/Lib/LoadDataEscape.h>

#include <Common/Config.h>
#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/FileUtils.h>
#include <Common/Random.h>
#include <Common/ScopeGuard.h>
#include <Common/StringExt.h>
#include <Common/md5.h>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include <re2/re2.h>

#include <cassert>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include <string.h>
}

#if TESTING_SUPPORT
#include "Common/Debug.hpp"
#define RANGE_MASK m_is_user
#undef	BEG_PREFIX
#undef	END_PREFIX
#undef	TIME_PREFIX
#define BEG_PREFIX "====> range "
#define END_PREFIX "<==== range "
#define TIME_PREFIX "________range "
#define SUFFIX		", name: " << m_name
#endif

using namespace Hypertable;
using namespace std;

Range::Range(Lib::Master::ClientPtr &master_client,
             const TableIdentifier &identifier, SchemaPtr &schema,
             const RangeSpec &range, RangeSet *range_set,
             const RangeState &state, bool needs_compaction)
  : m_master_client(master_client),
    m_hints_file(identifier.id, range.start_row, range.end_row),
    m_schema(schema), m_range_set(range_set),
    m_load_metrics(identifier.id, range.start_row, range.end_row) {
  m_metalog_entity = make_shared<MetaLogEntityRange>(identifier, range, state, needs_compaction);
  initialize();
}

Range::Range(Lib::Master::ClientPtr &master_client, SchemaPtr &schema,
             MetaLogEntityRangePtr &range_entity, RangeSet *range_set)
  : m_master_client(master_client),  m_metalog_entity(range_entity), 
    m_hints_file(range_entity->get_table_id(), range_entity->get_start_row(),
                 range_entity->get_end_row()),
    m_schema(schema), m_range_set(range_set),
    m_load_metrics(range_entity->get_table_id(), range_entity->get_start_row(),
                   range_entity->get_end_row()) {
  initialize();
}

void Range::initialize() {

  AccessGroupPtr ag;
  String start_row, end_row;

  m_metalog_entity->get_boundary_rows(start_row, end_row);

  m_metalog_entity->get_table_identifier(m_table);

#if TESTING_SUPPORT
  size_t min_len = std::min(end_row.length(), (size_t)5);
  m_name = format("%s[%s]", m_table.id, end_row.substr(0, min_len).c_str());
#else
  m_name = format("%s[%s..%s]", m_table.id, start_row.c_str(), end_row.c_str());
#endif

  m_is_metadata = m_table.is_metadata();
  m_is_user   = m_table.is_user();

  m_is_root = m_is_metadata && start_row.empty() &&
    end_row.compare(Key::END_ROOT_ROW) == 0;

  memset(m_added_deletes, 0, sizeof(m_added_deletes));

  uint64_t soft_limit = m_metalog_entity->get_soft_limit();
  if (m_is_metadata) {
    if (soft_limit == 0) {
      soft_limit = Global::range_metadata_split_size;
      m_metalog_entity->set_soft_limit(soft_limit);
    }
    m_split_threshold = soft_limit;
  }
  else {
    if (soft_limit == 0 || soft_limit > (uint64_t)Global::range_split_size) {
      soft_limit = Global::range_split_size;
      m_metalog_entity->set_soft_limit(soft_limit);
    }
    {
      lock_guard<mutex> lock(Global::mutex);
      int64_t random = Random::number64();
      if (random == 0) {
    	  Random::seed((int)time(NULL));
    	  random = Random::number64();
      }

      if (range_count(ST_load, ST_normal) < 4) {
    	  soft_limit = (1024 * 1024) * 512;
      }
	#if 0
	  else if (range_count(ST_load, ST_normal) < 10) {
		  soft_limit = (1024 * 1024) * 1024 * 1;
	  }
	#endif

      m_split_threshold = soft_limit + random % soft_limit;
      //m_split_threshold = 1024 * 1024;
      //time_debug_using("range random " << random << ", threshold " << StringSize(m_split_threshold));
    }
  }

  /**
   * Determine split side
   */
  if (m_metalog_entity->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
      m_metalog_entity->get_state() == RangeState::SPLIT_SHRUNK) {
    String split_row = m_metalog_entity->get_split_row();
    String old_boundary_row = m_metalog_entity->get_old_boundary_row();
    if (split_row.compare(old_boundary_row) < 0)
      m_split_off_high = true;
  }
  else {
    String split_off = Config::get_str("Hypertable.RangeServer.Range.SplitOff");
    if (split_off == "high")
      m_split_off_high = true;
    else
      HT_ASSERT(split_off == "low");
  }

  m_column_family_vector.resize(m_schema->get_max_column_family_id() + 1);

  // If no transfer log, check to see if hints file exists and if not, write
  // one.  This is to handle the case of the missing hints file for the
  // initially loaded range in a table
  if (m_metalog_entity->get_transfer_log().empty() && !m_hints_file.exists())
    m_hints_file.write(Global::location_initializer->get());

  // Read hints file and load AGname-to-hints map
  std::map<String, const AccessGroup::Hints *> hints_map;
  m_hints_file.read();
  for (const auto &h : m_hints_file.get())
    hints_map[h.ag_name] = &h;

  RangeSpecManaged range_spec;
  m_metalog_entity->get_range_spec(range_spec);
  for (auto ag_spec : m_schema->get_access_groups()) {
    const AccessGroup::Hints *h = 0;
    std::map<String, const AccessGroup::Hints *>::iterator iter = hints_map.find(ag_spec->get_name());
    if (iter != hints_map.end())
      h = iter->second;
    ag = make_shared<AccessGroup>(&m_table, m_schema, ag_spec, &range_spec, h);
    m_access_group_map[ag_spec->get_name()] = ag;
    m_access_group_vector.push_back(ag);

    for (auto cf_spec : ag_spec->columns())
      m_column_family_vector[cf_spec->get_id()] = ag;
  }

  if (RANGE_MASK) {
	  range_inc(ST_take);
	  time_debug_using(BEG_PREFIX << "load, name " << m_name << ", threshold " << string_size(m_split_threshold) << SUFFIX);
  }
}


void Range::deferred_initialization() {
  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);

  if (m_initialized)
    return;

  	CREATE_TIMER;
	if (RANGE_MASK) {
	  range_next(ST_load);
	}

  if (m_metalog_entity->get_state() == RangeState::SPLIT_LOG_INSTALLED) {
    split_install_log_rollback_metadata();
    time_debug_mute_warn(TIME_PREFIX << "split roll back range" << SUFFIX);
  }

  load_cell_stores();

  m_initialized = true;

  if (RANGE_MASK) {
	  UPDATE_TIMER;
	  range_next(ST_normal);
	  time_debug_mute_warn(TIME_PREFIX << "initialize range" << ", using " << string_timer(timer.last()) << SUFFIX);
	  time_debug_using(DumpStat());
  }
}

void Range::deferred_initialization(uint32_t timeout_millis) {

  if (m_initialized)
    return;

  auto expiration_time = chrono::fast_clock::now() +
    chrono::milliseconds(timeout_millis);

  deferred_initialization(expiration_time);
}

void Range::deferred_initialization(chrono::fast_clock::time_point expire_time) {

  if (m_initialized)
    return;

  while (true) {
    try {
      deferred_initialization();
    }
    catch (Exception &e) {
      auto now = chrono::fast_clock::now();
      if (now < expire_time) {
        this_thread::sleep_for(chrono::milliseconds(10000));
        continue;
      }
      throw;
    }
    break;
  }
}


void Range::split_install_log_rollback_metadata() {
  String metadata_key_str, start_row, end_row;
  KeySpec key;
  TableMutatorPtr mutator(Global::metadata_table->create_mutator());

  m_metalog_entity->get_boundary_rows(start_row, end_row);

  // Reset start row
  metadata_key_str = String(m_table.id) + ":" + end_row;
  key.row = metadata_key_str.c_str();
  key.row_len = metadata_key_str.length();
  key.column_qualifier = 0;
  key.column_qualifier_len = 0;
  key.column_family = "StartRow";
  mutator->set(key, (uint8_t *)start_row.c_str(), start_row.length());

  // Get rid of new range
  metadata_key_str = String(m_table.id) + ":" + m_metalog_entity->get_split_row();
  key.flag = FLAG_DELETE_ROW;
  key.row = metadata_key_str.c_str();
  key.row_len = metadata_key_str.length();
  key.column_qualifier = 0;
  key.column_qualifier_len = 0;
  key.column_family = 0;
  mutator->set_delete(key);

  mutator->flush();
}

namespace {
  void delete_metadata_pointer(Metadata **metadata) {
    delete *metadata;
    *metadata = 0;
  }
}


void Range::load_cell_stores() {
  Metadata *metadata = 0;
  AccessGroupPtr ag;
  CellStorePtr cellstore;
  const char *base, *ptr, *end;
  std::vector<String> csvec;
  String ag_name;
  String files;
  String file_str;
  String start_row, end_row;
  uint32_t nextcsid;

  HT_INFOF("Loading cellstores for '%s'", m_name.c_str());

  HT_ON_SCOPE_EXIT(&delete_metadata_pointer, &metadata);

  m_metalog_entity->get_boundary_rows(start_row, end_row);

  if (m_is_root) {
    lock_guard<mutex> schema_lock(m_schema_mutex);
    metadata = new MetadataRoot(m_schema);
  }
  else
    metadata = new MetadataNormal(&m_table, end_row);

  metadata->reset_files_scan();

  {
    lock_guard<mutex> schema_lock(m_schema_mutex);
    for (auto ag : m_access_group_vector)
      ag->pre_load_cellstores();
  }

  while (metadata->get_next_files(ag_name, files, &nextcsid)) {
    lock_guard<mutex> schema_lock(m_schema_mutex);
    csvec.clear();

    if ((ag = m_access_group_map[ag_name]) == 0) {
      HT_ERRORF("Unrecognized access group name '%s' found in METADATA for "
                "table '%s'", ag_name.c_str(), m_table.id);
      HT_ABORT;
    }

    ag->set_next_csid(nextcsid);

    ptr = base = (const char *)files.c_str();
    end = base + strlen(base);
    while (ptr < end) {

      while (*ptr != ';' && ptr < end)
        ptr++;

      file_str = String(base, ptr-base);
      boost::trim(file_str);

      if (!file_str.empty()) {
        if (file_str[0] == '#') {
          ++ptr;
          base = ptr;
          continue;
        }

        csvec.push_back(file_str);
      }
      ++ptr;
      base = ptr;
    }

    files = "";

    String file_basename = Global::toplevel_dir + "/tables/";

    bool skip_not_found = Config::properties->get_bool("Hypertable.RangeServer.CellStore.SkipNotFound");
    bool skip_bad = Config::properties->get_bool("Hypertable.RangeServer.CellStore.SkipBad");

    for (size_t i=0; i<csvec.size(); i++) {

      files += csvec[i] + ";\n";

      HT_INFOF("Loading CellStore %s", csvec[i].c_str());

      try {
        cellstore = CellStoreFactory::open(file_basename + csvec[i],
                                           start_row.c_str(), end_row.c_str());
      }
      catch (Exception &e) {
        // issue 986: mapr returns IO_ERROR if CellStore does not exist
	if (e.code() == Error::FSBROKER_FILE_NOT_FOUND ||
	    e.code() == Error::FSBROKER_BAD_FILENAME ||
	    e.code() == Error::FSBROKER_IO_ERROR) {
	  if (skip_not_found) {
	    HT_WARNF("CellStore file '%s' not found, skipping", csvec[i].c_str());
	    continue;
	  }
        }
	if (e.code() == Error::RANGESERVER_CORRUPT_CELLSTORE) {
	  if (skip_bad) {
	    HT_WARNF("CellStore file '%s' is corrupt, skipping", csvec[i].c_str());
	    continue;
	  }
	}
        HT_FATALF("Problem opening CellStore file '%s' - %s", csvec[i].c_str(),
                  Error::get_text(e.code()));
      }

      int64_t revision = boost::any_cast<int64_t>
        (cellstore->get_trailer()->get("revision"));
      if (revision > m_latest_revision)
        m_latest_revision = revision;

      ag->load_cellstore(cellstore);
    }
  }

  {
    lock_guard<mutex> schema_lock(m_schema_mutex);
    for (auto ag : m_access_group_vector)
      ag->post_load_cellstores();
  }

  HT_INFOF("Finished loading cellstores for '%s'", m_name.c_str());
}


void Range::update_schema(SchemaPtr &schema) {
  lock_guard<mutex> lock(m_schema_mutex);

  vector<AccessGroupSpec*> new_access_groups;
  AccessGroupPtr ag;
  AccessGroupMap::iterator ag_iter;
  size_t max_column_family_id = schema->get_max_column_family_id();

  // only update schema if there is more recent version
  if(schema->get_generation() <= m_schema->get_generation())
    return;

  // resize column family vector if needed
  if (max_column_family_id > m_column_family_vector.size()-1)
    m_column_family_vector.resize(max_column_family_id+1);

  // update all existing access groups & create new ones as needed
  for (auto ag_spec : schema->get_access_groups()) {
    if( (ag_iter = m_access_group_map.find(ag_spec->get_name())) !=
        m_access_group_map.end()) {
      ag_iter->second->update_schema(schema, ag_spec);
      for (auto cf_spec : ag_spec->columns()) {
        if (!cf_spec->get_deleted())
          m_column_family_vector[cf_spec->get_id()] = ag_iter->second;
      }
    }
    else {
      new_access_groups.push_back(ag_spec);
    }
  }

  // create new access groups
  {
    lock_guard<mutex> lock(m_mutex);
    m_table.generation = schema->get_generation();
    m_metalog_entity->set_table_generation(m_table.generation);
    RangeSpecManaged range_spec;
    m_metalog_entity->get_range_spec(range_spec);
    for (auto ag_spec : new_access_groups) {
      ag = make_shared<AccessGroup>(&m_table, schema, ag_spec, &range_spec);
      m_access_group_map[ag_spec->get_name()] = ag;
      m_access_group_vector.push_back(ag);
      for (auto cf_spec : ag_spec->columns()) {
        if (!cf_spec->get_deleted())
          m_column_family_vector[cf_spec->get_id()] = ag;
      }
    }
  }

  // TODO: remove deleted access groups
  m_schema = schema;
  return;
}


/**
 * This method must not fail.  The caller assumes that it will succeed.
 */
void Range::add(const Key &key, const ByteString value) {
  HT_DEBUG_OUT <<"key="<< key <<" value='";
  const uint8_t *p;
  size_t len = value.decode_length(&p);
  _out_ << format_bytes(20, p, len) << HT_END;

  if (key.flag != FLAG_INSERT && key.flag >= KEYSPEC_DELETE_MAX) {
    HT_ERRORF("Unknown key flag encountered (%d), skipping..", (int)key.flag);
    return;
  }

  if (key.flag == FLAG_DELETE_ROW) {
    for (size_t i=0; i<m_access_group_vector.size(); ++i)
      m_access_group_vector[i]->add(key, value);
  }
  else {
    if (key.column_family_code >= m_column_family_vector.size() ||
        m_column_family_vector[key.column_family_code] == 0) {
      HT_ERRORF("Bad column family code encountered (%d) for table %s, skipping...",
                (int)key.column_family_code, m_table.id);
      return;
    }
    m_column_family_vector[key.column_family_code]->add(key, value);
  }

  if (key.flag == FLAG_INSERT)
    m_added_inserts++;
  else
    m_added_deletes[key.flag]++;

  if (key.revision > m_revision)
    m_revision = key.revision;
}


void Range::create_scanner(ScanContextPtr &scan_ctx, MergeScannerRangePtr &scanner) {
  scanner = std::make_shared<MergeScannerRange>(m_table.id, scan_ctx);
  AccessGroupVector ag_vector(0);

  HT_ASSERT(m_initialized);

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
    m_scans++;
  }

  try {
    for (auto & ag : ag_vector) {
      if (ag->include_in_scan(scan_ctx.get()))
        scanner->add_scanner(ag->create_scanner(scan_ctx.get()));
    }
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, "");
  }

  // increment #scanners
}

CellListScanner *Range::create_scanner_pseudo_table(ScanContextPtr &scan_ctx,
                                                    const String &table_name) {
  CellListScannerBuffer *scanner = 0;
  AccessGroupVector ag_vector(0);

  if (!m_initialized)
    deferred_initialization(scan_ctx->timeout_ms);

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
    m_scans++;
  }

  if (table_name != ".cellstore.index")
    HT_THROW(Error::INVALID_PSEUDO_TABLE_NAME, table_name);

  scanner = new CellListScannerBuffer(scan_ctx);

  try {
    for (auto &ag : ag_vector)
      ag->populate_cellstore_index_pseudo_table_scanner(scanner);
  }
  catch (Exception &e) {
    delete scanner;
    HT_THROW2(e.code(), e, "");
  }

  return scanner;
}


bool Range::need_maintenance() {
  lock_guard<mutex> lock(m_schema_mutex);
  bool needed = false;
  int64_t mem, disk, disk_total = 0;
  if (!m_metalog_entity->get_load_acknowledged() || m_unsplittable)
    return false;
  for (size_t i=0; i<m_access_group_vector.size(); ++i) {
    m_access_group_vector[i]->space_usage(&mem, &disk);
    disk_total += disk;
    if (mem >= Global::access_group_max_mem)
      needed = true;
  }
  if (disk_total >= m_split_threshold)
    needed = true;
  return needed;
}


bool Range::cancel_maintenance() {
  return m_dropped ? true : false;
}

#if TESTING_SUPPORT
std::string
Range::DumpStat(DumpInfo* stat)
{
	AccessGroupVector ag_vector(0);
	{
	  lock_guard<mutex> lock(m_schema_mutex);
	  ag_vector = m_access_group_vector;
	}

	int count = 0;
	DumpInfo curr;

	string group;
	for (size_t i = 0; i < ag_vector.size(); i++) {
		if (strcmp(ag_vector[i]->get_name(), "default") == 0) {
			continue;
		}
		//char buf[128];
		//snprintf(buf, sizeof(buf), "ag: %s %s", ag_vector[i]->get_name(), ag_vector[i]->DumpStat(&curr).c_str());
		group += ag_vector[i]->DumpStat(&curr);
		count++;
	}

	const char* stat_str = "";
	switch (m_metalog_entity->get_state()) {
	case RangeState::STEADY:					stat_str = "";
		break;
	case RangeState::SPLIT_LOG_INSTALLED:		stat_str = "sp-pre";
		break;
	case RangeState::SPLIT_SHRUNK:				stat_str = "sp-shk";
		break;
	case RangeState::RELINQUISH_LOG_INSTALLED: 	stat_str = "rq-pre";
		break;
	case RangeState::RELINQUISH_COMPACTED:		stat_str = "rq-cpc";
		break;
	default:
		break;
	}

	if (m_is_compact) {
		stat_str = "cmpact";
	}
	char data[64* 1024];
	snprintf(data, sizeof(data), "range: %-10s, stat: %-6s, limit: %s, split: %s, %s",
    	m_name.c_str(), stat_str,
		string_size(m_split_threshold).c_str(),
		(curr.disk + curr.cache) > (int64_t)Global::range_maximum_size ? "true" : "false",
		group.c_str());

	if (stat) {
		stat->disk 	+= curr.disk;
		stat->cache += curr.cache;
		stat->file_count += curr.file_count;
		stat->index += curr.index;

		stat->cache_count = curr.cache_count;
	}
    return data;
}

uint64_t
Range::DumpCacheCount(bool total)
{
	uint64_t count = 0;
	/** already locked here */
	for (size_t i = 0; i < m_access_group_vector.size(); i++) {
		if (strcmp(m_access_group_vector[i]->get_name(), "default") == 0) {
			continue;
		}
		//char buf[128];
		//snprintf(buf, sizeof(buf), "ag: %s %s", ag_vector[i]->get_name(), ag_vector[i]->DumpStat(&curr).c_str());
		count += m_access_group_vector[i]->DumpCacheCount(total);
	}
	return count;
}
#endif

Range::MaintenanceData *
Range::get_maintenance_data(ByteArena &arena, time_t now,
                            int flags, TableMutator *mutator) {
  MaintenanceData *mdata = (MaintenanceData *)arena.alloc( sizeof(MaintenanceData) );
  AccessGroup::MaintenanceData **tailp = 0;
  AccessGroupVector  ag_vector(0);
  int64_t size=0;
  int64_t starting_maintenance_generation;

  memset(mdata, 0, sizeof(MaintenanceData));

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
    mdata->load_factors.scans = m_scans;
    mdata->load_factors.updates = m_updates;
    mdata->load_factors.bytes_written = m_bytes_written;
    mdata->load_factors.cells_written = m_cells_written;
    mdata->schema_generation = m_table.generation;
  }

  mdata->relinquish = m_relinquish;

  // record starting maintenance generation
  {
    lock_guard<mutex> lock(m_mutex);
    starting_maintenance_generation = m_maintenance_generation;
    mdata->load_factors.cells_scanned = m_cells_scanned;
    mdata->cells_returned = m_cells_returned;
    mdata->load_factors.bytes_scanned = m_bytes_scanned;
    mdata->bytes_returned = m_bytes_returned;
    mdata->load_factors.disk_bytes_read = m_disk_bytes_read;
    mdata->table_id = m_table.id;
    mdata->is_metadata = m_is_metadata;
    mdata->is_system = m_table.is_system();
    mdata->state = m_metalog_entity->get_state();
    mdata->soft_limit = m_metalog_entity->get_soft_limit();
    mdata->busy = m_maintenance_guard.in_progress() || !m_metalog_entity->get_load_acknowledged();
    mdata->needs_major_compaction = m_metalog_entity->get_needs_compaction();
    mdata->initialized = m_initialized;
    mdata->compaction_type_needed = m_compaction_type_needed;
  }

  for (size_t i=0; i<ag_vector.size(); i++) {
    if (mdata->agdata == 0) {
      mdata->agdata = ag_vector[i]->get_maintenance_data(arena, now, flags);
      tailp = &mdata->agdata;
    }
    else {
      (*tailp)->next = ag_vector[i]->get_maintenance_data(arena, now, flags);
      tailp = &(*tailp)->next;
    }
    size += (*tailp)->disk_estimate;
    mdata->disk_used += (*tailp)->disk_used;
    mdata->compression_ratio += (double)(*tailp)->disk_used / (*tailp)->compression_ratio;
    mdata->disk_estimate += (*tailp)->disk_estimate;
    mdata->memory_used += (*tailp)->mem_used;
    mdata->memory_allocated += (*tailp)->mem_allocated;
    mdata->block_index_memory += (*tailp)->block_index_memory;
    mdata->bloom_filter_memory += (*tailp)->bloom_filter_memory;
    mdata->bloom_filter_accesses += (*tailp)->bloom_filter_accesses;
    mdata->bloom_filter_maybes += (*tailp)->bloom_filter_maybes;
    mdata->bloom_filter_fps += (*tailp)->bloom_filter_fps;
    mdata->shadow_cache_memory += (*tailp)->shadow_cache_memory;
    mdata->cell_count += (*tailp)->cell_count;
    mdata->file_count += (*tailp)->file_count;
    mdata->key_bytes += (*tailp)->key_bytes;
    mdata->value_bytes += (*tailp)->value_bytes;
  }

  if (mdata->disk_used)
    mdata->compression_ratio = (double)mdata->disk_used / mdata->compression_ratio;
  else
    mdata->compression_ratio = 1.0;

  if (tailp)
    (*tailp)->next = 0;

  if (!m_unsplittable && size >= m_split_threshold)
    mdata->needs_split = true;

  mdata->unsplittable = m_unsplittable;

  if (size > Global::range_maximum_size) {
    lock_guard<mutex> lock(m_mutex);
    if (starting_maintenance_generation == m_maintenance_generation)
      m_capacity_exceeded_throttle = true;
  }

  mdata->load_acknowledged = load_acknowledged();

  if (mutator)
    m_load_metrics.compute_and_store(mutator, now, mdata->load_factors,
                                     mdata->disk_used, mdata->memory_used,
                                     mdata->compression_ratio);

  return mdata;
}


void Range::relinquish() {

  if (!m_initialized)
    deferred_initialization();

  time_debug_using(BEG_PREFIX << "relinquish" << SUFFIX);

  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);

  int state = m_metalog_entity->get_state();

  // Make sure range is in a relinquishable state
  if (state != RangeState::STEADY &&
      state != RangeState::RELINQUISH_LOG_INSTALLED &&
      state != RangeState::RELINQUISH_COMPACTED) {
    HT_INFOF("Cancelling relinquish because range is not in relinquishable state (%s)",
             RangeState::get_text(state).c_str());
    return;
  }

  range_inc(ST_relinquish);

  try {
    switch (state) {
    case (RangeState::STEADY):
      {
        RangeSpecManaged range_spec;
        m_metalog_entity->get_range_spec(range_spec);
        if (Global::immovable_range_set_contains(m_table, range_spec)) {
          HT_WARNF("Aborting relinquish of %s because marked immovable.", m_name.c_str());
          range_dec(ST_relinquish);
          return;
        }
      }
      relinquish_install_log();
    case (RangeState::RELINQUISH_LOG_INSTALLED):
      relinquish_compact();
    case (RangeState::RELINQUISH_COMPACTED):
      relinquish_finalize();
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  time_debug_using(TIME_PREFIX << "relinquish, try to get lock for maintain generation" << SUFFIX);
  {
    lock_guard<mutex> lock(m_mutex);
    m_capacity_exceeded_throttle = false;
    m_maintenance_generation++;
  }

  HT_INFO("Relinquish Complete.");

  range_dec(ST_normal);
  range_dec(ST_relinquish);
  time_debug_mute_warn(END_PREFIX << "complete relinquish" << SUFFIX);
}


void Range::relinquish_install_log() {
  String logname;
  AccessGroupVector ag_vector(0);

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  {
    lock_guard<mutex> lock(m_mutex);

    logname = TransferLog(Global::transfer_dfs, Global::toplevel_dir,
                          m_table.id, m_metalog_entity->get_end_row()).name();

    Global::transfer_dfs->mkdirs(logname);

    m_metalog_entity->set_transfer_log(logname);


    /**
     * Persist RELINQUISH_LOG_INSTALLED Metalog state
     */
    m_metalog_entity->set_state(RangeState::RELINQUISH_LOG_INSTALLED,
                                Global::location_initializer->get());

    for (int i=0; true; i++) {
      try {
        Global::rsml_writer->record_state(m_metalog_entity);
        break;
      }
      catch (Exception &e) {
        if (i<3) {
          HT_WARNF("%s - %s", Error::get_text(e.code()), e.what());
          this_thread::sleep_for(chrono::milliseconds(5000));
          continue;
        }
        HT_ERRORF("Problem updating meta log entry with RELINQUISH_LOG_INSTALLED state for %s",
                  m_name.c_str());
        HT_FATAL_OUT << e << HT_END;
      }
    }
    time_debug_using(TIME_PREFIX << "relinquish, lock mkdir and set meta log" << SUFFIX);
  }

  /**
   * Create and install the transfer log
   */
  {
    Barrier::ScopedActivator block_updates(m_update_barrier);
    time_debug_using(TIME_PREFIX << "relinquish, get barrier for switch" << SUFFIX);
    lock_guard<mutex> lock(m_mutex);
    time_debug_using(TIME_PREFIX << "relinquish, get barrier and lock for switch" << SUFFIX);
	
	#if TESTING_SUPPORT
	m_transfer_log = make_shared<CommitLog>(Global::log_dfs, Global::transfer_dfs, logname, !m_table.is_user());
	#else
	m_transfer_log = make_shared<CommitLog>(Global::dfs, logname, !m_table.is_user());
	#endif
    
    for (size_t i=0; i<ag_vector.size(); i++)
      ag_vector[i]->stage_compaction();
    time_debug_using(TIME_PREFIX << "relinquish, lock and stage access compaction" << SUFFIX);
  }

}

void Range::relinquish_compact() {
  String location = Global::location_initializer->get();
  AccessGroupVector ag_vector(0);

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  /**
   * Perform minor compactions
   */
  std::vector<AccessGroup::Hints> hints(ag_vector.size());
  for (size_t i=0; i<ag_vector.size(); i++)
    ag_vector[i]->run_compaction(MaintenanceFlag::COMPACT_MINOR |
                                 MaintenanceFlag::RELINQUISH, &hints[i]);
  time_debug_mute_warn(TIME_PREFIX << "relinquish, compaction complete" << SUFFIX);

  m_hints_file.set(hints);
  m_hints_file.write(location);
  time_debug_using(TIME_PREFIX << "relinquish, write hint file" << SUFFIX);

  String end_row = m_metalog_entity->get_end_row();

  // Record "move" in sys/RS_METRICS
  if (Global::rs_metrics_table) {
    TableMutatorPtr mutator(Global::rs_metrics_table->create_mutator());
    KeySpec key;
    String row = location + ":" + m_table.id;
    key.row = row.c_str();
    key.row_len = row.length();
    key.column_family = "range_move";
    key.column_qualifier = end_row.c_str();
    key.column_qualifier_len = end_row.length();
    try {
      mutator->set(key, 0, 0);
      mutator->flush();
    }
    catch (Exception &e) {
      HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
    }
    time_debug_using(TIME_PREFIX << "relinquish, set range move info" << SUFFIX);
  }

  // Mark range as "dropped" preventing further scans and updates
  drop();

  m_metalog_entity->set_state(RangeState::RELINQUISH_COMPACTED,
                              Global::location_initializer->get());
}

void Range::relinquish_finalize() {
  TableIdentifierManaged table_frozen;
  String start_row, end_row;

  {
    lock_guard<mutex> lock(m_schema_mutex);
    lock_guard<mutex> lock2(m_mutex);
    table_frozen = m_table;
  }

  m_metalog_entity->get_boundary_rows(start_row, end_row);

  HT_INFOF("Reporting relinquished range %s to Master", m_name.c_str());

  RangeSpecManaged range_spec;
  m_metalog_entity->get_range_spec(range_spec);

  HT_MAYBE_FAIL("relinquish-move-range");

  m_master_client->move_range(m_metalog_entity->get_source(),
                              m_metalog_entity->id(),
			      table_frozen, range_spec,
                              m_metalog_entity->get_transfer_log(),
                              m_metalog_entity->get_soft_limit(), false);

  // Remove range from TableInfo
  if (!m_range_set->remove(start_row, end_row)) {
    HT_ERROR_OUT << "Problem removing range " << m_name << HT_END;
    HT_ABORT;
  }

  // Mark the Range entity for removal
  std::vector<MetaLog::EntityPtr> entities;
  m_metalog_entity->mark_for_removal();
  entities.push_back(m_metalog_entity);

  time_debug_using(TIME_PREFIX << "relinquish, notify master move" << SUFFIX);

  // Add acknowledge relinquish task
  MetaLog::EntityTaskPtr acknowledge_relinquish_task =
    make_shared<MetaLog::EntityTaskAcknowledgeRelinquish>(m_metalog_entity->get_source(),
                                                          m_metalog_entity->id(),
							  table_frozen, range_spec);
  entities.push_back(acknowledge_relinquish_task);

  /**
   * Add the log removal task and remove range from RSML
   */
  for (int i=0; true; i++) {
    try {
      Global::rsml_writer->record_state(entities);
      break;
    }
    catch (Exception &e) {
      if (i<6) {
        HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
        this_thread::sleep_for(chrono::milliseconds(5000));
        continue;
      }
      HT_ERRORF("Problem recording removal for range %s", m_name.c_str());
      HT_FATAL_OUT << e << HT_END;
    }
  }

  // Add tasks to work queue
  Global::add_to_work_queue(acknowledge_relinquish_task);

  // disables any further maintenance
  m_maintenance_guard.disable();

  time_debug_using(TIME_PREFIX << "relinquish, write meta log" << SUFFIX);
}





void Range::split() {

  if (!m_initialized)
    deferred_initialization();

  time_reset();

  time_debug_using(BEG_PREFIX << "split" << SUFFIX);

  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);
  String old_start_row;

  // do not split if the RangeServer is not yet fully initialized
  if (Global::rsml_writer.get() == 0)
    return;

  HT_ASSERT(!m_is_root);

  int state = m_metalog_entity->get_state();

  // Make sure range is in a splittable state
  if (state != RangeState::STEADY &&
      state != RangeState::SPLIT_LOG_INSTALLED &&
      state != RangeState::SPLIT_SHRUNK) {
    HT_INFOF("Cancelling split because range is not in splittable state (%s)",
             RangeState::get_text(state).c_str());
    return;
  }

  if (m_unsplittable) {
    HT_WARNF("Split attempted on range %s, but marked unsplittable",
             m_name.c_str());
    return;
  }

  range_inc(ST_split);
  try {
    switch (state) {

    case (RangeState::STEADY):
      split_install_log();

    case (RangeState::SPLIT_LOG_INSTALLED):
      split_compact_and_shrink();

    case (RangeState::SPLIT_SHRUNK):
      split_notify_master();
    }
  }
  catch (Exception &e) {
	  range_dec(ST_split);
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  {
    lock_guard<mutex> lock(m_mutex);
    m_capacity_exceeded_throttle = false;
    m_maintenance_generation++;
  }

  HT_INFOF("Split Complete.  New Range end_row=%s",
           m_metalog_entity->get_end_row().c_str());

  range_dec(ST_split);
  time_debug_mute_warn(END_PREFIX << "complete split" << SUFFIX);
}



/**
 */
void Range::split_install_log() {
  String split_row;
  std::vector<String> split_rows;
  AccessGroupVector ag_vector(0);
  String logname;
  String start_row, end_row;

  m_metalog_entity->get_boundary_rows(start_row, end_row);

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  /**
   * Split row determination Algorithm:
   *
   * TBD
   */

  time_debug_using(TIME_PREFIX << "split, get access group and bondary" << SUFFIX);

  {
    StlArena arena(128000);
    CellList::SplitRowDataMapT split_row_data = 
      CellList::SplitRowDataMapT(LtCstr(), CellList::SplitRowDataAlloc(arena));

    // Fetch CellStore block index split row data from
    for (const auto &ag : ag_vector)
      ag->split_row_estimate_data_stored(split_row_data);
    time_debug_using(TIME_PREFIX << "split, get stor split row" << SUFFIX);

    // Fetch CellCache split row data from
    for (const auto &ag : ag_vector)
      ag->split_row_estimate_data_cached(split_row_data);
    time_debug_using(TIME_PREFIX << "split, get cache split row" << SUFFIX);

    // Estimate split row from split row data
    if (!estimate_split_row(split_row_data, split_row)) {
      if (Global::row_size_unlimited) {
        m_unsplittable = true;
        HT_WARNF("Split attempt aborted for range %s because it is marked unsplittable",
                 m_name.c_str());
        HT_THROW(Error::CANCELLED, "");
      }
      m_error = Error::RANGESERVER_ROW_OVERFLOW;
      HT_THROWF(Error::RANGESERVER_ROW_OVERFLOW,
                "Unable to determine split row for range %s",
                m_name.c_str());
    }

    // Instrumentation for issue 1193
    if (split_row.compare(end_row) >= 0 || split_row.compare(start_row) <= 0) {
      LoadDataEscape escaper;
      String escaped_start_row, escaped_end_row, escaped_split_row;
      for (auto &entry : split_row_data) {
	escaper.escape(entry.first, strlen(entry.first), escaped_split_row);
	HT_ERRORF("[split_row_data] %lld %s", (Lld)entry.second, escaped_split_row.c_str());
      }
      escaper.escape(start_row.c_str(), start_row.length(), escaped_start_row);
      escaper.escape(end_row.c_str(), end_row.length(), escaped_end_row);
      escaper.escape(split_row.c_str(), split_row.length(), escaped_split_row);
      HT_FATALF("Bad split row estimate (%s) for range %s[%s..%s]",
		escaped_split_row.c_str(), m_table.id, escaped_start_row.c_str(),
		escaped_end_row.c_str());
    }

    HT_INFOF("Split row estimate for %s is '%s'",
             m_name.c_str(), split_row.c_str());

    size_t min_len = std::min(end_row.length(), (size_t)5);
    time_debug_using(TIME_PREFIX << "split, get split row [" << split_row.substr(0, min_len) << "]" << SUFFIX);
  }


  {
    lock_guard<mutex> lock(m_mutex);
    m_metalog_entity->set_split_row(split_row);

    logname = TransferLog(Global::transfer_dfs, Global::toplevel_dir,
                          m_table.id, split_row).name();

    Global::transfer_dfs->mkdirs(logname);

    m_metalog_entity->set_transfer_log(logname);

    if (m_split_off_high)
      m_metalog_entity->set_old_boundary_row(end_row);
    else
      m_metalog_entity->set_old_boundary_row(start_row);

    /**
     * Persist SPLIT_LOG_INSTALLED Metalog state
     */
    m_metalog_entity->set_state(RangeState::SPLIT_LOG_INSTALLED,
                                Global::location_initializer->get());
    for (int i=0; true; i++) {
      try {
        Global::rsml_writer->record_state(m_metalog_entity);
        break;
      }
      catch (Exception &e) {
        if (i<3) {
          HT_WARNF("%s - %s", Error::get_text(e.code()), e.what());
          this_thread::sleep_for(chrono::milliseconds(5000));
          continue;
        }
        HT_ERRORF("Problem updating meta log with SPLIT_LOG_INSTALLED state for %s "
                  "split-point='%s'", m_name.c_str(), split_row.c_str());
        HT_FATAL_OUT << e << HT_END;
      }
    }
    time_debug_using(TIME_PREFIX << "split, lock mkdir and write meta log" << SUFFIX);
  }

  /**
   * Create and install the transfer log
   */
  {
    Barrier::ScopedActivator block_updates(m_update_barrier);
    time_debug_using(TIME_PREFIX << "split, get barrier for switch" << SUFFIX);
    lock_guard<mutex> lock(m_mutex);
    time_debug_using(TIME_PREFIX << "split, get barrier and lock for switch" << SUFFIX);

    m_split_row = split_row;
    for (size_t i=0; i<ag_vector.size(); i++)
      ag_vector[i]->stage_compaction();
    time_debug_using(TIME_PREFIX << "split, lock stage access group compaction" << SUFFIX);

	#if TESTING_SUPPORT
	m_transfer_log = make_shared<CommitLog>(Global::log_dfs, Global::transfer_dfs, logname, !m_table.is_user());
	#else
    m_transfer_log = make_shared<CommitLog>(Global::dfs, logname, !m_table.is_user());
	#endif
  }

  HT_MAYBE_FAIL("split-1");
  HT_MAYBE_FAIL_X("metadata-split-1", m_is_metadata);
}

bool Range::estimate_split_row(CellList::SplitRowDataMapT &split_row_data, String &row) {

  // Set target to half the total number of keys
  int64_t target = 0;
  for (CellList::SplitRowDataMapT::iterator iter=split_row_data.begin();
       iter != split_row_data.end(); ++iter)
    target += iter->second;
  target /= 2;

  row.clear();
  if (target == 0)
    return false;

  int64_t cumulative = 0;
  for (CellList::SplitRowDataMapT::iterator iter=split_row_data.begin();
       iter != split_row_data.end(); ++iter) {
    if (cumulative + iter->second >= target) {
      if (cumulative > 0)
        --iter;
      row = iter->first;
      break;
    }
    cumulative += iter->second;
  }
  HT_ASSERT(!row.empty());
  // If row chosen above is same as end row, find largest row <= end_row
  String end_row = m_metalog_entity->get_end_row();
  if (row.compare(end_row) >= 0) {
    row.clear();
    for (CellList::SplitRowDataMapT::iterator iter=split_row_data.begin();
         iter != split_row_data.end(); ++iter) {
      if (strcmp(iter->first, end_row.c_str()) < 0)
        row = iter->first;
      else
        break;
    }
    return !row.empty();
  }
  return true;
}


void Range::split_compact_and_shrink() {
  int error;
  String start_row, end_row, split_row;
  AccessGroupVector ag_vector(0);
  String location = Global::location_initializer->get();

  m_metalog_entity->get_boundary_rows(start_row, end_row);
  split_row = m_metalog_entity->get_split_row();

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  AccessGroupHintsFile new_hints_file(m_table.id, start_row, end_row);
  std::vector<AccessGroup::Hints> hints(ag_vector.size());

  STADGE_TIMER;
  std::string transfer_info;

  /**
   * Perform major compactions
   */
  for (size_t i=0; i<ag_vector.size(); i++)
    ag_vector[i]->run_compaction(MaintenanceFlag::COMPACT_MAJOR|MaintenanceFlag::SPLIT,
                                 &hints[i]);
  STADGE_UPDATE;
  time_debug_mute_warn(TIME_PREFIX << "split, compact compelte" << SUFFIX);

  m_hints_file.set(hints);
  new_hints_file.set(hints);

  String files;
  String metadata_row_low, metadata_row_high;
  int64_t total_blocks;
  KeySpec key_low, key_high;
  char buf[32];

  TableMutatorPtr mutator(Global::metadata_table->create_mutator());

  // For new range with existing end row, update METADATA entry with new
  // 'StartRow' column.

  metadata_row_high = String("") + m_table.id + ":" + end_row;
  key_high.row = metadata_row_high.c_str();
  key_high.row_len = metadata_row_high.length();
  key_high.column_qualifier = 0;
  key_high.column_qualifier_len = 0;
  key_high.column_family = "StartRow";
  mutator->set(key_high, (uint8_t *)split_row.c_str(), split_row.length());

  // This is needed to strip out the "live file" references
  if (m_split_off_high) {
    key_high.column_family = "Files";
    for (size_t i=0; i<ag_vector.size(); i++) {
      key_high.column_qualifier = ag_vector[i]->get_name();
      key_high.column_qualifier_len = strlen(ag_vector[i]->get_name());
      ag_vector[i]->get_file_data(files, &total_blocks, false);
      if (files != "")
        mutator->set(key_high, (uint8_t *)files.c_str(), files.length());
    }
  }

  // For new range whose end row is the split point, create a new METADATA
  // entry
  metadata_row_low = format("%s:%s", m_table.id, split_row.c_str());
  key_low.row = metadata_row_low.c_str();
  key_low.row_len = metadata_row_low.length();
  key_low.column_qualifier = 0;
  key_low.column_qualifier_len = 0;

  key_low.column_family = "StartRow";
  mutator->set(key_low, start_row.c_str(), start_row.length());

  for (size_t i=0; i<ag_vector.size(); i++) {
    ag_vector[i]->get_file_data(files, &total_blocks, m_split_off_high);
    key_low.column_family = key_high.column_family = "BlockCount";
    key_low.column_qualifier = key_high.column_qualifier = ag_vector[i]->get_name();
    key_low.column_qualifier_len = key_high.column_qualifier_len = strlen(ag_vector[i]->get_name());
    sprintf(buf, "%llu", (Llu)total_blocks/2);
    mutator->set(key_low, (uint8_t *)buf, strlen(buf));
    mutator->set(key_high, (uint8_t *)buf, strlen(buf));
    if (files != "") {
      key_low.column_family = "Files";
      mutator->set(key_low, (uint8_t *)files.c_str(), files.length());
    }
  }
  if (m_split_off_high) {
    key_low.column_qualifier = 0;
    key_low.column_qualifier_len = 0;
    key_low.column_family = "Location";
    mutator->set(key_low, location.c_str(), location.length());
  }

  mutator->flush();

  STADGE_UPDATE;
  time_debug_using(TIME_PREFIX << "split, notify global range route" << SUFFIX);

  /**
   *  Shrink the range
   */
  {
	  time_reset();
    Barrier::ScopedActivator block_updates(m_update_barrier);
    time_debug_using(TIME_PREFIX << "split, get barrier for shrink, wait until resp"
    		<< SUFFIX);
    Barrier::ScopedActivator block_scans(m_scan_barrier);
    lock_guard<mutex> lock(m_mutex);
    time_debug_using(TIME_PREFIX << "split, get update scan barrier and lock for shrink" << SUFFIX);

    transfer_info = m_transfer_log->DumpStat();
    STADGE_UPDATE;
    // Shrink access groups
    if (m_split_off_high)
      m_range_set->change_end_row(start_row, end_row, split_row);
    else
      m_range_set->change_start_row(start_row, split_row, end_row);

    std::string origin_end = end_row;
    // Shrink access groups
    if (m_split_off_high) {
      m_metalog_entity->set_end_row(split_row);
      m_hints_file.change_end_row(split_row);
      new_hints_file.change_start_row(split_row);
      end_row = split_row;
    }
    else {
      m_metalog_entity->set_start_row(split_row);
      m_hints_file.change_start_row(split_row);
      new_hints_file.change_end_row(split_row);
      start_row = split_row;
    }

    m_load_metrics.change_rows(start_row, end_row);

	#if TESTING_SUPPORT
      size_t min_len = std::min(end_row.length(), (size_t)5);
      m_name = format("%s[%s]", m_table.id, end_row.substr(0, min_len).c_str());
	#else
	  m_name = String(m_table.id)+"["+start_row+".."+end_row+"]";
	#endif

	 min_len = std::min(origin_end.length(), (size_t)5);
	 time_debug_using(TIME_PREFIX << "split, local origin end [" << origin_end.substr(0, min_len) << "]" << SUFFIX);

	 uint64_t origin = 0;
	 uint64_t remain = 0;
    for (size_t i=0; i<ag_vector.size(); i++) {
      origin += ag_vector[i]->DumpCacheCount(false);
      ag_vector[i]->shrink(split_row, m_split_off_high, &hints[i]);
      remain += ag_vector[i]->DumpCacheCount(false);
    }
    time_debug_using(TIME_PREFIX << "split, lock and access group shrink, "
    	<< "traverse " << string_count(origin) << ", insert " << string_count(remain) << SUFFIX);

    STADGE_UPDATE;
    // Close and uninstall split log
    m_split_row = "";
    if ((error = m_transfer_log->close()) != Error::OK) {
      HT_ERRORF("Problem closing split log '%s' - %s",
                m_transfer_log->get_log_dir().c_str(), Error::get_text(error));
    }
    m_transfer_log = 0;

    time_debug_using(TIME_PREFIX << "split, lock close transfer log" << SUFFIX);

    /**
     * Write existing hints file and new hints file.
     * The hints array will have been setup by the call to shrink() for the
     * existing range.  The new_hints_file will get it's disk usage updated
     * by subtracting the disk usage of the existing hints file from the
     * original disk usage.
     */
    m_hints_file.set(hints);
    m_hints_file.write(location);
    for (size_t i=0; i<new_hints_file.get().size(); i++) {
      if (hints[i].disk_usage > new_hints_file.get()[i].disk_usage) {
        // issue 1159
        HT_ERRORF("hints[%d].disk_usage (%llu) > new_hints_file.get()[%d].disk_usage (%llu)",
                  (int)i, (Llu)hints[i].disk_usage, (int)i, (Llu)new_hints_file.get()[i].disk_usage);
        HT_ERRORF("%s", ag_vector[i]->describe().c_str());
        HT_ASSERT(hints[i].disk_usage <= new_hints_file.get()[i].disk_usage);
      }
      new_hints_file.get()[i].disk_usage -= hints[i].disk_usage;
    }
    new_hints_file.write("");
    time_debug_using(TIME_PREFIX << "split, lock write hint" << SUFFIX);
  }

  if (m_split_off_high) {
    /** Create FS directories for this range **/
    {
      char md5DigestStr[33];
      String table_dir, range_dir;

      md5_trunc_modified_base64(end_row.c_str(), md5DigestStr);
      md5DigestStr[16] = 0;
      table_dir = Global::toplevel_dir + "/tables/" + m_table.id;

      {
        lock_guard<mutex> lock(m_schema_mutex);
        for (auto ag_spec : m_schema->get_access_groups()) {
          // notice the below variables are different "range" vs. "table"
          range_dir = table_dir + "/" + ag_spec->get_name() + "/" + md5DigestStr;
          Global::dfs->mkdirs(range_dir);
        }
      }
      time_debug_using(TIME_PREFIX << "split, mkdir" << SUFFIX);
    }

  }

  /**
   * Persist SPLIT_SHRUNK MetaLog state
   */
  {
    lock_guard<mutex> lock(m_mutex);
    m_metalog_entity->set_state(RangeState::SPLIT_SHRUNK, location);
    for (int i=0; true; i++) {
      try {
        Global::rsml_writer->record_state(m_metalog_entity);
        break;
      }
      catch (Exception &e) {
        if (i<3) {
          HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
          this_thread::sleep_for(chrono::milliseconds(5000));
          continue;
        }
        HT_ERRORF("Problem updating meta log entry with SPLIT_SHRUNK state %s "
                  "split-point='%s'", m_name.c_str(), split_row.c_str());
        HT_FATAL_OUT << e << HT_END;
      }
    }
    time_debug_using(TIME_PREFIX << "split, lock write meta log" << SUFFIX);
  }
  STADGE_UPDATE;

  time_debug_using(TIME_PREFIX << "export time: access compact " << STADGE_STRING
		  << ", notify range change " << STADGE_STRING
		  << ", wait lock " << STADGE_STRING
		  << ", shrink " << STADGE_STRING
		  << ", write meta " << STADGE_STRING
		  << ", transfer " << transfer_info << SUFFIX);

  HT_MAYBE_FAIL("split-2");
  HT_MAYBE_FAIL_X("metadata-split-2", m_is_metadata);
}


void Range::split_notify_master() {
  RangeSpecManaged range;
  TableIdentifierManaged table_frozen;
  String start_row, end_row, old_boundary_row;
  int64_t soft_limit = m_metalog_entity->get_soft_limit();

  m_metalog_entity->get_boundary_rows(start_row, end_row);
  old_boundary_row = m_metalog_entity->get_old_boundary_row();

  if (cancel_maintenance())
    HT_THROW(Error::CANCELLED, "");

  if (m_split_off_high) {
    range.set_start_row(end_row);
    range.set_end_row(old_boundary_row);
  }
  else {
    range.set_start_row(old_boundary_row);
    range.set_end_row(start_row);
  }

  // update the latest generation, this should probably be protected
  {
    lock_guard<mutex> lock(m_schema_mutex);
    table_frozen = m_table;
  }

  HT_INFOF("Reporting newly split off range %s[%s..%s] to Master",
           m_table.id, range.start_row, range.end_row);

  time_debug_using(TIME_PREFIX << "split, get range row and frozen table" << SUFFIX);
  if (!m_is_metadata && soft_limit < Global::range_split_size) {
    soft_limit *= 2;
    if (soft_limit > Global::range_split_size)
      soft_limit = Global::range_split_size;
  }

  m_master_client->move_range(m_metalog_entity->get_source(),
                              m_metalog_entity->id(),
			      table_frozen, range,
                              m_metalog_entity->get_transfer_log(),
                              soft_limit, true);

  time_debug_using(TIME_PREFIX << "split, notify master" << SUFFIX);

  /**
   * NOTE: try the following crash and make sure that the master does
   * not try to load the range twice.
   */

  HT_MAYBE_FAIL("split-3");
  HT_MAYBE_FAIL_X("metadata-split-3", m_is_metadata);

  MetaLog::EntityTaskPtr acknowledge_relinquish_task;
  std::vector<MetaLog::EntityPtr> entities;

  // Add Range entity with updated state
  entities.push_back(m_metalog_entity);

  // Add acknowledge relinquish task
  acknowledge_relinquish_task = 
    make_shared<MetaLog::EntityTaskAcknowledgeRelinquish>(m_metalog_entity->get_source(),
                                                          m_metalog_entity->id(),
							  table_frozen, range);
  entities.push_back(acknowledge_relinquish_task);

  /**
   * Persist STEADY Metalog state and log removal task
   */
  m_metalog_entity->clear_state();
  m_metalog_entity->set_soft_limit(soft_limit);

  for (int i=0; true; i++) {
    try {
      Global::rsml_writer->record_state(entities);
      break;
    }
    catch (Exception &e) {
      if (i<2) {
        HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
        this_thread::sleep_for(chrono::milliseconds(5000));
        continue;
      }
      HT_ERRORF("Problem updating meta log with STEADY state for %s",
                m_name.c_str());
      HT_FATAL_OUT << e << HT_END;
    }
  }

  time_debug_using(TIME_PREFIX << "split, write meta log" << SUFFIX);

  // Add tasks to work queue
  Global::add_to_work_queue(acknowledge_relinquish_task);

  HT_MAYBE_FAIL("split-4");
  HT_MAYBE_FAIL_X("metadata-split-4", m_is_metadata);
}


void Range::compact(MaintenanceFlag::Map &subtask_map) {

  if (!m_initialized)
    deferred_initialization();

  range_inc(ST_compact);
  time_debug_using(BEG_PREFIX << "compact" << SUFFIX);

  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);
  AccessGroupVector ag_vector(0);
  int flags = 0;
  int state = m_metalog_entity->get_state();

  // Make sure range is in a compactible state
  if (state == RangeState::RELINQUISH_LOG_INSTALLED ||
      state == RangeState::SPLIT_LOG_INSTALLED ||
      state == RangeState::SPLIT_SHRUNK) {
    HT_INFOF("Cancelling compact because range is not in compactable state (%s)",
             RangeState::get_text(state).c_str());
    return;
  }

  m_is_compact = true;
  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  try {

    // Initiate minor compactions (freeze cell cache)
    {
    	time_reset();
      Barrier::ScopedActivator block_updates(m_update_barrier);
      time_debug_using(TIME_PREFIX << "compact, get barrier" << SUFFIX);
      lock_guard<mutex> lock(m_mutex);
      time_debug_using(TIME_PREFIX << "compact, get barrier and lock" << SUFFIX);

      for (size_t i=0; i<ag_vector.size(); i++) {
        if (m_metalog_entity->get_needs_compaction() ||
            subtask_map.compaction(ag_vector[i].get()))
          ag_vector[i]->stage_compaction();
      }
      time_debug_using(TIME_PREFIX << "compact, lock stadge compaction" << SUFFIX);
    }

    // do compactions
    bool successfully_compacted = false;
    std::vector<AccessGroup::Hints> hints(ag_vector.size());
    for (size_t i=0; i<ag_vector.size(); i++) {

      if (m_metalog_entity->get_needs_compaction())
        flags = MaintenanceFlag::COMPACT_MOVE;
      else
        flags = subtask_map.flags(ag_vector[i].get());

      if (flags & MaintenanceFlag::COMPACT) {
        try {
          ag_vector[i]->run_compaction(flags, &hints[i]);
          successfully_compacted = true;
        }
        catch (Exception &e) {
          ag_vector[i]->unstage_compaction();
          ag_vector[i]->load_hints(&hints[i]);
        }
      }
      else
        ag_vector[i]->load_hints(&hints[i]);
    }
    if (successfully_compacted) {
      m_hints_file.set(hints);
      m_hints_file.write(Global::location_initializer->get());
    }
  }
  catch (Exception &e) {
	  m_is_compact = false;
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  time_debug_mute_warn(TIME_PREFIX << "compact, complete access compaction" << SUFFIX);

  if (m_metalog_entity->get_needs_compaction()) {
    try {
      lock_guard<mutex> lock(m_mutex);
      m_metalog_entity->set_needs_compaction(false);
      Global::rsml_writer->record_state(m_metalog_entity);
    }
    catch (Exception &e) {
    	m_is_compact = false;
      HT_ERRORF("Problem updating meta log entry for %s", m_name.c_str());
    }
    time_debug_mute_warn(END_PREFIX << "compact, lock write meta log" << SUFFIX);
  }

  {
    lock_guard<mutex> lock(m_mutex);
    m_compaction_type_needed = 0;
    m_capacity_exceeded_throttle = false;
    m_maintenance_generation++;
  }

  m_is_compact = false;
  range_dec(ST_compact);
  time_debug_mute_warn(END_PREFIX << "complete compact" << SUFFIX);
}



void Range::purge_memory(MaintenanceFlag::Map &subtask_map) {

  if (!m_initialized)
    deferred_initialization();

  time_debug_using(BEG_PREFIX << "purge memory" << SUFFIX);
  time_reset();

  RangeMaintenanceGuard::Activator activator(m_maintenance_guard);
  AccessGroupVector ag_vector(0);
  uint64_t memory_purged = 0;
  int state = m_metalog_entity->get_state();

  // Make sure range is in a compactible state
  if (state == RangeState::RELINQUISH_LOG_INSTALLED ||
      state == RangeState::SPLIT_LOG_INSTALLED ||
      state == RangeState::SPLIT_SHRUNK) {
    HT_INFOF("Cancelling memory purge because range is not in purgeable state (%s)",
             RangeState::get_text(state).c_str());
    return;
  }

  {
    lock_guard<mutex> lock(m_schema_mutex);
    ag_vector = m_access_group_vector;
  }

  try {
    for (size_t i=0; i<ag_vector.size(); i++) {
      if ( subtask_map.memory_purge(ag_vector[i].get()) )
        memory_purged += ag_vector[i]->purge_memory(subtask_map);
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::CANCELLED || cancel_maintenance())
      return;
    throw;
  }

  time_debug_using(END_PREFIX << "purge, try to get lock" << SUFFIX);
  {
    lock_guard<mutex> lock(m_mutex);
    m_maintenance_generation++;
  }

  HT_INFOF("Memory Purge complete for range %s.  Purged %llu bytes of memory",
    m_name.c_str(), (Llu)memory_purged);
  time_debug_using(END_PREFIX << "purge complete" << SUFFIX);
}


/**
 * This method is called when the range is offline so no locking is needed
 */
void Range::recovery_finalize() {
  int state = m_metalog_entity->get_state();  

  if ((state & RangeState::SPLIT_LOG_INSTALLED)
      == RangeState::SPLIT_LOG_INSTALLED ||
      (state & RangeState::RELINQUISH_LOG_INSTALLED)
      == RangeState::RELINQUISH_LOG_INSTALLED) {
    CommitLogReaderPtr commit_log_reader =
      make_shared<CommitLogReader>(Global::log_dfs, Global::transfer_dfs, m_metalog_entity->get_transfer_log());

    replay_transfer_log(commit_log_reader.get());

    commit_log_reader = 0;

	#if TESTING_SUPPORT
	m_transfer_log = make_shared<CommitLog>(Global::log_dfs, Global::transfer_dfs, m_metalog_entity->get_transfer_log(),
                                   !m_table.is_user());
	#else
	m_transfer_log = make_shared<CommitLog>(Global::dfs, m_metalog_entity->get_transfer_log(),
                                   !m_table.is_user());
	#endif
    

    // re-initiate compaction
    for (size_t i=0; i<m_access_group_vector.size(); i++)
      m_access_group_vector[i]->stage_compaction();

    String transfer_log = m_metalog_entity->get_transfer_log();
    if ((state & RangeState::SPLIT_LOG_INSTALLED)
            == RangeState::SPLIT_LOG_INSTALLED) {
      m_split_row = m_metalog_entity->get_split_row();
      HT_INFOF("Restored range state to SPLIT_LOG_INSTALLED (split point='%s' "
               "xfer log='%s')", m_split_row.c_str(), transfer_log.c_str());
    }
    else
      HT_INFOF("Restored range state to RELINQUISH_LOG_INSTALLED (xfer "
               "log='%s')", transfer_log.c_str());
  }

  for (size_t i=0; i<m_access_group_vector.size(); i++)
    m_access_group_vector[i]->recovery_finalize();
}


void Range::lock() {
  m_schema_mutex.lock();
  m_updates++;  // assumes this method is called for updates only
  for (size_t i=0; i<m_access_group_vector.size(); ++i)
    m_access_group_vector[i]->lock();
  m_revision = TIMESTAMP_MIN;
}


void Range::unlock() {

  // this needs to happen before the subsequent m_mutex lock
  // because the lock ordering is assumed to be Range::m_mutex -> AccessGroup::lock
  for (size_t i=0; i<m_access_group_vector.size(); ++i)
    m_access_group_vector[i]->unlock();

  {
    lock_guard<mutex> lock(m_mutex);
    if (m_revision > m_latest_revision)
      m_latest_revision = m_revision;
  }

  m_schema_mutex.unlock();
}


/**
 * Called before range has been flipped live so no locking needed
 */
void Range::replay_transfer_log(CommitLogReader *commit_log_reader) { BlockHeaderCommitLog header;
  const uint8_t *base, *ptr, *end;
  size_t len;
  ByteString key, value;
  Key key_comps;
  size_t nblocks = 0;
  size_t count = 0;
  TableIdentifier table_id;

  range_inc(ST_replay);

  m_revision = TIMESTAMP_MIN;

  time_debug_using(BEG_PREFIX << "replay transfer" << SUFFIX);

  LogFragmentQueue& queue = commit_log_reader->fragment_queue();
  size_t file = queue.size();
  size_t size = 0;
  for (auto it = queue.begin(); it != queue.end(); it++) {
	  size += (*it)->size;
  }

  int64_t total_len = 0;
  CREATE_TIMER;

  try {

    while (commit_log_reader->next(&base, &len, &header)) {

      ptr = base;
      end = base + len;

      table_id.decode(&ptr, &len);

      if (strcmp(m_table.id, table_id.id))
        HT_THROWF(Error::RANGESERVER_CORRUPT_COMMIT_LOG,
                  "Table name mis-match in split log replay \"%s\" != \"%s\"",
                  m_table.id, table_id.id);

      while (ptr < end) {
        key.ptr = (uint8_t *)ptr;
        key_comps.load(key);
        ptr += key_comps.length;
        value.ptr = (uint8_t *)ptr;
        ptr += value.length();
        add(key_comps, value);
        count++;
      }
      nblocks++;
      total_len += len;
    }

    if (m_revision > m_latest_revision)
      m_latest_revision = m_revision;

    HT_INFOF("Replayed %d updates (%d blocks) from transfer log '%s' into %s",
             (int)count, (int)nblocks, commit_log_reader->get_log_dir().c_str(),
             m_name.c_str());

    m_added_inserts = 0;
    memset(m_added_deletes, 0, 3*sizeof(int64_t));

  }
  catch (Hypertable::Exception &e) {
    HT_ERROR_OUT << "Problem replaying split log - " << e << HT_END;
    if (m_revision > m_latest_revision)
      m_latest_revision = m_revision;
    throw;
  }

  UPDATE_TIMER;
  range_inc(ST_replay_size, total_len);
  range_inc(ST_replay_time, timer.last());

  range_dec(ST_replay);
  time_debug_using(END_PREFIX << "complete replay transfer"
	 << ", record " << count << ", block " << nblocks
	 << ", file count " << file << ", length " << string_size(size)
	 << ", using " << string_timer(timer.last()) << SUFFIX);
}


int64_t Range::get_scan_revision(uint32_t timeout_ms) {

  if (!m_initialized)
    deferred_initialization(timeout_ms);

  lock_guard<mutex> lock(m_mutex);
  return m_latest_revision;
}

void Range::acknowledge_load(uint32_t timeout_ms) {

  if (!m_initialized)
    deferred_initialization(timeout_ms);

  lock_guard<mutex> lock(m_mutex);
  m_metalog_entity->set_load_acknowledged(true);

  if (Global::rsml_writer == 0)
    HT_THROW(Error::SERVER_SHUTTING_DOWN, "Pointer to RSML Writer is NULL");

  HT_MAYBE_FAIL_X("user-range-acknowledge-load-pause-1", !m_table.is_system());
  HT_MAYBE_FAIL_X("user-range-acknowledge-load-1", !m_table.is_system());

  try {
    Global::rsml_writer->record_state(m_metalog_entity);
  }
  catch (Exception &e) {
    m_metalog_entity->set_load_acknowledged(false);
    throw;
  }
}


std::ostream &Hypertable::operator<<(std::ostream &os, const Range::MaintenanceData &mdata) {
  os << "table_id=" << mdata.table_id << "\n";
  os << "scans=" << mdata.load_factors.scans << "\n";
  os << "updates=" << mdata.load_factors.updates << "\n";
  os << "cells_scanned=" << mdata.load_factors.cells_scanned << "\n";
  os << "cells_returned=" << mdata.cells_returned << "\n";
  os << "cells_written=" << mdata.load_factors.cells_written << "\n";
  os << "bytes_scanned=" << mdata.load_factors.bytes_scanned << "\n";
  os << "bytes_returned=" << mdata.bytes_returned << "\n";
  os << "bytes_written=" << mdata.load_factors.bytes_written << "\n";
  os << "disk_bytes_read=" << mdata.load_factors.disk_bytes_read << "\n";
  os << "purgeable_index_memory=" << mdata.purgeable_index_memory << "\n";
  os << "compact_memory=" << mdata.compact_memory << "\n";
  os << "soft_limit=" << mdata.soft_limit << "\n";
  os << "schema_generation=" << mdata.schema_generation << "\n";
  os << "priority=" << mdata.priority << "\n";
  os << "state=" << mdata.state << "\n";
  os << "maintenance_flags=" << mdata.maintenance_flags << "\n";
  os << "file_count=" << mdata.file_count << "\n";
  os << "cell_count=" << mdata.cell_count << "\n";
  os << "memory_used=" << mdata.memory_used << "\n";
  os << "memory_allocated=" << mdata.memory_allocated << "\n";
  os << "key_bytes=" << mdata.key_bytes << "\n";
  os << "value_bytes=" << mdata.value_bytes << "\n";
  os << "compression_ratio=" << mdata.compression_ratio << "\n";
  os << "disk_used=" << mdata.disk_used << "\n";
  os << "disk_estimate=" << mdata.disk_estimate << "\n";
  os << "shadow_cache_memory=" << mdata.shadow_cache_memory << "\n";
  os << "block_index_memory=" << mdata.block_index_memory << "\n";
  os << "bloom_filter_memory=" << mdata.bloom_filter_memory << "\n";
  os << "bloom_filter_accesses=" << mdata.bloom_filter_accesses << "\n";
  os << "bloom_filter_maybes=" << mdata.bloom_filter_maybes << "\n";
  os << "bloom_filter_fps=" << mdata.bloom_filter_fps << "\n";
  os << "busy=" << (mdata.busy ? "true" : "false") << "\n";
  os << "is_metadata=" << (mdata.is_metadata ? "true" : "false") << "\n";
  os << "is_system=" << (mdata.is_system ? "true" : "false") << "\n";
  os << "relinquish=" << (mdata.relinquish ? "true" : "false") << "\n";
  os << "needs_major_compaction=" << (mdata.needs_major_compaction ? "true" : "false") << "\n";
  os << "needs_split=" << (mdata.needs_split ? "true" : "false") << "\n";
  os << "load_acknowledged=" << (mdata.load_acknowledged ? "true" : "false") << "\n";
  os << "unsplittable=" << (mdata.unsplittable ? "true" : "false") << "\n";
  return os;
}
