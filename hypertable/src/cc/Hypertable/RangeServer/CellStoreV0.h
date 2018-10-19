/* -*- c++ -*-
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

#ifndef Hypertable_RangeServer_CellStoreV0_h
#define Hypertable_RangeServer_CellStoreV0_h

#include <map>
#include <string>
#include <vector>

#include "CellStoreBlockIndexArray.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/DynamicBuffer.h"
#include "Common/BloomFilter.h"
#include "Common/BlobHashSet.h"
#include "Common/Filesystem.h"

#include "Hypertable/Lib/BlockCompressionCodec.h"
#include "Hypertable/Lib/SerializedKey.h"

#include "CellStore.h"
#include "CellStoreTrailerV0.h"


/**
 * Forward declarations
 */
namespace Hypertable {
  class BlockCompressionCodec;
  class Client;
  class Protocol;
}

namespace Hypertable {

  class CellStoreV0 : public CellStore {

  public:
    CellStoreV0(Filesystem *filesys);
    virtual ~CellStoreV0();

    void create(const char *fname, size_t max_entries, PropertiesPtr &props,
                const TableIdentifier *table_id=0) override;
    void add(const Key &key, const ByteString value) override;
    void finalize(TableIdentifier *table_identifier) override;
    void open(const String &fname, const String &start_row,
              const String &end_row, int32_t fd, int64_t file_length,
              CellStoreTrailer *trailer) override;
    int64_t get_blocksize() override { return m_trailer.blocksize; }
    bool may_contain(const void *ptr, size_t len);
    bool may_contain(const String &key) {
      return may_contain(key.data(), key.size());
    }

    bool may_contain(ScanContext *scan_ctx) override;

    uint64_t disk_usage() override {
      if (m_disk_usage < 0)
        HT_WARN_OUT << "[Issue 339] Disk usage for " << m_filename << "=" << m_disk_usage
                    << HT_END;
      return m_disk_usage;
    }
    float compression_ratio() override { return m_trailer.compression_ratio; }
    int64_t get_total_entries() override { return m_trailer.total_entries; }
    std::string &get_filename() override { return m_filename; }
    int get_file_id() override { return m_file_id; }
    CellListScannerPtr create_scanner(ScanContext *scan_ctx) override;
    BlockCompressionCodec *create_block_compression_codec() override;
    void display_block_info() override;
    int64_t end_of_last_block() override { return m_trailer.fix_index_offset; }
    size_t bloom_filter_size() override { return m_bloom_filter ? m_bloom_filter->size() : 0; }
    int64_t bloom_filter_memory_used() override { return 0; }
    int64_t block_index_memory_used() override { return 0; }
    uint64_t purge_indexes() override { return 0; }
    bool restricted_range() override { return true; }

    int32_t get_fd() override {
      std::lock_guard<std::mutex> lock(m_mutex);
      return m_fd;
    }

    int32_t reopen_fd() override {
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_fd != -1)
        m_filesys->close(m_fd);
      m_fd = m_filesys->open(m_filename, 0);
      return m_fd;
    }

    CellStoreTrailer *get_trailer() override { return &m_trailer; }

    uint16_t block_header_format() override;

  protected:
    void add_index_entry(const SerializedKey key, uint32_t offset);
    void create_bloom_filter(bool is_approx = false);
    void load_index();

    typedef std::map<SerializedKey, uint32_t> IndexMap;
    typedef BlobHashSet<> BloomFilterItems;

    Filesystem            *m_filesys;
    int32_t                m_fd;
    std::string            m_filename;
    IndexMap               m_index;
    CellStoreBlockIndexArray<uint32_t> m_index_map32;
    CellStoreTrailerV0     m_trailer;
    BlockCompressionCodec *m_compressor;
    DynamicBuffer          m_buffer;
    DynamicBuffer          m_fix_index_buffer;
    DynamicBuffer          m_var_index_buffer;
    uint32_t               m_memory_consumed;
    DispatchHandlerSynchronizer  m_sync_handler;
    uint32_t               m_outstanding_appends;
    uint32_t               m_offset;
    ByteString             m_last_key;
    int64_t                m_file_length;
    int64_t                m_disk_usage;
    int                    m_file_id;
    float                  m_uncompressed_data;
    float                  m_compressed_data;
    uint32_t               m_uncompressed_blocksize;
    BlockCompressionCodec::Args m_compressor_args;
    size_t                 m_max_entries;

    BloomFilterMode        m_bloom_filter_mode;
    BloomFilter           *m_bloom_filter;
    BloomFilterItems      *m_bloom_filter_items;
    uint32_t               m_max_approx_items;
  };

}

#endif // Hypertable_RangeServer_CellStoreV0_h
