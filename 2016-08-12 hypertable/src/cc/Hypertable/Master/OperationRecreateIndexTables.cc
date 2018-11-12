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
/// Definitions for OperationRecreateIndexTables.
/// This file contains definitions for OperationRecreateIndexTables, an
/// Operation class for reconstructing a table's index tables.

#include <Common/Compat.h>
#include "OperationRecreateIndexTables.h"

#include <Hypertable/Master/OperationCreateTable.h>
#include <Hypertable/Master/OperationDropTable.h>
#include <Hypertable/Master/OperationToggleTableMaintenance.h>
#include <Hypertable/Master/ReferenceManager.h>
#include <Hypertable/Master/Utility.h>

#include <Hypertable/Lib/Key.h>

#include <Hyperspace/Session.h>

#include <Common/Error.h>
#include <Common/FailureInducer.h>
#include <Common/ScopeGuard.h>
#include <Common/Serialization.h>

#include <boost/algorithm/string.hpp>

#include <poll.h>

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

OperationRecreateIndexTables::OperationRecreateIndexTables(ContextPtr &context,
                                                           std::string name,
                                                           TableParts parts) :
  Operation(context, MetaLog::EntityType::OPERATION_RECREATE_INDEX_TABLES),
  m_params(name, parts) {
  m_exclusivities.insert(m_params.name());
}


OperationRecreateIndexTables::OperationRecreateIndexTables(ContextPtr &context,
                                                           const MetaLog::EntityHeader &header)
  : Operation(context, header) {
}

OperationRecreateIndexTables::OperationRecreateIndexTables(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_RECREATE_INDEX_TABLES) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  m_params.decode(&ptr, &remaining);
  m_exclusivities.insert(m_params.name());
}

/// @detail
/// This method carries out the operation via the following states:
///
/// <table>
/// <tr>
/// <th>State</th>
/// <th>Description</th>
/// </tr>
/// <tr>
/// <td>INITIAL</td>
/// <td><ul>
/// <li>Reads table schema from Hyperspace</li>
/// <li>Combines information about existing index tables with the specified
///     index tables passed in via #m_parts and stores the result to
///     #m_parts</li>
/// <li>If no index tables are specified after previous step,
///     complete_error() is called with Error::NOTHING_TO_DO</li>
/// <li>State is transitioned to SUSPEND_TABLE_MAINTENANCE</li>
/// <li>Persists operation to MML and drops through</li>
/// </ul></td>
/// </tr>
/// <tr>
/// <td>SUSPEND_TABLE_MAINTENANCE</td>
/// <td><ul>
/// <li>Creates OperationToggleMaintenance sub operation to turn maintenance
///     off</li>
/// <li>Stages sub operation with a call to stage_subop()</li>
/// <li>Transition state to DROP_INDICES</li>
/// <li>Persists operation with a call to record_state() and returns</li>
/// </ul></td>
/// </tr>
/// <tr>
/// <td>DROP_INDICES</td>
/// <td><ul>
/// <li>Handles result of toggle maintenance sub operation with a call to
///     validate_subops(), returning on failure</li>
/// <li>Creates OperationDropTable sub operation to drop index tables</li>
/// <li>Stages sub operation with a call to stage_subop()</li>
/// <li>Transition state to CREATE_INDICES</li>
/// <li>Persists operation with a call to record_state() and returns</li>
/// </ul></td>
/// </tr>
/// <tr>
/// <td>CREATE_INDICES</td>
/// <td><ul>
/// <li>Handles result of drop table sub operation with a call to
///     validate_subops(), returning on failure</li>
/// <li>Fetches schema from Hyperspace and creates an OperationCreateTable
///     sub operation to create index tables</li>
/// <li>Stages sub operation with a call to stage_subop()</li>
/// <li>Transition state to RESUME_TABLE_MAINTENANCE</li>
/// <li>Persists operation with a call to record_state() and returns</li>
/// </ul></td>
/// </tr>
/// <tr>
/// <td>RESUME_TABLE_MAINTENANCE</td>
/// <td><ul>
/// <li>Handles result of create table sub operation with a call to
///     validate_subops(), returning on failure</li>
/// <li>Creates OperationToggleMaintenance sub operation to turn maintenance
///     back on</li>
/// <li>Stages sub operation with a call to stage_subop()</li>
/// <li>Transition state to FINALIZE</li>
/// <li>Persists operation with a call to record_state() and returns</li>
/// </ul></td>
/// </tr>
/// <tr>
/// <td>FINALIZE</td>
/// <td><ul>
/// <li>Handles result of toggle table maintenance sub operation with a call
///     to validate_subops(), returning on failure</li>
/// <li>%Operation is completed with a call to complete_ok()</li>
/// </ul></td>
/// </tr>
/// </table>
void OperationRecreateIndexTables::execute() {
  std::vector<Entity *> entities;
  int32_t state = get_state();

  HT_INFOF("Entering RecreateIndexTables-%lld (table=%s, parts=%s) state=%s",
           (Lld)header.id, m_params.name().c_str(),
           m_params.parts().to_string().c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    {
      string schema_str;
      if (!fetch_schema(schema_str))
        break;
      SchemaPtr schema( Schema::new_instance(schema_str) );
      uint8_t parts = 0;
      for (auto cf_spec : schema->get_column_families()) {
        if (m_params.parts().value_index() && cf_spec->get_value_index())
          parts |= TableParts::VALUE_INDEX;
        if (m_params.parts().qualifier_index() && cf_spec->get_qualifier_index())
          parts |= TableParts::QUALIFIER_INDEX;
      }
      if (parts == 0) {
        complete_error(Error::NOTHING_TO_DO, m_params.name());
        break;
      }
      m_parts = TableParts(parts);
    }
    set_state(OperationState::SUSPEND_TABLE_MAINTENANCE);
    m_context->mml_writer->record_state(shared_from_this());
    HT_MAYBE_FAIL("recreate-index-tables-INITIAL");

    // drop through ...

  case OperationState::SUSPEND_TABLE_MAINTENANCE:
    stage_subop(make_shared<OperationToggleTableMaintenance>(m_context, m_params.name(),
                                                             TableMaintenance::OFF));
    set_state(OperationState::DROP_INDICES);
    record_state();
    break;

  case OperationState::DROP_INDICES:
    if (!validate_subops())
      break;
    stage_subop(make_shared<OperationDropTable>(m_context, m_params.name(), true, m_parts));
    set_state(OperationState::CREATE_INDICES);
    record_state();
    break;

  case OperationState::CREATE_INDICES:
    if (!validate_subops())
      break;

    if (m_parts) {
      string schema;
      if (!fetch_schema(schema))
        break;
      stage_subop(make_shared<OperationCreateTable>(m_context, m_params.name(), schema, m_parts));
    }
    set_state(OperationState::RESUME_TABLE_MAINTENANCE);
    record_state();
    break;

  case OperationState::RESUME_TABLE_MAINTENANCE:

    if (!validate_subops())
      break;

    stage_subop(make_shared<OperationToggleTableMaintenance>(m_context, m_params.name(),
                                                             TableMaintenance::ON));
    set_state(OperationState::FINALIZE);
    HT_MAYBE_FAIL("recreate-index-tables-RESUME_TABLE_MAINTENANCE-a");
    record_state();
    HT_MAYBE_FAIL("recreate-index-tables-RESUME_TABLE_MAINTENANCE-b");
    break;

  case OperationState::FINALIZE:
    if (!validate_subops())
      break;
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving RecreateIndexTables-%lld (table=%s, parts=%s) state=%s",
           (Lld)header.id, m_params.name().c_str(),
           m_params.parts().to_string().c_str(),
           OperationState::get_text(get_state()));
}

void OperationRecreateIndexTables::display_state(std::ostream &os) {
  os << " table_name=" << m_params.name()
     << " parts=" << m_params.parts().to_string();
}

uint8_t OperationRecreateIndexTables::encoding_version_state() const {
  return 1;
}

size_t OperationRecreateIndexTables::encoded_length_state() const {
  return m_params.encoded_length() + m_parts.encoded_length();
}

void OperationRecreateIndexTables::encode_state(uint8_t **bufp) const {
  m_params.encode(bufp);
  m_parts.encode(bufp);
}

void OperationRecreateIndexTables::decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  m_params.decode(bufp, remainp);
  m_parts.decode(bufp, remainp);
}

void OperationRecreateIndexTables::decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) {
  string name = Serialization::decode_vstr(bufp, remainp);
  int8_t parts = (int8_t)Serialization::decode_byte(bufp, remainp);
  m_parts = TableParts(parts);
  m_params = Lib::Master::Request::Parameters::RecreateIndexTables(name, m_parts);
}

const String OperationRecreateIndexTables::name() {
  return "OperationRecreateIndexTables";
}

const String OperationRecreateIndexTables::label() {
  return format("Recreate Index Tables (table=%s, parts=%s)",
                m_params.name().c_str(), m_params.parts().to_string().c_str());
}

bool OperationRecreateIndexTables::fetch_schema(std::string &schema) {
  string table_id;
  bool is_namespace;
  if (m_context->namemap->name_to_id(m_params.name(), table_id, &is_namespace)) {
    if (is_namespace) {
      complete_error(Error::TABLE_NOT_FOUND, format("%s is a namespace", m_params.name().c_str()));
      return false;
    }
  }
  else {
    complete_error(Error::TABLE_NOT_FOUND, m_params.name());
    return false;
  }
  DynamicBuffer value_buf;
  string filename = m_context->toplevel_dir + "/tables/" + table_id;
  m_context->hyperspace->attr_get(filename, "schema", value_buf);
  schema = string((char *)value_buf.base, strlen((char *)value_buf.base));
  return true;
}

