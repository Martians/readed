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

/// @file
/// Declarations for OperationRecreateIndexTables.
/// This file contains declarations for OperationRecreateIndexTables, an
/// Operation class for reconstructing a table's index tables.

#ifndef Hypertable_Master_OperationRecreateIndexTables_h
#define Hypertable_Master_OperationRecreateIndexTables_h

#include "Operation.h"

#include <Hypertable/Lib/Master/Request/Parameters/RecreateIndexTables.h>
#include <Hypertable/Lib/TableParts.h>

#include <Common/String.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Reconstructs a table's index tables.
  class OperationRecreateIndexTables : public Operation {
  public:

    /// Constructor.
    /// Initializes object by passing
    /// MetaLog::EntityType::OPERATION_RECREATE_INDEX_TABLES to parent Operation
    /// constructor and then initializes #m_params with <code>name</code> and
    /// <code>parts</code>.  Adds <code>name</code> as an exclusivity.
    /// @param context %Master context
    /// @param name Pathname of table
    /// @param parts Describes which index tables to re-create
    OperationRecreateIndexTables(ContextPtr &context, std::string name,
                                 TableParts parts);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header %MetaLog header
    OperationRecreateIndexTables(ContextPtr &context,
                                 const MetaLog::EntityHeader &header);

    /// Constructor with client request.
    /// Initializes base class constructor, decodes request parameters and adds
    /// table name as an exclusivity.
    /// @param context %Master context
    /// @param event %Event received from AsyncComm from client request
    OperationRecreateIndexTables(ContextPtr &context, EventPtr &event);
    
    /// Destructor.
    virtual ~OperationRecreateIndexTables() { }

    /// Carries out recreate index tables operation.
    void execute() override;

    /// Returns name of operation.
    /// Returns name of operation "OperationRecreateIndexTables"
    /// @return %Operation name
    const String name() override;

    /// Returns descriptive label for operation
    /// @return Descriptive label for operation
    const String label() override;

    /// Writes human readable representation of object to output stream.
    /// @param os Output stream
    void display_state(std::ostream &os) override;

    /// Returns encoding version of serialization format.
    /// @return Encoding version of serialization format.
    uint8_t encoding_version_state() const override;

    /// Returns serialized state length.
    /// This method returns the length of the serialized representation of the
    /// object state.
    /// @return Serialized length.
    /// @see encode() for a description of the serialized %format.
    size_t encoded_length_state() const override;

    /// Writes serialized encoding of object state.
    /// This method writes a serialized encoding of object state to the memory
    /// location pointed to by <code>*bufp</code>.  The encoding has the
    /// following format:
    /// <table>
    ///   <tr>
    ///   <th>Encoding</th><th>Description</th>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>Specification for which index tables to
    ///       re-create (#m_parts)</td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_state(uint8_t **bufp) const override;

    /// Reads serialized encoding of object state.
    /// This method restores the state of the object by decoding a serialized
    /// representation of the state from the memory location pointed to by
    /// <code>*bufp</code>.
    /// @param version Encoding version
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    /// @see encode() for a description of the serialized %format.
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

  private:

    bool fetch_schema(std::string &schema);

    /// Request parmaeters
    Lib::Master::Request::Parameters::RecreateIndexTables m_params;

    /// Table parts to recreate
    TableParts m_parts {0};

  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_OperationRecreateIndexTables_h
