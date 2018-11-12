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
/// Declarations for OperationDropTable.
/// This file contains declarations for OperationDropTable, an Operation class
/// for dropping (removing) a table from the system.

#ifndef Hypertable_Master_OperationDropTable_h
#define Hypertable_Master_OperationDropTable_h

#include <Hypertable/Master/Operation.h>

#include <Hypertable/Lib/Master/Request/Parameters/DropTable.h>
#include <Hypertable/Lib/TableParts.h>

#include <Common/StringExt.h>

#include <vector>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out a drop table operation.
  class OperationDropTable : public Operation {
  public:

    /// Constructor.
    /// Initializes object by passing MetaLog::EntityType::OPERATION_DROP_TABLE
    /// to parent Operation constructor and initializes #m_params with
    /// <code>name</code> and <code>if_exists</code>.  Lastly, it initializes
    /// the dependency graph state as follows:
    ///   - <b>dependencies</b> - INIT
    ///   - <b>exclusivities</b> - %Table pathname
    /// initialization with a call to initialize_dependencies().
    /// @param context %Master context
    /// @param name Pathname of table to drop
    /// @param if_exists Flag indicating if operation should succeed if table
    /// does not exist
    /// @param parts Which parts of the table to drop
    OperationDropTable(ContextPtr &context, const String &name, bool if_exists,
                       TableParts parts);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header_ %MetaLog header
    OperationDropTable(ContextPtr &context, const MetaLog::EntityHeader &header_);

    /// Constructor with client request.
    /// @param context %Master context
    /// @param event %Event received from AsyncComm from client request
    OperationDropTable(ContextPtr &context, EventPtr &event);

    /// Destructor.
    virtual ~OperationDropTable() { }

    /// Carries out the drop table operation.
    void execute() override;

    /// Returns name of operation ("OperationDropTable")
    /// @return %Operation name
    const String name() override;

    /// Returns descriptive label for operation.
    /// This method returns a descriptive label for the operation which
    /// is constructed as the string "DropTable " followed by the name of the
    /// table being dropped.
    /// @return Descriptive label for operation
    const String label() override;

    /// Writes human readable representation of object to output stream.
    /// The string returned has the following format:
    /// <pre>
    ///  name=&lt;TableName&gt; id=&lt;TableId&gt;
    /// </pre>
    /// @param os Output stream
    void display_state(std::ostream &os) override;

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
    ///   <td>bool</td><td><i>if exists</i> flag</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table identifier</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Size of #m_completed</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td><b>foreach</b> server in #m_completed, server name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>[VERSION 2] Size of #m_servers</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>[VERSION 2] <b>foreach</b> server in #m_servers, server name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>TableParts</td><td>[VERSION 3] %Table parts to create (#m_parts)
    ///   </td>
    ///   </tr>
    /// </table>
    /// @param bufp Address of destination buffer pointer (advanced by call)
    void encode_state(uint8_t **bufp) const override;

    /// Decodes state from serialized object.
    /// This method restores the state of the object by decoding a serialized
    /// representation of the state from the memory location pointed to by
    /// <code>*bufp</code>.
    /// @param version Encoding version
    /// @param bufp Address of source buffer pointer (advanced by call)
    /// @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
    /// (decremented by call)
    /// @see encode() for a description of the serialized %format
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

  private:

    /// Request parmaeters
    Lib::Master::Request::Parameters::DropTable m_params;

    /// %Table ID string
    String m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// %Range servers for which drop table operation has completed successfuly
    StringSet m_completed;

    /// Specification for which parts of table to drop
    TableParts m_parts {TableParts::ALL};
  };

  /// @}

}

#endif // Hypertable_Master_OperationDropTable_h
