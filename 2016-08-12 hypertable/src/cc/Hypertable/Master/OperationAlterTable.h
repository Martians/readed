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
/// Declarations for OperationAlterTable.
/// This file contains declarations for OperationAlterTable, an Operation class
/// for carrying out an <i>alter table</i> operation.

#ifndef Hypertable_Master_OperationAlterTable_h
#define Hypertable_Master_OperationAlterTable_h

#include <Hypertable/Master/Operation.h>

#include <Hypertable/Lib/Master/Request/Parameters/AlterTable.h>

#include <Common/StringExt.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out an alter table operation.
  class OperationAlterTable : public Operation {
  public:

    /** Constructor for constructing object from %MetaLog entry.
     * @param context %Master context
     * @param header_ %MetaLog header
     */
    OperationAlterTable(ContextPtr &context, const MetaLog::EntityHeader &header_);
    
    /** Constructor for constructing object from AsyncComm event 
     * Decodes #m_params from message payload and then initializes the
     * dependency graph state as follows:
     *   - <b>dependencies</b> - Dependency::INIT
     *   - <b>exclusivities</b> - %Table pathname
     * @param context %Master context
     * @param event %Event received from AsyncComm from client request
     */
    OperationAlterTable(ContextPtr &context, EventPtr &event);

    /** Destructor. */
    virtual ~OperationAlterTable() { }

    /** Carries out the alter table operation. */
    void execute() override;

    /** Returns name of operation ("OperationAlterTable")
     * @return %Operation name
     */
    const String name() override;

    /** Returns descriptive label for operation
     * @return Descriptive label for operation
     */
    const String label() override;

    /** Writes human readable representation of object to output stream.
     * @param os Output stream
     */
    void display_state(std::ostream &os) override;

    /// Returns encoding version
    /// @return Encoding version
    uint8_t encoding_version_state() const override;

    /** Returns serialized state length.
     * This method returns the length of the serialized representation of the
     * object state.  See encode() for a description of the serialized format.
     * @return Serialized length
     */
    size_t encoded_length_state() const override;

    /** Writes serialized encoding of object state.
     * This method writes a serialized encoding of object state to the memory
     * location pointed to by <code>*bufp</code>.  The encoding has the
     * following format:
     * <table>
     *   <tr><th> Encoding </th><th> Description </th></tr>
     *   <tr><td> vstr </td><td> Pathname of table to compact </td></tr>
     *   <tr><td> vstr </td><td> New table schema </td></tr>
     *   <tr><td> vstr </td><td> %Table identifier </td></tr>
     *   <tr><td> i32  </td><td> Size of #m_completed </td></tr>
     *   <tr><td> vstr </td><td> <b>Foreach server</b> in #m_completed, server
     *                           name </td></tr>
     *   <tr><td> i32  </td><td> [VERSION 2] Size of #m_servers </td></tr>
     *   <tr><td> vstr </td><td> [VERSION 2] <b>Foreach server</b>
     *                           in #m_servers, server name </td></tr>
     *   <tr><td> TableParts </td><td> [VERSION 3] Index tables to be created
     *                                 or dropped </td></tr>
     * </table>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    void encode_state(uint8_t **bufp) const override;

    /** Reads serialized encoding of object state.
     * This method restores the state of the object by decoding a serialized
     * representation of the state from the memory location pointed to by
     * <code>*bufp</code>.  See encode() for a description of the
     * serialized format.
     * @param version Encoding version
     * @param bufp Address of source buffer pointer (advanced by call)
     * @param remainp Amount of remaining buffer pointed to by <code>*bufp</code>
     * (decremented by call)
     */
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

  private:

    /// Gets schema objects.
    /// If either <code>original_schema</code> and <code>alter_schema</code> are
    /// are set to nullptr, this member function constructs and returns original
    /// schema object and alter schema object.  The original schema is
    /// constructed from XML read from Hyperspace and the alter schema is
    /// constructed from #m_schema.  If any errors are encountered,
    /// the operation is terminated with a call to complete_error() and
    //// <i>false</i> is returned.
    /// @param original_schema Reference to original schema
    /// @param alter_schema Reference to alter schema
    /// @return <i>true</i> on success, <i>false</i> on error signaling operaton
    /// has been terminated.
    bool get_schemas(SchemaPtr &original_schema, SchemaPtr &alter_schema);

    /// Determines which index tables to drop.
    /// Determines if either the value or qualifier index table was required in
    /// <code>original_schema</code> but not in <code>alter_schema</code>.  Each
    /// index type, for which this is the case, is marked in a TableParts object
    /// that is returned.
    /// @param original_schema Original schema
    /// @param alter_schema New altered schema
    /// @return TableParts describing index tables to drop
    TableParts get_drop_index_parts(SchemaPtr &original_schema,
                                    SchemaPtr &alter_schema);

    /// Determines which index tables to create.
    /// Determines if either the value or qualifier index table was not required
    /// in <code>original_schema</code> but is required in
    /// <code>alter_schema</code>.  Each index type, for which this is the case,
    /// is marked in a TableParts object that is returned.
    /// @param original_schema Original schema
    /// @param alter_schema New altered schema
    /// @return TableParts describing index tables to create
    TableParts get_create_index_parts(SchemaPtr &original_schema,
                                      SchemaPtr &alter_schema);

    /// Request parmaeters
    Lib::Master::Request::Parameters::AlterTable m_params;

    /// %Schema for the table
    String m_schema;

    /// %Table identifier
    String m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// Set of range servers that have completed operation
    StringSet m_completed;

    /// Index tables to be created or dropped
    TableParts m_parts;

  };

  /// @}

} // namespace Hypertable

#endif // Hypertable_Master_OperationAlterTable_h
