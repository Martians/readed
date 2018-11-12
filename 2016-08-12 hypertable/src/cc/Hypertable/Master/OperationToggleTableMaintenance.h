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
/// Declarations for OperationToggleTableMaintenance.
/// This file contains declarations for OperationToggleTableMaintenance, an
/// Operation class for toggling maintenance for a table either on or off.

#ifndef Hypertable_Master_OperationToggleTableMaintenance_h
#define Hypertable_Master_OperationToggleTableMaintenance_h

#include "Operation.h"

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// %Table maintenance constants
  namespace TableMaintenance {
    /// Constant representing <i>off</i>
    const bool OFF = false;
    /// Constant representing <i>on</i>
    const bool ON = true;
  }

  /// Enables or disables maintenance for a table.
  class OperationToggleTableMaintenance : public Operation {
  public:

    /// Constructor.
    /// Initializes object by passing
    /// MetaLog::EntityType::OPERATION_TOGGLE_TABLE_MAINTENANCE to parent Operation
    /// constructor, sets #m_name to canonicalized <code>table_name</code>, and
    /// adds INIT as a dependency.
    /// @param context %Master context
    /// @param table_name %Table pathname
    /// @param toggle_on Flag indicating if maintenance is to be turned on or off
    OperationToggleTableMaintenance(ContextPtr &context,
                                    const std::string &table_name,
                                    bool toggle_on);

    /// Constructor with MML entity.
    /// @param context %Master context
    /// @param header %MetaLog header
    OperationToggleTableMaintenance(ContextPtr &context,
                                    const MetaLog::EntityHeader &header);
    
    /// Destructor.
    virtual ~OperationToggleTableMaintenance() { }

    /// Carries out a toggle maintenance operation.
    void execute() override;

    /// Returns name of operation ("OperationToggleTableMaintenance")
    /// @return %Operation name
    const String name() override;

    /// Returns descriptive label for operation
    /// @return Descriptive label for operation
    const String label() override;

    /// Writes human readable representation of object to output stream.
    /// @param os Output stream
    void display_state(std::ostream &os) override;

    uint8_t encoding_version_state() const override;

    /// Returns serialized state length.
    /// This method returns the length of the serialized representation of the
    /// object state.
    /// @return Serialized length
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
    ///   <td>vstr</td><td>%Table name (#m_name)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td>%Table ID (#m_id)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>bool</td><td>Flag indicating if maintenance is to be enabled or
    ///                    disabled (#m_toggle_on)</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Size of #m_servers</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td><b>Foreach server</b> in #m_servers, server name</td>
    ///   </tr>
    ///   <tr>
    ///   <td>i32</td><td>Size of #m_completed</td>
    ///   </tr>
    ///   <tr>
    ///   <td>vstr</td><td><b>Foreach server</b> in #m_completed, server name</td>
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

    /// %Table pathname
    std::string m_name;

    /// %Table ID
    std::string m_id;

    /// Set of range servers participating in toggle
    std::set<std::string> m_servers;

    /// Set of range servers that have completed toggle
    std::set<std::string> m_completed;

    /// Flag indicating if maintenance is to be toggled on or off
    bool m_toggle_on {};
  };

  /// @}

}

#endif // Hypertable_Master_OperationToggleTableMaintenance_h
