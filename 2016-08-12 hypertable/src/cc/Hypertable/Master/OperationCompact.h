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

/** @file
 * Declarations for OperationCompact.
 * This file contains declarations for OperationCompact, an Operation class
 * for carrying out a manual compaction operation.
 */

#ifndef Hypertable_Master_OperationCompact_h
#define Hypertable_Master_OperationCompact_h

#include "Operation.h"

#include <Hypertable/Lib/Master/Request/Parameters/Compact.h>

#include <Common/StringExt.h>

#include <string>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out a manual compaction operation.
  class OperationCompact : public Operation {
  public:

    /// Constructor for constructing object from %MetaLog entry.
    /// @param context %Master context
    /// @param header_ %MetaLog header
    OperationCompact(ContextPtr &context, const MetaLog::EntityHeader &header_);
    
    /// Constructor for constructing object from AsyncComm event.
    /// Decodes #m_params from message payload, then initializes the dependency
    /// graph state as follows:
    ///   - <b>dependencies</b> - Dependency::INIT
    ///   - <b>exclusivities</b> - %Table pathname
    /// @param context %Master context
    /// @param event %Event received from AsyncComm from client request
    OperationCompact(ContextPtr &context, EventPtr &event);

    /// Destructor.
    virtual ~OperationCompact() { }

    /** Carries out the manual compaction operation. */
    void execute() override;

    /** Returns name of operation ("OperationCompact")
     * @return %Operation name
     */
    const std::string name() override;

    /** Returns descriptive label for operation
     * @return Descriptive label for operation
     */
    const std::string label() override;

    /** Writes human readable representation of object to output stream.
     * @param os Output stream
     */
    void display_state(std::ostream &os) override;

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
     *   <tr>
     *   <th>Encoding</th><th>Description</th>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>Pathname of table to compact</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>Row key of range to compact</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>Compaction flags (see RangeServerProtocol::CompactionFlags)</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>%Table identifier</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>Size of #m_completed</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td><b>Foreach server</b> in #m_completed, server name</td>
     *   </tr>
     *   <tr>
     *   <td>i32</td><td>[VERSION 2] Size of #m_servers</td>
     *   </tr>
     *   <tr>
     *   <td>vstr</td><td>[VERSION 2] <b>Foreach server</b> in #m_servers, server name</td>
     *   </tr>
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

    /** Initializes dependency graph state.
     * This method initializes the dependency graph state as follows:
     *
     *   - <b>dependencies</b> - Dependency::INIT
     *   - <b>exclusivities</b> - %Table pathname
     */
    void initialize_dependencies();

    /// Request parmaeters
    Lib::Master::Request::Parameters::Compact m_params;

    /// %Table identifier
    std::string m_id;

    /// Set of participating range servers
    StringSet m_servers;

    /// Set of range servers that have completed operation
    StringSet m_completed;
  };

  /// @}

}

#endif // Hypertable_Master_OperationCompact_h
