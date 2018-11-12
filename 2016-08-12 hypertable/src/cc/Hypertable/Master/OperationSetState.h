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
 * Declarations for OperationSetState.
 * This file contains declarations for OperationSetState, an Operation class
 * for setting system state variables.
 */

#ifndef Hypertable_Master_OperationSetState_h
#define Hypertable_Master_OperationSetState_h

#include "Operation.h"

#include <Hypertable/Lib/Master/Request/Parameters/SetState.h>
#include <Hypertable/Lib/SystemVariable.h>

#include <Common/StringExt.h>

namespace Hypertable {

  /// @addtogroup Master
  /// @{

  /// Carries out a set variables operation
  class OperationSetState : public Operation {
  public:

    /** Constructor.
     * This constructor is used to create a SetState operation that is the
     * result of an automated condition change.  Creating an object with
     * this constructor should be done after SystemState::auto_set() is
     * called and returns <i>true</i>.  Sets exclusivity "SET VARIABLES".
     * @param context %Master context
     */
    OperationSetState(ContextPtr &context);

    /** Constructor.
     * This constructor is used to create a SetState operation from a Metalog
     * (MML) entry.
     * @param context %Master context
     * @param header_ MML entity header
     */
    OperationSetState(ContextPtr &context, const MetaLog::EntityHeader &header_);

    /** Constructor.
     * This constructor is used to create a SetState operation from a client
     * request event.  It decodes the request, which populates the #m_specs
     * vector, and then calls SystemState::admin_set() passing in #m_specs.
     * Sets exclusivity "SET VARIABLES".
     * @param context %Master context
     * @param event Reference to event object
     */
    OperationSetState(ContextPtr &context, EventPtr &event);

    /** Destructor. */
    virtual ~OperationSetState() { }

    /** Executes "set state" operation.
     */
    void execute() override;
    
    /** Returns operation name (OperationSetState)
     * @return Operation name
     */
    const String name() override;

    /** Returns textual string describing operation plus state
     * @return String label
     */
    const String label() override;

    /** Displays textual representation of object state.
     * This method prints each variable and value in #m_specs
     * vector.
     */
    void display_state(std::ostream &os) override;

    uint8_t encoding_version_state() const override;

    /** Returns length of encoded state. */
    size_t encoded_length_state() const override;

    /** Encodes operation state.
     * This method encodes the operation state formatted as follows:
     * <pre>
     *   Format version number
     *   System state generation number
     *   Number of variable specs to follow
     *   foreach variable spec {
     *     variable code
     *     variable value
     *   }
     *   Client originated flag
     * </pre>
     * @param bufp Address of destination buffer pointer (advanced by call)
     */
    void encode_state(uint8_t **bufp) const override;

    /** Decodes operation state.
     * See encode_state() for format description.
     * @param version Encoding version
     * @param bufp Address of destination buffer pointer (advanced by call)
     * @param remainp Address of integer holding amount of remaining buffer
     */
    void decode_state(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

    void decode_state_old(uint8_t version, const uint8_t **bufp, size_t *remainp) override;

  private:

    /** Initializes operation dependencies.
     * This method initializes the operation dependencies which include:
     *
     *   - exclusivity: "SET VARIABLES"
     */
    void initialize_dependencies();

    /// Request parmaeters
    Lib::Master::Request::Parameters::SetState m_params;

    /// Generation number of system state variables (#m_specs)a
    uint64_t m_generation;

    /// Current system state variables
    std::vector<SystemVariable::Spec> m_specs;
  };

  /// @}

}

#endif // Hypertable_Master_OperationSetState_h
