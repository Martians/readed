/** -*- c++ -*-
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

#include "Common/Compat.h"
#include <iostream>

#include "CellStoreReleaseCallback.h"
#include "AccessGroup.h"

using namespace Hypertable;
using namespace std;

/**
 */
CellStoreReleaseCallback::CellStoreReleaseCallback(AccessGroup *ag)
  : m_access_group(ag) {
}

void CellStoreReleaseCallback::operator()() const {
  m_access_group->release_files(m_filenames);
}
