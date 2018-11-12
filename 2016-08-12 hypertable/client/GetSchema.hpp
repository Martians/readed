
#ifndef _HYPER_SCHEMA_H
#define _HYPER_SCHEMA_H

#include <Common/Compat.h>
#include <Common/Logger.h>
#include <Common/System.h>

#include <Hypertable/Lib/Key.h>
#include <Hypertable/Lib/KeySpec.h>

#include <ThriftBroker/Client.h>
#include <ThriftBroker/gen-cpp/Client_types.h>
#include <ThriftBroker/gen-cpp/HqlService.h>
#include <ThriftBroker/ThriftHelper.h>
#include <ThriftBroker/SerializedCellsReader.h>
#include <ThriftBroker/SerializedCellsWriter.h>

#include <vector>
#include <string>

using namespace Hypertable;
using namespace Hypertable::ThriftGen;
using namespace std;

namespace Hyper {
}

#endif //_HYPER_SCHEMA_H
