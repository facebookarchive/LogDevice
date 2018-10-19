/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/clients/python/util/util.h"
#include "logdevice/include/Client.h"

using namespace boost::python;
using namespace facebook::logdevice;

namespace {
uint64_t extract_data_logid(DataRecord const& record) {
  return record.logid.val();
}
uint64_t extract_gap_logid(GapRecord const& record) {
  return record.logid.val();
}

lsn_t extract_lsn(DataRecord const& record) {
  return record.attrs.lsn;
}

double extract_timestamp(DataRecord const& record) {
  using namespace std::chrono;
  return duration_cast<duration<double>>(record.attrs.timestamp).count();
}

/**
 * Return the payload of the DataRecord, as a Python (byte) string;
 * since NULL bytes don't terminate the Python string, this can also
 * transport binary data back to the Python layer.
 */
object extract_payload(DataRecord const& record) {
  return object(handle<>(PyBytes_FromStringAndSize(
      static_cast<const char*>(record.payload.data()), record.payload.size())));
}

} // namespace

void register_logdevice_record() {
  static const char* DataRecord_doc = R"DOC(
A record that was written to a log, containing a data payload.
)DOC";
  class_<DataRecord, boost::shared_ptr<DataRecord>, boost::noncopyable>(
      "DataRecord", DataRecord_doc, no_init)
      .add_property("logid", &extract_data_logid)
      .add_property("lsn", &extract_lsn)
      .add_property("timestamp", &extract_timestamp)
      .add_property("payload", &extract_payload);

  static const char* GapRecord_doc = R"DOC(
A gap in the numbering sequence of a log, which can occur during reading.

Most gaps are a benign side-effect of LogDevice internal workings. An
exception are gaps of type GapType.DATALOSS, which indicate that the LogDevice
system has lost data. Although the system tries hard to avoid data loss, an
application needs to be ready to handle the possibility. At a minimum,
consider logging data loss gaps for investigation.
)DOC";
  class_<GapRecord, boost::shared_ptr<GapRecord>, boost::noncopyable>(
      "GapRecord", GapRecord_doc, no_init)
      .add_property("logid", &extract_gap_logid)
      .def_readonly("type", &GapRecord::type)
      .def_readonly("lo", &GapRecord::lo, "lowest LSN in this gap (inclusive)")
      .def_readonly(
          "hi", &GapRecord::hi, "highest LSN in this gap (inclusive)");

  static const char* GapType_doc = R"DOC(
Enum for types of gaps that can show up during reading.  See
logdevice/include/Record.h for details on the different gap types.
)DOC";
  enum_<GapType>("GapType", GapType_doc)
      .value("UNKNOWN", GapType::UNKNOWN)
      .value("BRIDGE", GapType::BRIDGE)
      .value("HOLE", GapType::HOLE)
      .value("DATALOSS", GapType::DATALOSS)
      .value("TRIM", GapType::TRIM)
      .value("ACCESS", GapType::ACCESS)
      .value("NOTINCONFIG", GapType::NOTINCONFIG)
      .value("FILTERED_OUT", GapType::FILTERED_OUT);
  static_assert(int(GapType::MAX) == int(GapType::FILTERED_OUT) + 1,
                "update this list if adding a new gap");
}
