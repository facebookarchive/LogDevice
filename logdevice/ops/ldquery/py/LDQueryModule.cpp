/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>

#include <boost/python.hpp>
#include <boost/python/suite/indexing/map_indexing_suite.hpp>
#include <boost/python/suite/indexing/vector_indexing_suite.hpp>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/debug.h"
#include "logdevice/ops/ldquery/Errors.h"
#include "logdevice/ops/ldquery/LDQuery.h"
#include "logdevice/ops/ldquery/TableRegistry.h"
#include "logdevice/ops/ldquery/Utils.h"

using namespace boost::python;
using namespace facebook::logdevice;
using namespace facebook::logdevice::ldquery;
using namespace facebook::logdevice::dbg;

namespace facebook { namespace logdevice { namespace ldquery {

object ldQueryErrorClass;
object ldStatementErrorClass;

template <typename T>
boost::shared_ptr<T>
make_boost_shared_ptr_from_std_shared_ptr(std::shared_ptr<T> ptr) {
  return boost::shared_ptr<T>(ptr.get(), [ptr](T*) mutable { ptr.reset(); });
}

// Not to be confused with extract_string() in logdevice_python.h/python.cpp
static std::string extract_string(const object& from, const char* /* name */) {
  if (PyString_Check(from.ptr())) {
    return extract<std::string>(from);
  }

  return extract<std::string>(from.attr("encode")("utf-8"));
}

boost::shared_ptr<LDQuery> ldquery_make_ldquery(object config,
                                                int command_timeout_sec,
                                                bool use_ssl = false) {
  std::chrono::milliseconds cmd_timeout = std::chrono::milliseconds::max();
  if (command_timeout_sec > 0) {
    cmd_timeout = std::chrono::milliseconds{command_timeout_sec * 1000};
  }
  std::shared_ptr<LDQuery> ldquery = std::make_shared<LDQuery>(
      extract_string(config, "config"), cmd_timeout, use_ssl);
  return make_boost_shared_ptr_from_std_shared_ptr(ldquery);
}

LDQuery::QueryResults* ldquery_query(LDQuery& self, object query) {
  auto object = extract_string(query, "query");
  LDQuery::QueryResults res;
  {
    gil_release_and_guard guard;
    res = self.query(std::move(object));
  }

  std::unique_ptr<LDQuery::QueryResults> res_ptr(
      new LDQuery::QueryResults(std::move(res)));

  return res_ptr.release();
}

template <class T>
void translateException(T err, object pythonError) {
  PyErr_SetString(pythonError.ptr(), err.what());
}

bool operator==(const TableColumn& lhs, const TableColumn& rhs) {
  return (lhs.name == rhs.name) && (lhs.type == rhs.type);
}

bool operator==(const TableMetadata& lhs, const TableMetadata& rhs) {
  return (lhs.name == rhs.name) && (lhs.description == rhs.description) &&
      (lhs.columns == rhs.columns);
}

}}} // namespace facebook::logdevice::ldquery

BOOST_PYTHON_MODULE(ext) {
  ldQueryErrorClass = createExceptionClass(
      "LDQueryError",
      "A LDQueryError is a general super type for all ldquery errors");

  ldStatementErrorClass =
      createExceptionClass("StatementError",
                           "An exception in the query statement",
                           ldQueryErrorClass.ptr());

  class_<LDQuery, boost::shared_ptr<LDQuery>, boost::noncopyable>(
      "LDQueryBinding", no_init)

      .def("__init__",
           make_constructor(&ldquery_make_ldquery,
                            default_call_policies(),
                            (arg("config"),
                             arg("command_timeout_sec"),
                             arg("use_ssl") = false)))

      .def("set_pretty_output",
           +[](LDQuery& self, object val) {
             self.setPrettyOutput(extract<bool>(val));
           },
           args("val"))
      .def("get_pretty_output",
           +[](LDQuery& self) { return self.getPrettyOutput(); },
           args("val"))
      .def("set_cache_ttl",
           +[](LDQuery& self, object val) {
             self.setCacheTTL(std::chrono::seconds(extract<long>(val)));
           },
           args("seconds"))
      .def("get_cache_ttl",
           +[](LDQuery& self) {
             return std::chrono::duration_cast<std::chrono::seconds>(
                        self.getCacheTTL())
                 .count();
           })
      .def("enable_server_side_filtering",
           +[](LDQuery& self, object val) {
             self.enableServerSideFiltering(extract<bool>(val));
           },
           args("val"))
      .def("server_side_filtering_enabled",
           +[](LDQuery& self) { return self.serverSideFilteringEnabled(); })

      .def("query",
           &ldquery_query,
           return_value_policy<manage_new_object>(),
           args("query"))
      .def("get_tables",
           +[](LDQuery& self) { return self.getTables(); },
           default_call_policies(),
           return_value_policy<copy_const_reference>());

  class_<std::vector<std::string>>("StringVec")
      .def(vector_indexing_suite<std::vector<std::string>>());

  class_<std::vector<size_t>>("SizetVec")
      .def(vector_indexing_suite<std::vector<size_t>>());

  class_<LDQuery::Rows>("Rows")
      .def(vector_indexing_suite<LDQuery::Rows>())
      .def_readonly("size", &LDQuery::Rows::size);

  class_<LDQuery::QueryResult, boost::noncopyable>("QueryResult", no_init)
      .def_readonly("headers", &LDQuery::QueryResult::headers)
      .def_readonly("rows", &LDQuery::QueryResult::rows)
      .def_readonly("cols_max_size", &LDQuery::QueryResult::cols_max_size)
      .def_readonly("metadata", &LDQuery::QueryResult::metadata);

  class_<LDQuery::QueryResults>("QueryResults")
      .def(vector_indexing_suite<LDQuery::QueryResults>());

  class_<FailedNodeDetails>("Details")
      .def_readonly("address", &FailedNodeDetails::address)
      .def_readonly("failure_reason", &FailedNodeDetails::failure_reason)
      .def("__str__", &FailedNodeDetails::toString)
      .def("__repr__", &FailedNodeDetails::toString);

  class_<std::map<int, FailedNodeDetails>>("FailedNodes")
      .def(map_indexing_suite<std::map<int, FailedNodeDetails>>())
      .def_readonly("size", &std::map<int, FailedNodeDetails>::size);

  class_<ActiveQueryMetadata>("Metadata")
      .def_readonly("failures", &ActiveQueryMetadata::failures)
      .def_readonly("contacted_nodes", &ActiveQueryMetadata::contacted_nodes)
      .def_readonly("latency", &ActiveQueryMetadata::latency)
      .def_readonly("success", &ActiveQueryMetadata::success);

  class_<TableColumn>("Column")
      .def_readonly("name", &TableColumn::name)
      .def_readonly("type", &TableColumn::type_as_string)
      .def_readonly("description", &TableColumn::description);

  class_<std::vector<TableColumn>>("Columns")
      .def(vector_indexing_suite<std::vector<TableColumn>>())
      .def("__len__", &std::vector<TableColumn>::size);

  class_<TableMetadata>("Table")
      .def_readonly("name", &TableMetadata::name)
      .def_readonly("description", &TableMetadata::description)
      .def_readonly("columns", &TableMetadata::columns);

  class_<std::vector<TableMetadata>>("Tables")
      .def(vector_indexing_suite<std::vector<TableMetadata>>())
      .def("__len__", &std::vector<TableMetadata>::size);

  register_exception_translator<LDQueryError>([](LDQueryError err) {
    return translateException(err, ldQueryErrorClass);
  });

  register_exception_translator<StatementError>([](StatementError err) {
    return translateException(err, ldStatementErrorClass);
  });
}
