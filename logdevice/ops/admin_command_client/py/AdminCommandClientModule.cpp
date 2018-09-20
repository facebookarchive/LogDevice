/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <boost/python.hpp>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/ops/admin_command_client/AdminCommandClient.h"

using namespace boost::python;
using namespace facebook::logdevice;

namespace facebook { namespace logdevice {

object adminCommandClientException;

std::string send_to_node(AdminCommandClient& self,
                         std::string cmd,
                         std::string host,
                         int port,
                         float timeout,
                         AdminCommandClient::ConnectionType conntype) {
  folly::SocketAddress addr(host, port, true);
  AdminCommandClient::RequestResponses reqs;
  reqs.emplace_back(addr, cmd, conntype);
  {
    gil_release_and_guard guard;
    self.send(reqs, std::chrono::milliseconds(int(timeout * 1000)));
  }

  if (reqs[0].success) {
    return reqs[0].response;
  } else {
    object args = make_tuple(reqs[0].failure_reason.c_str());
    throw_python_exception(adminCommandClientException, args);
  }
}

}}; // namespace facebook::logdevice

BOOST_PYTHON_MODULE(admin_command_client) {
  adminCommandClientException =
      createExceptionClass("AdminCommandClientException",
                           "An AdminCommandClientException is an error raised "
                           "by the AdminCommandClient");

  enum_<AdminCommandClient::ConnectionType>("ConnectionType")
      .value("UNKNOWN", AdminCommandClient::ConnectionType::UNKNOWN)
      .value("PLAIN", AdminCommandClient::ConnectionType::PLAIN)
      .value("ENCRYPTED", AdminCommandClient::ConnectionType::ENCRYPTED);

  class_<AdminCommandClient,
         boost::shared_ptr<AdminCommandClient>,
         boost::noncopyable>("AdminCommandClient")
      .def("send",
           &send_to_node,
           args("self", "cmd", "host", "port", "timeout", "conntype"));
}
