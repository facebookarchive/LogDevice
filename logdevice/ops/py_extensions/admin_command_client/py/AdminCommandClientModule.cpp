/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <boost/python.hpp>

#include "logdevice/clients/python/util/util.h"
#include "logdevice/ops/py_extensions/admin_command_client/AdminCommandClient.h"

using namespace boost::python;
using namespace facebook::logdevice;

namespace facebook { namespace logdevice {

object adminCommandClientException;

std::string send_to_node(AdminCommandClient& self,
                         std::string cmd,
                         std::string host,
                         int port,
                         float timeout) {
  folly::SocketAddress addr(host, port, true);
  std::vector<AdminCommandClient::Request> reqs;
  std::vector<AdminCommandClient::Response> responses;
  reqs.emplace_back(addr, cmd);
  {
    gil_release_and_guard guard;
    responses = self.send(reqs, std::chrono::milliseconds(int(timeout * 1000)));
  }

  if (responses.at(0).success) {
    return responses.at(0).response;
  } else {
    object args = make_tuple(responses.at(0).failure_reason.c_str());
    throw_python_exception(adminCommandClientException, args);
  }
}

}}; // namespace facebook::logdevice

BOOST_PYTHON_MODULE(admin_command_client) {
  adminCommandClientException =
      createExceptionClass("AdminCommandClientException",
                           "An AdminCommandClientException is an error raised "
                           "by the AdminCommandClient");

  class_<AdminCommandClient,
         boost::shared_ptr<AdminCommandClient>,
         boost::noncopyable>("AdminCommandClient")
      .def("send",
           &send_to_node,
           args("self", "cmd", "host", "port", "timeout"));
}
