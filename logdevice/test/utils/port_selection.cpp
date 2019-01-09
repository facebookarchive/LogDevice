/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/test/utils/port_selection.h"

#include <cerrno>
#include <random>

#include <folly/Memory.h>
#include <netinet/in.h>

#include "event2/util.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {
namespace detail {

std::unique_ptr<PortOwner> claim_port(int port) {
  int rv;
  const Sockaddr addr("::", std::to_string(port));
  int sock = socket(AF_INET6, SOCK_STREAM, 0);

  ld_check(sock != -1);

  // Subprocesses must not inherit this fd
  rv = LD_EV(evutil_make_socket_closeonexec)(sock);
  ld_check(rv == 0);
  // Subprocesses need to be able to bind to this port immediately after we
  // close it
  rv = LD_EV(evutil_make_listen_socket_reuseable)(sock);
  ld_check(rv == 0);

  struct sockaddr_storage ss;
  int len = addr.toStructSockaddr(&ss);
  ld_check(len != -1);
  rv = bind(sock, reinterpret_cast<struct sockaddr*>(&ss), len);
  if (rv != 0) {
    ld_check(errno == EADDRINUSE);
    close(sock);
    return nullptr;
  }

  rv = listen(sock, 0);
  if (rv != 0) {
    ld_check(errno == EADDRINUSE);
    close(sock);
    return nullptr;
  }

  return std::make_unique<PortOwner>(port, sock);
}

int find_free_port_set(int npairs, std::vector<PortOwnerPtrTuple>& ports_out) {
  std::random_device rnd;
  const int port_from = 38000, port_upto = 49000;
  const int port_range_size = port_upto - port_from + 1;
  const int offset =
      std::uniform_int_distribution<int>(0, port_range_size - 1)(rnd);

  for (int i = 0; ports_out.size() < npairs && i < port_range_size;) {
    int port1 = port_from + (offset + i) % port_range_size, port2 = port1 + 1,
        port3 = port2 + 1;

    PortOwnerPtrTuple tuple(
        claim_port(port1), claim_port(port2), claim_port(port3));
    if (std::get<0>(tuple) && std::get<1>(tuple) && std::get<2>(tuple)) {
      ports_out.push_back(std::move(tuple));
      i += 2;
    } else {
      i += 1;
    }
  }
  return ports_out.size() == npairs ? 0 : -1;
}

}}}} // namespace facebook::logdevice::IntegrationTestUtils::detail
