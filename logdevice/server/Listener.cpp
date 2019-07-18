/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/Listener.h"

#include <memory>
#include <netdb.h>
#include <string>

#include <folly/ScopeGuard.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "event2/event.h"
#include "event2/listener.h"
#include "event2/util.h"
#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

namespace {
/**
 * Helper for setup_evconnlisteners() that creates a new socket for listening
 * on the specified address.  Mostly copied from evconnlistener_new_bind()
 * implementation, with the addition of making ipv6 addresses ipv6-only.
 */
int new_listener_socket(const struct sockaddr* sa, int socklen) {
  int family = sa->sa_family;
  int fd = socket(family, SOCK_STREAM, 0);
  int off = 0, on = 1;

  if (fd == -1) {
    ld_error("socket() failed, errno=%d (%s)", errno, strerror(errno));
    goto err;
  }

  if (LD_EV(evutil_make_socket_nonblocking)(fd) != 0) {
    ld_error("evutil_make_socket_nonblocking() failed");
    goto err;
  }

  if (LD_EV(evutil_make_socket_closeonexec)(fd) != 0) {
    ld_error("evutil_make_socket_closeonexec() failed");
    goto err;
  }

  if (LD_EV(evutil_make_listen_socket_reuseable)(fd) != 0) {
    ld_error("evutil_make_listen_socket_reuseable() failed");
    goto err;
  }

  if (family == AF_INET6 || family == AF_INET) {
    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, (void*)&on, sizeof on) != 0) {
      ld_error("setsockopt() failed to set SO_KEEPALIVE, errno=%d (%s)",
               errno,
               strerror(errno));
      goto err;
    }
  }

  if (family == AF_INET6) {
    // Make sure ipv6 sockets are ipv6-only
    if (setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, (void*)&on, sizeof on) != 0) {
      ld_error("setsockopt() failed to make socket ipv6-only, errno=%d (%s)",
               errno,
               strerror(errno));
      goto err;
    }
  }

  if (bind(fd, sa, socklen) != 0) {
    ld_error("bind() failed, errno=%d (%s)", errno, strerror(errno));
    goto err;
  }
  return fd;

err:
  if (fd != -1) {
    LD_EV(evutil_closesocket)(fd);
  }
  return -1;
}

int setupTcpSockets(const Listener::InterfaceDef& iface,
                    std::vector<int>& fds_out) {
  ld_check(iface.isPort());
  struct addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;     // v4 and v6
  hints.ai_socktype = SOCK_STREAM; // tcp
  hints.ai_flags = AI_PASSIVE;     // for incoming connections

  struct addrinfo* result = nullptr;
  int rv = getaddrinfo(
      nullptr, std::to_string(iface.port()).c_str(), &hints, &result);
  if (rv != 0 || result == nullptr) {
    ld_error(
        "getaddrinfo() failed with error %d (\"%s\")", rv, gai_strerror(rv));
    return -1;
  }

  SCOPE_EXIT {
    freeaddrinfo(result);
  };

  for (struct addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
    int fd = new_listener_socket(ai->ai_addr, ai->ai_addrlen);
    if (fd == -1) {
      return -1;
    }

    fds_out.push_back(fd);
  }

  return 0;
}

int setupUnixSocket(const Listener::InterfaceDef& iface,
                    std::vector<int>& fds_out) {
  ld_check(!iface.isPort());
  unlink(iface.path().c_str());
  Sockaddr addr(iface.path());
  struct sockaddr_storage ss;
  int len = addr.toStructSockaddr(&ss);
  int fd = new_listener_socket(reinterpret_cast<struct sockaddr*>(&ss), len);
  if (fd == -1) {
    return -1;
  }

  fds_out.push_back(fd);

  return 0;
}

void accept_error_callback(struct evconnlistener* /*listener*/, void* /*arg*/) {
  int err = EVUTIL_SOCKET_ERROR();
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  1,
                  "accept failed with error code %d (%s)",
                  err,
                  evutil_socket_error_to_string(err));
}

} // namespace

Listener::Listener(InterfaceDef iface, KeepAlive loop)
    : iface_(std::move(iface)), loop_(loop) {}

Listener::~Listener() {
  stopAcceptingConnections().wait();
}

folly::SemiFuture<bool> Listener::startAcceptingConnections() {
  folly::Promise<bool> promise;
  auto res = promise.getSemiFuture();
  loop_->add([this, promise = std::move(promise)]() mutable {
    promise.setValue(setupEvConnListeners());
  });
  return res;
}

folly::SemiFuture<folly::Unit> Listener::stopAcceptingConnections() {
  folly::Promise<folly::Unit> promise;
  auto res = promise.getSemiFuture();
  loop_->add([this, promise = std::move(promise)]() mutable {
    evconnlisteners_.clear();
    promise.setValue();
  });
  return res;
}

void Listener::staticAcceptCallback(struct evconnlistener* /*listener*/,
                                    evutil_socket_t sock,
                                    struct sockaddr* addr,
                                    int len,
                                    void* arg) {
  auto arg_listener = reinterpret_cast<Listener*>(arg);
  folly::SocketAddress socketAddress;
  ld_check(addr);
  socketAddress.setFromSockaddr(addr, len);
  arg_listener->acceptCallback(sock, socketAddress);
}

bool Listener::setupEvConnListeners() {
  if (evconnlisteners_.size()) {
    // Already listening
    return true;
  }
  std::vector<int> fds;
  int rv = iface_.isPort() ? setupTcpSockets(iface_, fds)
                           : setupUnixSocket(iface_, fds);
  if (rv != 0) {
    for (auto fd : fds) {
      LD_EV(evutil_closesocket)(fd);
    }
    return false;
  }
  event_base* base = loop_->getEventBase();
  ld_check(base != nullptr);

  for (auto it = fds.begin(); it != fds.end(); ++it) {
    const unsigned flags = LEV_OPT_CLOSE_ON_FREE;
    struct evconnlistener* listener =
        LD_EV(evconnlistener_new)(base,
                                  staticAcceptCallback,
                                  this,
                                  flags,
                                  -1, // auto-pick backlog
                                  *it);

    if (listener == nullptr) {
      ld_error("evconnlistener_new() failed (port:%d, path:%s): %s",
               iface_.isPort() ? iface_.port() : -1,
               iface_.isPort() ? "-" : iface_.path().c_str(),
               strerror(errno));
      evconnlisteners_.clear();
      return false;
    }

    LD_EV(evconnlistener_set_error_cb)(listener, accept_error_callback);
    evconnlisteners_.emplace_back(listener, LD_EV(evconnlistener_free));
  }

  if (iface_.isPort()) {
    ld_info("Listening on %zu interfaces, port %d",
            evconnlisteners_.size(),
            iface_.port());
  } else {
    ld_info("Listening on %s", iface_.path().c_str());
  }
  return true;
}

}} // namespace facebook::logdevice
