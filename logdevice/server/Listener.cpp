/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "Listener.h"

#include <memory>
#include <netdb.h>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>

#include "event2/event.h"
#include "event2/listener.h"
#include "event2/util.h"
#include <folly/ScopeGuard.h>

#include "logdevice/common/ConstructorFailed.h"
#include "logdevice/common/Sockaddr.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/libevent/compat.h"

namespace facebook { namespace logdevice {

Listener::Listener(InterfaceDef iface, std::string thread_name)
    : EventLoop(thread_name, ThreadID::Type::UTILITY),
      iface_(std::move(iface)) {
  const int rv = iface_.isPort() ? setupTcpSockets() : setupUnixSocket();

  if (rv != 0) {
    throw ConstructorFailed();
  }
}

Listener::~Listener() {
  for (int fd : socket_fds_) {
    LD_EV(evutil_closesocket)(fd);
  }
}

void Listener::staticAcceptCallback(struct evconnlistener* /*listener*/,
                                    evutil_socket_t sock,
                                    struct sockaddr* addr,
                                    int len,
                                    void* arg) {
  auto arg_listener = reinterpret_cast<Listener*>(arg);
  if (arg_listener->accept_.load()) {
    arg_listener->acceptCallback(sock, addr, len);
  } else {
    LD_EV(evutil_closesocket)(sock);
  }
}

static void accept_error_callback(struct evconnlistener* /*listener*/,
                                  void* /*arg*/) {
  int err = EVUTIL_SOCKET_ERROR();
  RATELIMIT_ERROR(std::chrono::seconds(10),
                  1,
                  "accept failed with error code %d (%s)",
                  err,
                  evutil_socket_error_to_string(err));
}

/**
 * Helper for setup_evconnlisteners() that creates a new socket for listening
 * on the specified address.  Mostly copied from evconnlistener_new_bind()
 * implementation, with the addition of making ipv6 addresses ipv6-only.
 */
static int new_listener_socket(const struct sockaddr* sa, int socklen) {
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

int Listener::setupTcpSockets() {
  ld_check(iface_.isPort());
  struct addrinfo hints;
  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC;     // v4 and v6
  hints.ai_socktype = SOCK_STREAM; // tcp
  hints.ai_flags = AI_PASSIVE;     // for incoming connections

  struct addrinfo* result = nullptr;
  int rv = getaddrinfo(
      nullptr, std::to_string(iface_.port()).c_str(), &hints, &result);
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

    socket_fds_.push_back(fd);
  }

  return 0;
}

int Listener::setupUnixSocket() {
  ld_check(!iface_.isPort());
  unlink(iface_.path().c_str());
  Sockaddr addr(iface_.path());
  struct sockaddr_storage ss;
  int len = addr.toStructSockaddr(&ss);
  int fd = new_listener_socket(reinterpret_cast<struct sockaddr*>(&ss), len);
  if (fd == -1) {
    return -1;
  }

  socket_fds_.push_back(fd);

  return 0;
}

int Listener::startAcceptingConnections() {
  struct event_base* base = getEventBase();
  ld_check(base != nullptr);

  for (auto it = socket_fds_.begin(); it != socket_fds_.end();) {
    auto cur = it++;

    const unsigned flags = LEV_OPT_CLOSE_ON_FREE;
    struct evconnlistener* listener =
        LD_EV(evconnlistener_new)(base,
                                  staticAcceptCallback,
                                  this,
                                  flags,
                                  -1, // auto-pick backlog
                                  *cur);

    if (listener == nullptr) {
      ld_error("evconnlistener_new() failed (port:%d, path:%s): %s",
               iface_.isPort() ? iface_.port() : -1,
               iface_.isPort() ? "-" : iface_.path().c_str(),
               strerror(errno));
      return -1;
    }

    LD_EV(evconnlistener_set_error_cb)(listener, accept_error_callback);
    evconnlisteners_.emplace_back(listener, LD_EV(evconnlistener_free));

    socket_fds_.erase(cur);
  }

  ld_check(socket_fds_.begin() == socket_fds_.end());

  if (iface_.isPort()) {
    ld_info("Listening on %zu interfaces, port %d",
            evconnlisteners_.size(),
            iface_.port());
  } else {
    ld_info("Listening on %s", iface_.path().c_str());
  }
  return 0;
}

}} // namespace facebook::logdevice
