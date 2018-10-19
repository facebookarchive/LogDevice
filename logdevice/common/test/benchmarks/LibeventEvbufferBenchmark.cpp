/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>
#include <err.h>
#include <fcntl.h>
#include <iostream>
#include <memory>
#include <thread>
#include <unistd.h>

#include <folly/Benchmark.h>
#include <folly/ScopeGuard.h>
#include <gflags/gflags.h>
#include <sys/socket.h>
#include <sys/types.h>

#include "event2/buffer.h"
#include "event2/bufferevent.h"
#include "event2/event.h"
#include "logdevice/common/libevent/compat.h"

namespace {

/**
 * @file Benchmark assessing the cost of evbuffer_add and
 *       evbuffer_add_reference.
 */

enum : size_t {
  // Dispatch to libevent so socket events can be processed
  // after every 'n' queues to the evbuffer.
  DISPATCH_INTERVAL = 100,
  MAX_PAYLOADSZ = 131072
};

using EvbWriteFn = std::function<void(struct evbuffer*, size_t)>;

static char sourceBuffer[MAX_PAYLOADSZ];

void copyWriter(struct evbuffer* evbuf, size_t payloadsz) {
  if (LD_EV(evbuffer_add)(evbuf, sourceBuffer, payloadsz) != 0) {
    err(1, "evbuffer_add failed");
  }
}

void refWriter(struct evbuffer* evbuf, size_t payloadsz) {
  if (LD_EV(evbuffer_add_reference)(
          evbuf, sourceBuffer, payloadsz, nullptr, nullptr) != 0) {
    err(1, "evbuffer_add_reference failed");
  }
}

void write(int n, size_t payloadsz, EvbWriteFn writefn, int outfd) {
  int flags = fcntl(outfd, F_GETFL, 0);

  // Make outfd non-blocking.
  if (flags < 0) {
    err(1, "fcnt(F_GETFL) failed");
  }

  if (fcntl(outfd, F_SETFL, flags | O_NONBLOCK) < 0) {
    err(1, "fcnt(F_GETFL) failed");
  }

  struct event_base* base = LD_EV(event_base_new)();
  struct bufferevent* bev =
      LD_EV(bufferevent_socket_new)(base, outfd, BEV_OPT_CLOSE_ON_FREE);
  if (!bev) {
    err(1, "bufferevent_socket_new failed");
  }
  SCOPE_EXIT {
    LD_EV(bufferevent_free)(bev);
    LD_EV(event_base_free)(base);
  };

  for (int i = 1; i <= n; ++i) {
    writefn(LD_EV(bufferevent_get_output)(bev), payloadsz);
    if ((i % DISPATCH_INTERVAL) == 0) {
      LD_EV(event_base_dispatch)(base);
    }
  }
}

void bitbucket(int infd) {
  static int loops;
  char buf[1024];

  while (read(infd, buf, sizeof(buf)) > 0) {
    ++loops;
  }
  close(infd);
}

void bench(int n, size_t payloadsz, EvbWriteFn writefn) {
  int fds[2];

  if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
    err(1, "pipe(2) failed");
  }

  std::thread writer(write, n, payloadsz, writefn, fds[0]);
  std::thread reader(bitbucket, fds[1]);
  writer.join();
  reader.join();
}

#define BENCH(payloadsz)                                                 \
  BENCHMARK_NAMED_PARAM(bench, copy_##payloadsz, payloadsz, copyWriter); \
  BENCHMARK_RELATIVE_NAMED_PARAM(                                        \
      bench, ref_##payloadsz, payloadsz, refWriter);                     \
  BENCHMARK_DRAW_LINE();

BENCH(1);
BENCH(64);
BENCH(128);
BENCH(256);
BENCH(512);
BENCH(1024);
BENCH(2048);
BENCH(4096);
BENCH(8129);
BENCH(16384);
BENCH(32768);
BENCH(65536);
BENCH(131072);

} // namespace

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
#endif
