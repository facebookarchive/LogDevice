/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/fatalsignal.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <memory>
#include <pwd.h>
#include <signal.h>
#include <unistd.h>

#include <folly/experimental/symbolizer/Symbolizer.h>
#include <sys/mman.h>
#include <sys/types.h>

#include "logdevice/common/debug.h"
#include "rocksdb/cache.h"

namespace facebook { namespace logdevice {

RocksDBCachesInfo g_rocksdb_caches;

static size_t unmap_count = 0, page_count = 0;

static const size_t PAGE_SHIFT = 12;
static const size_t PAGE_SIZE = (1UL << 12);
static const size_t PAGE_MASK = ~((1UL << PAGE_SHIFT) - 1);

struct Segment {
  size_t start;
  size_t end;
  Segment(int s, int e) : start(s), end(e) {}
};

static Segment* sarray = nullptr;
static size_t s_index = 0;
static const size_t SEGMAP_SIZE = 1024 * 1024 * 1024;
static const size_t max_num_segs = SEGMAP_SIZE / sizeof(Segment);

static void unmap_callback(void* entry, size_t charge) {
  if (s_index >= max_num_segs) {
    return;
  }
  // align both start and end to page boundaries
  sarray[s_index].start = (size_t)entry & PAGE_MASK;
  sarray[s_index].end = ((size_t)entry + charge + PAGE_SIZE - 1) & PAGE_MASK;
  ++s_index;
}

static void safe_print(const char msg[]) {
  write(2, msg, std::strlen(msg));
}

static void safe_print_unsigned(size_t num) {
  char buf[64];
  size_t i = 0;
  if (num == 0) {
    safe_print("0");
    return;
  }
  while (num != 0) {
    buf[i++] = (num % 10) + '0';
    num /= 10;
  }
  buf[i] = '\0';
  for (size_t j = 0, k = i - 1; j < k; ++j, --k) {
    size_t tmp = buf[j];
    buf[j] = buf[k];
    buf[k] = tmp;
  }
  safe_print(buf);
}

// Construct this on startup, since it allocates on the heap and we don't want
// to do that in a signal handler.  Leak it so we don't have to worry about
// destruction order.
folly::symbolizer::SafeStackTracePrinter* gStackTrace =
    new folly::symbolizer::SafeStackTracePrinter();

void handle_fatal_signal(int sig) {
  static std::atomic<pthread_t> insegv(0);
  pthread_t old_val(0);

  if (!insegv.compare_exchange_strong(old_val, pthread_self())) {
    /* another thread is in the handler, suspend this one forever */
    if (pthread_self() != old_val) {
      pause();
      _exit(EXIT_FAILURE);
    }
    /* recursive call from the same thread, give up and dump core*/
    raise(SIGTRAP);
    _exit(EXIT_FAILURE);
  }

  safe_print("handle_fatal_signal(): Caught coredump signal ");
  safe_print_unsigned(sig);
  safe_print("\n");

  gStackTrace->printStackTrace(true);

  // allocate a big enough VM to hold all segments
  sarray = (Segment*)mmap(nullptr,
                          SEGMAP_SIZE,
                          PROT_READ | PROT_WRITE,
                          MAP_PRIVATE | MAP_ANONYMOUS,
                          -1,
                          0);
  if (sarray == (Segment*)MAP_FAILED) {
    safe_print("handle_fatal_signal(): allocate seg array failed.\n");
    raise(SIGTRAP);
    _exit(EXIT_FAILURE);
  }

  for (auto cache_weak_ptr : {g_rocksdb_caches.block_cache,
                              g_rocksdb_caches.block_cache_compressed,
                              g_rocksdb_caches.metadata_block_cache}) {
    std::shared_ptr<rocksdb::Cache> cache = cache_weak_ptr.lock();
    if (cache != nullptr) {
      cache->ApplyToAllCacheEntries(unmap_callback, false);
    }
  }

  if (s_index == 0) {
    safe_print("handle_fatal_signal(): No rocksdb caches found.\n");
    munmap((void*)sarray, SEGMAP_SIZE);
    raise(SIGTRAP);
    return;
  }

  safe_print("handle_fatal_signal(): processed segments: ");
  safe_print_unsigned(s_index);
  safe_print("\n");

  std::sort(
      sarray, sarray + s_index, [](const Segment& a, const Segment& b) -> bool {
        return a.start < b.start;
      });

  size_t cur_start = sarray[0].start, cur_end = sarray[0].end;
  for (size_t i = 1; i <= s_index; ++i) {
    // unmap a segment
    if (i == s_index || sarray[i].start > cur_end) {
      size_t len = cur_end - cur_start;
      munmap((void*)cur_start, len);
      ++unmap_count;
      page_count += (len >> PAGE_SHIFT);
      if (i < s_index) {
        cur_start = sarray[i].start;
        cur_end = sarray[i].end;
      }
    }
    // could merge
    else {
      cur_end = std::max(cur_end, sarray[i].end);
    }
  }

  safe_print("handle_fatal_signal(): unmapped pages - unmap calls: ");
  safe_print_unsigned(page_count);
  safe_print(" - ");
  safe_print_unsigned(unmap_count);
  safe_print("\n");

  // unmap the seg array
  munmap((void*)sarray, SEGMAP_SIZE);

  raise(SIGTRAP);
}

}} // namespace facebook::logdevice
