/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <cstdlib>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>

#include <folly/AtomicHashMap.h>
#include <folly/Benchmark.h>
#include <folly/Memory.h>
#include <folly/SharedMutex.h>
#include <folly/ThreadLocal.h>
#include <folly/container/F14Map.h>
#include <gflags/gflags.h>
#include <google/dense_hash_map>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

using namespace facebook::logdevice;

namespace {

/**
 * @file Benchmark comparing access and update performance of folly's
 *       AtomicHashMap, and a std::unorderd_map protected by folly's
 *       SharedMutex.
 *
 *       Run with --bm_min_usec=1000000.
 */

enum : size_t {
  MAP_ENTRIES_START = 16 * 1024,
  MAP_ENTRIES_FULL = 2 * MAP_ENTRIES_START,
};

class RandSeedStore;
folly::ThreadLocal<unsigned int, RandSeedStore> seeds;

namespace atomic_map_of_ptrs {
class Map {
  using MapType = folly::AtomicHashMap<logid_t::raw_type,
                                       std::unique_ptr<logid_t>,
                                       Hash64<logid_t::raw_type>>;

 public:
  Map() : map_(MAP_ENTRIES_FULL) {}

  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
};

int Map::insert(const logid_t& k) {
  std::unique_ptr<logid_t> logid = std::make_unique<logid_t>(k);
  std::pair<MapType::iterator, bool> insertion_result;

  try {
    insertion_result = map_.insert(k.val_, std::move(logid));
  } catch (folly::AtomicHashMapFullError&) {
    err = E::NOBUFS;
    return -1;
  }

  return 0;
}

int Map::erase(const logid_t& k) {
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace atomic_map_of_ptrs

namespace atomic_map_of_ptrs_with_lock {
class Map {
  using MapType = folly::AtomicHashMap<logid_t::raw_type,
                                       std::unique_ptr<logid_t>,
                                       Hash64<logid_t::raw_type>>;

 public:
  Map() : map_(MAP_ENTRIES_FULL) {}

  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
  folly::SharedMutex map_mutex_;
};

int Map::insert(const logid_t& k) {
  std::unique_ptr<logid_t> logid = std::make_unique<logid_t>(k);
  std::pair<MapType::iterator, bool> insertion_result;

  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  try {
    insertion_result = map_.insert(k.val_, std::move(logid));
  } catch (folly::AtomicHashMapFullError&) {
    err = E::NOBUFS;
    return -1;
  }

  return 0;
}

int Map::erase(const logid_t& k) {
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace atomic_map_of_ptrs_with_lock

namespace atomic_map_of_shared_ptrs_with_lock {
class Map {
  using MapType = folly::AtomicHashMap<logid_t::raw_type,
                                       std::shared_ptr<logid_t>,
                                       Hash64<logid_t::raw_type>>;

 public:
  Map() : map_(MAP_ENTRIES_FULL) {}

  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
  folly::SharedMutex map_mutex_;
};

int Map::insert(const logid_t& k) {
  std::shared_ptr<logid_t> logid = std::make_shared<logid_t>(k);
  std::pair<MapType::iterator, bool> insertion_result;

  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  try {
    insertion_result = map_.emplace(k.val_, std::move(logid));
  } catch (folly::AtomicHashMapFullError&) {
    err = E::NOBUFS;
    return -1;
  }

  return 0;
}

int Map::erase(const logid_t& k) {
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace atomic_map_of_shared_ptrs_with_lock

namespace atomic_map_of_updateable_shared_ptrs {
class Map {
  using MapType = folly::AtomicHashMap<logid_t::raw_type,
                                       UpdateableSharedPtr<logid_t>,
                                       Hash64<logid_t::raw_type>>;

 public:
  Map() : map_(MAP_ENTRIES_FULL) {}

  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
};

int Map::insert(const logid_t& k) {
  auto logid = std::make_shared<logid_t>(k);
  std::pair<MapType::iterator, bool> insertion_result;

  try {
    insertion_result = map_.emplace(k.val_, std::move(logid));
  } catch (folly::AtomicHashMapFullError&) {
    err = E::NOBUFS;
    return -1;
  }

  return 0;
}

int Map::erase(const logid_t& k) {
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get().get();
}
} // namespace atomic_map_of_updateable_shared_ptrs

namespace map_of_ptrs {
class Map {
  using MapType = std::unordered_map<logid_t::raw_type,
                                     std::unique_ptr<logid_t>,
                                     Hash64<logid_t::raw_type>>;

 public:
  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
  folly::SharedMutex map_mutex_;
};

int Map::insert(const logid_t& k) {
  std::pair<MapType::iterator, bool> insertion_result;
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  insertion_result = map_.emplace(k.val(), std::make_unique<logid_t>(k));
  return insertion_result.second ? 0 : 1;
}

int Map::erase(const logid_t& k) {
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace map_of_ptrs

namespace map_of_shared_ptrs {
class Map {
  using MapType = std::unordered_map<logid_t::raw_type,
                                     std::shared_ptr<logid_t>,
                                     Hash64<logid_t::raw_type>>;

 public:
  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
  folly::SharedMutex map_mutex_;
};

int Map::insert(const logid_t& k) {
  std::pair<MapType::iterator, bool> insertion_result;
  std::shared_ptr<logid_t> logid = std::make_shared<logid_t>(k);
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  insertion_result = map_.emplace(std::piecewise_construct,
                                  std::forward_as_tuple(k.val()),
                                  std::forward_as_tuple(logid));
  return insertion_result.second ? 0 : 1;
}

int Map::erase(const logid_t& k) {
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace map_of_shared_ptrs

namespace dense_map_of_shared_ptrs {
class Map {
  using MapType = google::dense_hash_map<logid_t::raw_type,
                                         std::shared_ptr<logid_t>,
                                         Hash64<logid_t::raw_type>>;

 public:
  Map();
  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
  folly::SharedMutex map_mutex_;
};

Map::Map() {
  map_.set_empty_key(LOGID_INVALID.val());
  map_.set_deleted_key(LOGID_INVALID2.val());
}

int Map::insert(const logid_t& k) {
  auto logid = std::make_shared<logid_t>(k);
  std::pair<MapType::iterator, bool> insertion_result;

  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  insertion_result = map_.insert(std::make_pair(k.val(), std::move(logid)));
  return insertion_result.second ? 0 : 1;
}

int Map::erase(const logid_t& k) {
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace dense_map_of_shared_ptrs

namespace f14_map_of_shared_ptrs {
class Map {
  using MapType = folly::F14FastMap<logid_t::raw_type,
                                    std::shared_ptr<logid_t>,
                                    Hash64<logid_t::raw_type>>;

 public:
  Map();
  int insert(const logid_t& k);
  int erase(const logid_t& k);
  logid_t* find(const logid_t& k);

 private:
  MapType map_;
  folly::SharedMutex map_mutex_;
};

Map::Map() {}

int Map::insert(const logid_t& k) {
  auto logid = std::make_shared<logid_t>(k);
  std::pair<MapType::iterator, bool> insertion_result;

  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  insertion_result = map_.insert(std::make_pair(k.val(), std::move(logid)));
  return insertion_result.second ? 0 : 1;
}

int Map::erase(const logid_t& k) {
  folly::SharedMutex::WriteHolder map_lock(map_mutex_);
  return map_.erase(k.val_);
}

logid_t* Map::find(const logid_t& k) {
  folly::SharedMutex::ReadHolder map_lock(map_mutex_);
  auto it = map_.find(k.val_);
  if (it == map_.end()) {
    return nullptr;
  }
  return it->second.get();
}
} // namespace f14_map_of_shared_ptrs

template <typename Map>
void benchInit(Map& m) {
  BENCHMARK_SUSPEND {
    *seeds.get() = 0;

    for (int i = 0; i < MAP_ENTRIES_START; i++) {
      m.insert(logid_t((rand_r(seeds.get()) % MAP_ENTRIES_FULL) + 1));
    }
  }
}

template <typename MapType>
void readMap(MapType& map) {
  logid_t* log_id =
      map.find(logid_t((rand_r(seeds.get()) % MAP_ENTRIES_FULL) + 1));
  folly::doNotOptimizeAway(log_id);
}

template <typename MapType>
void writeMap(MapType& map) {
  logid_t log_id((rand_r(seeds.get()) % MAP_ENTRIES_FULL) + 1);

  for (;;) {
    if (map.insert(log_id) == 0) {
      break;
    }
    map.erase(log_id);
  }
}

template <class MapType>
void benchReads(int n) {
  MapType map;

  benchInit(map);

  for (int i = 0; i < n; ++i) {
    readMap(map);
  }
}

template <class MapType>
void benchWrites(int n) {
  MapType map;

  benchInit(map);

  for (int i = 0; i < n; ++i) {
    writeMap(map);
  }
}

template <class MapType>
void benchReadsWhenWriting(int n) {
  MapType map;
  std::atomic<bool> shutdown{false};
  std::thread writing_thread;

  benchInit(map);

  BENCHMARK_SUSPEND {
    writing_thread = std::thread([&map, &shutdown] {
      // Use a different seed from the reader.
      *seeds.get() = 1;

      while (!shutdown.load()) {
        writeMap(map);
      }
    });
  }

  for (int i = 0; i < n; ++i) {
    readMap(map);
  }

  BENCHMARK_SUSPEND {
    shutdown.store(true);
    writing_thread.join();
  }
}

template <class MapType>
void benchWritesWhenReading(int n) {
  MapType map;
  std::atomic<bool> shutdown{false};
  std::thread reading_thread;

  benchInit(map);

  BENCHMARK_SUSPEND {
    reading_thread = std::thread([&map, &shutdown] {
      // Use a different seed from the writer.
      *seeds.get() = 1;

      while (!shutdown.load()) {
        readMap(map);
      }
    });
  }

  for (int i = 0; i < n; ++i) {
    writeMap(map);
  }

  BENCHMARK_SUSPEND {
    shutdown.store(true);
    reading_thread.join();
  }
}

template <class MapType>
void benchReadsIn10Threads(int n) {
  MapType map;
  std::atomic<bool> shutdown{false};
  std::vector<std::thread> threads(10);
  int threadNumber{1};

  benchInit(map);

  for (std::thread& t : threads) {
    t = std::thread([&map, threadNumber, n] {
      *seeds.get() = threadNumber;
      for (int i = 0; i < n; ++i) {
        readMap(map);
      }
    });
    threadNumber++;
  }

  for (std::thread& t : threads) {
    t.join();
  }
}

#define BENCH(name)                                               \
  BENCHMARK(name##_AtomicMapOfPtrs, n) {                          \
    bench##name<atomic_map_of_ptrs::Map>(n);                      \
  }                                                               \
  BENCHMARK_RELATIVE(name##_AtomicMapOfPtrsWithLock, n) {         \
    bench##name<atomic_map_of_ptrs_with_lock::Map>(n);            \
  }                                                               \
  BENCHMARK_RELATIVE(name##_AtomicMapOfUpdateableSharedPtrs, n) { \
    bench##name<atomic_map_of_updateable_shared_ptrs::Map>(n);    \
  }                                                               \
  BENCHMARK_RELATIVE(name##_MapOfPtrs, n) {                       \
    bench##name<map_of_ptrs::Map>(n);                             \
  }                                                               \
  BENCHMARK_RELATIVE(name##_MapOfSharedPtrs, n) {                 \
    bench##name<map_of_shared_ptrs::Map>(n);                      \
  }                                                               \
  BENCHMARK_RELATIVE(name##_DenseMapOfSharedPtrs, n) {            \
    bench##name<dense_map_of_shared_ptrs::Map>(n);                \
  }                                                               \
  BENCHMARK_RELATIVE(name##_F14MapOfSharedPtrs, n) {              \
    bench##name<f14_map_of_shared_ptrs::Map>(n);                  \
  }                                                               \
  BENCHMARK_DRAW_LINE();

BENCH(Reads);
BENCH(Writes);
BENCH(ReadsWhenWriting);
BENCH(WritesWhenReading);
BENCH(ReadsIn10Threads);

} // namespace

#ifndef BENCHMARK_BUNDLE

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  folly::runBenchmarks();

  return 0;
}
#endif
