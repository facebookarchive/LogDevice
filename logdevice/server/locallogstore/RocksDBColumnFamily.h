/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <list>

#include <folly/Synchronized.h>
#include <rocksdb/db.h>

#include "logdevice/common/Timestamp.h"
#include "logdevice/common/toString.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

// Used to express dependent memtables. MemtableFlushDependency.first is
// dependent on the data contained in MemtableFlushDependency.second.
using MemtableFlushDependency = std::pair<FlushToken, FlushToken>;

// Holds information about a rocksdb column family managed by logdevice.
struct RocksDBColumnFamily {
  // Underlying column family handle.
  const std::unique_ptr<rocksdb::ColumnFamilyHandle> cf_;

  // Writes by LogsDB into any column family update metadata column family as
  // well. For correctness, it is necessary to persist metadata cf updates
  // before corresponding writes in a column family. This data structure
  // tracks current active memtable and metadata column family memtable which
  // contains the corresponding updates. The flushing mechanism uses this data
  // to figure out if metadata memtable needs to be flushed and flushes the
  // memtables in the right order.
  //
  // Flusher needs to select memtables to flush and make sure the dependency
  // does not change through the flushing activity. To ensure this, decisions to
  // flush and invocation of rocksdb flush api is done from single thread. The
  // rocksdb flush implementation marks current memtable as immutable and
  // creates a new empty memtable and makes it active for the column family. It
  // pushes the immutable memtable to background threads where they get
  // persisted. In order to ensure depdency does not change, this entire
  // procedure of marking immutable and switching needs to be done atomically
  // for a memtable and it's metadata dependency. Also background threads need
  // to make sure that metadata memtable need to persist first before dependent
  // memtable. RocksDB satisfies this requirement by providing Flush api that
  // accepts multiple column families and atomically switches memtables of all
  // the column families under the db mutex. RocksDB background threads satisfy
  // the requirement of flushing in order by supporting the atomic flush feature
  // where all memtables that were atomically switched together are persisted as
  // a single transaction.
  //
  // Each writer thread after finishing the write queries the current active
  // metadata memtable and marks it as a dependency. There are multiple writer
  // threads writing into same column family at a time. Decision to flush a
  // column family is taken from a separate thread dedicated for running the
  // flushing logic. Assume that MemtableTracker contains some metadata memtable
  // dependency, which already flushed. A new write is initiated for the column
  // family which updates active memtable and newly created metadata memtable.
  // Before the new dependency can be register flusher threads decides to pick
  // this column family for flushing and it queries the tracker for dependent
  // memtable. Flusher will see that the metadata memtable was already flushed
  // and this column family can be flushed solo. This is incorrect because
  // active memtable contains data which is dependent on a later metadata
  // memtable. To avoid this, when initiating write to a column family
  // active_writers_count in Tracker is incremented and it is decremented when
  // plugging in the metadata dependency. If flusher thread decides to flush a
  // column family with non zero writers it gets FlushToken_MAX as the dependent
  // memtable for the column family which indicates it to flush the latest
  // metadata memtable anyway. beginWrite and endWrite methods mark the
  // beginning and installation of dependency respectively.
  struct MemtableTracker {
    // Tracks column family's most recent memtable's FlushToken. This value is
    // initialized when rocksdb memtable for the column family dirtied for the
    // first time. dirty flag indicates whether the recent memtable is in memory
    // and active or not.
    FlushToken recent_memtable{FlushToken_INVALID};

    // True if the recent_memtable is dirty and currently active. False if
    // recent_memtable is immutable and currently getting flushed, or if the
    // memtable is already flushed and new memtable for the column family was
    // not dirtied.
    bool dirty{false};

    // If valid contians metadata column family memtable. This memtable contain
    // updates that need to be persisted before persisting active memtable of
    // this column family.
    FlushToken dependent_memtable{FlushToken_INVALID};

    // Number of writer threads operating within RocksDB for this column family.
    // This informs the flush code that RocksDB operations are still in flight
    // that may still dirty the metadata CF.
    //
    // This count and special value for dependent_memtable protects from a race
    // where an incorrect memtable dependency would be recorded in the tracker
    // when there are active writers writing into the column family. The exact
    // scenario is described above. The pessimistic_metadata_memtable_flush stat
    // counts number of time dependent memtable was flushed because of special
    // value.
    uint32_t active_writers_count{0};

    // Ideally I would like to have the column_family handle of the dependent
    // memtable here as well. Currently it's assumed that dependent column
    // family is metadata column family. If that assumption changes then we
    // will have to add the dependent column family handle here. Also, if we are
    // going to implement complete transaction semantics with multiple column
    // families involved, there could be multiple dependent column families. In
    // that scenario we will have to extend this to be become a list of
    // dependent column families and the corresponding memtables.

    // Tracks memtables that are currently being written out to persistent
    // storage. This is used to assert that memtables are always flushed in
    // order and metadata memtable dependency is persisted when flushed
    // completed callback is invoked. Once we get the notification that the
    // flush completed they will be removed from here. It's expected that the
    // memtables are popped in the order that they were flushed. When flush
    // completes, we check to make sure that the dependent memtable was also
    // persisted.
    std::list<MemtableFlushDependency> memtables_being_flushed;
  };

  // Lowest order of lock used to protect the memtable tracker. Tracker is
  // accessed within rocksdb callback context when a memtable is created or when
  // memtable is flushed. In both cases tracker fields are updated. From
  // logdevice, tracker is accessed to update the dependent memtable. Updates
  // from these two contexts can race and hence the synchronized access.
  folly::Synchronized<MemtableTracker> tracker_;

  // Time when data was inserted for the first time in the current active
  // memtable. If current active memtable is empty, min().
  AtomicSteadyTimestamp first_dirtied_time_{SteadyTimestamp::min()};

  RocksDBColumnFamily(rocksdb::ColumnFamilyHandle* cf) : cf_(cf) {}

  uint32_t getID() {
    return cf_->GetID();
  }

  rocksdb::ColumnFamilyHandle* get() {
    return cf_.get();
  }

  // Invoked when rocksdb has decided to flush this column family, but before
  // the active MemTable is switched to a new, empty, MemTable instance. Push
  // the active and dependent memtables to the queue and mark the values as
  // invalid.
  void onFlushBegin() {
    // Clear first dirtied time to mark that data for this column family was
    // flushed.
    first_dirtied_time_ = SteadyTimestamp::min();
    tracker_.withWLock([&](auto& locked_dependencies) {
      auto& dirty = locked_dependencies.dirty;
      ld_check(dirty);
      auto& active = locked_dependencies.recent_memtable;
      auto& dependent = locked_dependencies.dependent_memtable;
      /* TODO : T38379675 Uncomment after implementing MarkFlushed for
       * memtablerep.
      auto& memtables_being_flushed =
          locked_dependencies.memtables_being_flushed;
      memtables_being_flushed.emplace_back(std::piecewise_construct,
                                           std::forward_as_tuple(active),
                                           std::forward_as_tuple(dependent));
      */
      dirty = false;
      dependent = FlushToken_INVALID;
    });
  }

  // Invoked from RocksDBMemTableRepFactory when a memtable for
  // this column family is dirtied for the first time. The FlushToken passed in
  // belongs to the newly allocated MemTable which is now serving as the active
  // MemTable for the column family.
  void onMemTableDirtied(FlushToken new_memtable_flush_token) {
    // This gets called when memtable is dirtied for the first time.
    // Note down the first dirtied time here.
    first_dirtied_time_ = SteadyTimestamp::now();
    tracker_.withWLock([&](auto& locked_dependencies) {
      ld_check(!locked_dependencies.dirty);
      locked_dependencies.dirty = true;
      // Dependent memtable cannot be asserted to have FlushToken_INVALID.
      // Because, some other thread which just completed the write and its data
      // was flushed in the previous memtable, can invoke endWrite before this
      // method gets invoked. endWrite invocation will update the
      // dependent_memtable from FlushToken_INVALID to a valid value.
      locked_dependencies.recent_memtable = new_memtable_flush_token;
    });
  }

  FlushToken activeMemtableFlushToken() {
    auto active = FlushToken_INVALID;
    tracker_.withRLock([&](auto& locked_dependencies) {
      if (locked_dependencies.dirty) {
        active = locked_dependencies.recent_memtable;
      }
    });
    return active;
  }

  FlushToken getMostRecentMemtableFlushToken() {
    return tracker_.rlock()->recent_memtable;
  }

  FlushToken dependentMemtableFlushToken() {
    auto dependent = FlushToken_MAX;
    tracker_.withRLock([&dependent](auto& locked) {
      if (locked.active_writers_count == 0) {
        dependent = locked.dependent_memtable;
      }
    });
    return dependent;
  }

  void updateMemtableDependency(const FlushToken& dependency) {
    tracker_.withWLock([&dependency](auto& locked) {
      locked.dependent_memtable = dependency;
    });
  }

  // Beginning of a write to a column family, after this rocksDB api will be
  // invoked.
  // Checkout MemtableTracker comments.
  void beginWrite() {
    tracker_.withWLock([&](auto& locked) { ++locked.active_writers_count; });
  }

  // End of a write to a column family, just after returning call of rocksDB
  // api. Returns active metadata memtable flushtoken.
  // Checkout MemtableTracker comments.
  FlushToken endWrite(FlushToken dependency) {
    auto flush_token = FlushToken_INVALID;
    tracker_.withWLock([&dependency, &flush_token](auto& locked) {
      ld_check(locked.active_writers_count > 0);
      --locked.active_writers_count;
      auto& dependent_memtable = locked.dependent_memtable;
      dependent_memtable = std::max(dependent_memtable, dependency);

      flush_token = locked.recent_memtable;
    });

    return flush_token;
  }
};

using WeakRocksDBCFPtr = std::weak_ptr<RocksDBColumnFamily>;
using RocksDBCFPtr = std::shared_ptr<RocksDBColumnFamily>;

}} // namespace facebook::logdevice
