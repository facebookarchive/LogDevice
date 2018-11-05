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

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

// Used to express dependent memtables. MemtableFlushDependency.first is
// dependent on the data contained in MemtableFlushDependency.second.
using MemtableFlushDependency = std::pair<FlushToken, FlushToken>;

// Holds information about a rocksdb column family managed by logdevice.
struct RocksDBColumnFamily {
  // Underlying column family handle.
  const std::unique_ptr<rocksdb::ColumnFamilyHandle> cf_;

  // Tracks active and flushed memtables for this column family. For every
  // flushed memtable, this tracks flush dependency between memtable of this
  // column family and any dependent memtable of other column family that needs
  // to be persisted before this column family's memtable.
  struct MemtableTracker {
    // Tracks column family's current memtable's FlushToken. This value is
    // initialized when rocksdb creates a new memtable for the column family.
    // This value is invalidated when the memtable for the column_family is
    // flushed. When memtable is selected for flushing, this value along with
    // the dependent memtable if any is pushed into memtables pending flush
    // list.
    FlushToken active_memtable{FlushToken_INVALID};

    // Tracks FlushToken of the memtable from different column family that will
    // be flushed before active_memtable. This allows to retire dependent data
    // in different column family before retiring this column family's data.
    // This value is updated till it's decided to flush the active_memtable.
    // Writes to the active_memtable lead to this value getting updated.
    // On deciding to flush active_memtable, this value is frozen.
    // Flushing logic will check if the dependent memtable is already flushed,
    // if not it will issue a flush for the column family so that data in
    // dependent_memtable gets persisted first. After this, active and dependent
    // memtables are added to the memtables pending flush list.
    FlushToken dependent_memtable{FlushToken_INVALID};

    // Ideally I would like to have the column_family handle of the dependent
    // memtable here as well. Currently it's assumed that dependent column
    // family is metadata column family. If that assumption changes then we
    // will have to add the dependent column family handle here. Also, if we are
    // going to implement complete transaction semantics with multiple column
    // families involved, there could be multiple dependent column families. In
    // that scenario we will have to extend this to be become a list of
    // dependent column families and the corresponding memtables.

    // Tracks memtables that are currently being written out to persistent
    // storage. Once we get the notification that the flush completed they
    // will be removed from here. It's expected that the memtables are popped
    // in the order that they were flushed. When flush completes, we check to
    // make sure that the dependent memtable was also persisted.
    std::list<MemtableFlushDependency> memtables_being_flushed;
  };

  // Lowest order of lock used to protect the memtable tracker. Tracker is
  // accessed within rocksdb callback context when a memtable is created or when
  // memtable is flushed. In both cases tracker fields are updated. From
  // logdevice, tracker is accessed to update the dependent memtable. Updates
  // from these two contexts can race and hence the synchronized access.
  folly::Synchronized<MemtableTracker> tracker_;

  RocksDBColumnFamily(rocksdb::ColumnFamilyHandle* cf) : cf_(cf) {}

  uint32_t getID() {
    return cf_->GetID();
  }

  rocksdb::ColumnFamilyHandle* get() {
    return cf_.get();
  }

  // Method returns dependent metadata memtable dependency for this
  // cf's active metadata memtable. Invoked when rocksdb has picked up
  // memtable of this column family for flushing.
  MemtableFlushDependency onMemtableFlushBegin() {
    return {FlushToken_INVALID, FlushToken_INVALID};
  }

  // Invoked when RocksDBMemTableRepFactory instantiates a new memtable for
  // this column family. FlushToken passed in argument belongs to the latest
  // memtable allocated for the column family.
  void newMemtableAllocated(FlushToken new_memtable_flush_token) {
    tracker_.withWLock([&](auto& locked_dependencies) {
      // Remove this once onMemtableFlushBegin is implemented to invalidate
      // active_memtable value on flush.
      ld_check(new_memtable_flush_token > locked_dependencies.active_memtable ||
               locked_dependencies.active_memtable == FlushToken_INVALID);
      locked_dependencies.active_memtable = new_memtable_flush_token;
      locked_dependencies.dependent_memtable = FlushToken_INVALID;
    });
  }

  FlushToken activeMemtableFlushToken() {
    return tracker_.rlock()->active_memtable;
  }
};

using WeakRocksDBCFPtr = std::weak_ptr<RocksDBColumnFamily>;
using RocksDBCFPtr = std::shared_ptr<RocksDBColumnFamily>;

}} // namespace facebook::logdevice
