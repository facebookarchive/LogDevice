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

  // Invoked when rocksdb has decided to flush this column family, but before
  // the active MemTable is switched to a new, empty, MemTable instance. Push
  // the active and dependent memtables to the queue and mark the values as
  // invalid.
  void onFlushBegin() {
    tracker_.withWLock([&](auto& locked_dependencies) {
      auto& active = locked_dependencies.active_memtable;
      auto& dependent = locked_dependencies.dependent_memtable;
      auto& memtables_being_flushed =
          locked_dependencies.memtables_being_flushed;
      memtables_being_flushed.emplace_back(std::piecewise_construct,
                                           std::forward_as_tuple(active),
                                           std::forward_as_tuple(dependent));
      active = FlushToken_INVALID;
      dependent = FlushToken_INVALID;
    });
  }

  // Invoked from RocksDBMemTableRepFactory when a memtable for
  // this column family is dirtied for the first time. The FlushToken passed in
  // belongs to the newly allocated MemTable which is now serving as the active
  // MemTable for the column family.
  void onMemTableDirtied(FlushToken new_memtable_flush_token) {
    tracker_.withWLock([&](auto& locked_dependencies) {
      ld_check(locked_dependencies.active_memtable == FlushToken_INVALID);
      // Dependent memtable can be asserted to have FlushToken_INVALID and
      // FlushToken_MAX.
      // Consider the following :
      // 1. Thread T1 initiates a write by marking dependent_memtable as
      // FlushToken_MAX and successfully writes into the memtable.
      // 2. Another thread T2 after write to memtable by T1 initiates flush of
      // the memtable for some reason. Before switching the active memtable the
      // dependent_memtable is marked as FlushToken_INVALID.
      // 3. Now on T2 the new memtable will be allocated and let's assume for
      // simplicity the first write happens on the same thread. This method is
      // invoked on the first write.
      // Now we can end up in following scenarios :
      // 1. T1 has not updated the dependent memtable value, that means on
      // invocation of this method it's still FlushToken_INVALID. T1 won't be
      // able to update dependency anymore.
      // 2. T2 writes before T1 can update dependency. In this case this method
      // is called first and dependent_memtable value is still
      // FlushToken_INVALID. T1 won't be able to update dependency anymore.
      // 3. Another thread T3 can initiate write to the
      // same column family after the memtable is switched but before this
      // method gets called and it will mark the dependent value as
      // FlushToken_MAX. ld_check_in(locked_dependencies.dependent_memtable,
      // {FlushToken_INVALID, FlushToken_MAX});
      locked_dependencies.active_memtable = new_memtable_flush_token;
    });
  }

  FlushToken activeMemtableFlushToken() {
    return tracker_.rlock()->active_memtable;
  }
};

using WeakRocksDBCFPtr = std::weak_ptr<RocksDBColumnFamily>;
using RocksDBCFPtr = std::shared_ptr<RocksDBColumnFamily>;

}} // namespace facebook::logdevice
