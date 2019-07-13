/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/locallogstore/FailingLocalLogStore.h"

namespace facebook { namespace logdevice {

namespace {

class FailingReadIterator : public LocalLogStore::ReadIterator {
 public:
  explicit FailingReadIterator(const FailingLocalLogStore* store)
      : LocalLogStore::ReadIterator(store) {}
  IteratorState state() const override {
    return IteratorState::ERROR;
  }
  bool accessedUnderReplicatedRegion() const override {
    return false;
  }

  // These assert to help us harden callsites against accessing iterators in
  // the error state.
  lsn_t getLSN() const override {
    ld_check(false);
    return LSN_INVALID;
  }
  Slice getRecord() const override {
    ld_check(false);
    return Slice(nullptr, 0);
  }
  void prev() override {
    ld_check(false);
  }
  void next(LocalLogStore::ReadFilter*, LocalLogStore::ReadStats*) override {
    ld_check(false);
  }
  // Seeks are just no-op.
  void seek(lsn_t,
            LocalLogStore::ReadFilter*,
            LocalLogStore::ReadStats*) override {}
  void seekForPrev(lsn_t) override {}
};

class FailingAllLogsIterator : public LocalLogStore::AllLogsIterator {
 public:
  struct LocationImpl : public Location {
    std::string toString() const override {
      return "failing";
    }
  };

  explicit FailingAllLogsIterator(const FailingLocalLogStore* store)
      : store_(store) {}

  IteratorState state() const override {
    return IteratorState::ERROR;
  }
  std::unique_ptr<Location> getLocation() const override {
    ld_check(false);
    return nullptr;
  }
  logid_t getLogID() const override {
    ld_check(false);
    return LOGID_INVALID;
  }
  lsn_t getLSN() const override {
    ld_check(false);
    return LSN_INVALID;
  }
  Slice getRecord() const override {
    ld_check(false);
    return Slice();
  }
  void seek(const Location& location,
            LocalLogStore::ReadFilter*,
            LocalLogStore::ReadStats*) override {
    ld_check(dynamic_cast<const LocationImpl*>(&location));
  }
  void next(LocalLogStore::ReadFilter*, LocalLogStore::ReadStats*) override {
    ld_check(false);
  }
  std::unique_ptr<Location> minLocation() const override {
    return std::make_unique<LocationImpl>();
  }
  std::unique_ptr<Location> metadataLogsBegin() const override {
    return std::make_unique<LocationImpl>();
  }
  void invalidate() override {}

  const LocalLogStore* getStore() const override {
    return store_;
  }

  const FailingLocalLogStore* store_;
};

} // namespace

std::unique_ptr<LocalLogStore::ReadIterator>
FailingLocalLogStore::read(logid_t, const ReadOptions&) const {
  return std::unique_ptr<ReadIterator>(new FailingReadIterator(this));
}

std::unique_ptr<LocalLogStore::AllLogsIterator>
FailingLocalLogStore::readAllLogs(
    const ReadOptions&,
    const folly::Optional<
        std::unordered_map<logid_t, std::pair<lsn_t, lsn_t>>>&) const {
  return std::unique_ptr<AllLogsIterator>(new FailingAllLogsIterator(this));
}

}} // namespace facebook::logdevice
