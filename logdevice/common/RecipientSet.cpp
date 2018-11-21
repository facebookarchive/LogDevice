/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/RecipientSet.h"

#include "logdevice/common/Appender.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/protocol/STORE_Message.h"

namespace facebook { namespace logdevice {

void RecipientSet::replace(const StoreChainLink copyset[],
                           int size,
                           Appender* appender) {
  recipients_.clear(); // unregisters all socket callbacks
  stored_ = 0;

  for (int i = 0; i < size; i++) {
    recipients_.emplace_back(copyset[i].destination, appender);
  }
}

int RecipientSet::indexOf(ShardID shard) {
  int i = 0;
  for (Recipient& r : recipients_) {
    if (r.getShardID() == shard) {
      return i;
    }
    ++i;
  }

  return -1;
}

bool RecipientSet::isFullyReplicated() const {
  ld_check(stored_ <= replication_);
  return stored_ == replication_;
}

std::string RecipientSet::dumpRecipientSet() {
  std::string ret = "{";
  for (int i = 0; i < recipients_.size(); ++i) {
    Recipient& r = recipients_[i];
    ret += r.getShardID().toString();
    ret += ": ";
    ret += Recipient::reasonString(r.state_);
    if (i != recipients_.size() - 1) {
      ret += ", ";
    }
  }
  ret += "}";
  return ret;
}

ShardID RecipientSet::getFirstOutstandingRecipient() {
  for (int i = 0; i < recipients_.size(); ++i) {
    Recipient& r = recipients_[i];
    if (r.state_ == Recipient::State::OUTSTANDING) {
      return r.getShardID();
    }
  }

  return ShardID();
}

void RecipientSet::forEachOutstandingRecipient(
    folly::Function<void(const ShardID&)> cb) const {
  for (const auto& r : recipients_) {
    if (r.state_ == Recipient::State::OUTSTANDING) {
      cb(r.getShardID());
    }
  }
}

bool RecipientSet::allRecipientsOutstanding() {
  for (auto& recipient : recipients_) {
    if (recipient.state_ != Recipient::State::OUTSTANDING) {
      return false;
    }
  }
  return true;
}

void RecipientSet::onStored(Recipient* r) {
  ld_check(r);
  ld_check(stored_ < recipients_.size());

  off_t pos = r - &recipients_[0];
  ld_check(pos >= stored_); // must not be marked stored yet
  ld_check(pos < recipients_.size());

  r->swap(recipients_[stored_]);
  ++stored_;

  if (folly::kIsDebug) {
    checkConsistency();
  }
}

void RecipientSet::getReleaseSet(copyset_custsz_t<4>& out) {
  out.resize(stored_);
  for (copyset_size_t i = 0; i < stored_; i++) {
    out[i] = recipients_[i].getShardID();
  }
}

void RecipientSet::getDeleteSet(copyset_custsz_t<4>& out) {
  out.resize(recipients_.size() - stored_);
  for (copyset_size_t i = stored_; i < recipients_.size(); i++) {
    out[i - stored_] = recipients_[i].getShardID();
  }
}

void RecipientSet::checkConsistency() {
  if (folly::kIsDebug) {
    ld_check(recipients_.size() >= stored_);
    ld_check(stored_ <= replication_);

    // Check that all recipients in `recipients_` are distinct.
    std::vector<ShardID> indexes(recipients_.size());
    std::transform(recipients_.begin(),
                   recipients_.end(),
                   indexes.begin(),
                   [](const Recipient& r) { return r.getShardID(); });
    std::sort(indexes.begin(), indexes.end());
    ld_assert(std::unique(indexes.begin(), indexes.end()) == indexes.end());
  }
}
}} // namespace facebook::logdevice
