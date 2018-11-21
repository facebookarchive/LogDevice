/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/FBVector.h>
#include <folly/Function.h>

#include "logdevice/common/CopySet.h"
#include "logdevice/common/Recipient.h"

namespace facebook { namespace logdevice {

class Appender;
struct StoreChainLink;

/**
 * @file  a RecipientSet represents a set of nodes to which an Appender
 *        sent copies of a particular log record as STORE messages.
 *
 *        The defining property of a RecipientSet is that all copies
 *        sent to nodes in a given RecipientSet share the same copyset
 *        in their headers. We say that all those copies belong to the
 *        same _wave_ of STORE messages for that record.
 *
 *        A RecipientSet also keeps track of nodes that have
 *        positively acknowledged STORE messages sent to them.
 */
class RecipientSet {
 public:
  explicit RecipientSet() : replication_(0), stored_(0) {}

  /**
   * @return  the current number of nodes in this RecipientSet. The
   *          size of a RecipientSet cannot exceed COPYSET_SIZE_MAX.
   */
  copyset_size_t size() const {
    ld_check(recipients_.size() <= COPYSET_SIZE_MAX);
    return recipients_.size();
  }

  bool empty() const {
    return size() == 0;
  }

  void reset(copyset_size_t replication, copyset_size_t extra) {
    replication_ = replication;
    recipients_.reserve(replication_ + extra);
  }

  copyset_size_t getReplication() const {
    return replication_;
  }

  copyset_size_t storeCount() const {
    return stored_;
  }

  /**
   * We use this method to deallocate all memory allocated by a
   * RecipientSet embedded in an Appender on the same thread that
   * created the Appender.
   */
  void clear() {
    recipients_.clear();
    // this calls destructors of on_socket_close_ callbacks in all
    // Recipient records and removes the records from their respective
    // socket callback lists.

    recipients_.shrink_to_fit();
  }

  /**
   * Mark the given Recipient as having acknowledged the record
   * copy that was sent to it in a STORE message as fully stored.
   * This is done by swapping the Recipient with the one just past the end of
   * the vector prefix containing Recipients that have stored their records.
   *
   * @param  r  The recipient that acknowledged the STORE message.
   */
  void onStored(Recipient* r);

  /**
   * Replace all recipients in the set with those in copyset[]. Unregister
   * all existing socket callbacks.
   *
   * @param copyset       Set of recipients.
   * @param size          number of elements in copyset[]
   * @param appender  appender this RecipientSet belongs to
   */
  void replace(const StoreChainLink copyset[], int size, Appender* appender);

  /**
   * @return a pointer to Recipient with the specified nid in the set,
   *          or nullptr if nid is not in the set.
   */
  Recipient* find(ShardID shard) {
    for (Recipient& r : recipients_) {
      if (r.getShardID() == shard) {
        return &r;
      }
    }

    return nullptr;
  }

  /**
   * @return Index of the given node in the set or -1 if nid is not in the set.
   */
  int indexOf(ShardID shard);

  /**
   * @return true if the record is fully replicated, ie replication_ recipients
   *         acknowledged that they stored a copy.
   * TODO: This is incorrect when extras are enabled: it ignores cross-domain
   *       replication requirements.
   */
  bool isFullyReplicated() const;

  /**
   * Get the set of recipients that have acknowledged the record copy as fully
   * stored.
   *
   * @param out Array where to write these recipients.
   */
  void getReleaseSet(copyset_custsz_t<4>& out);

  /**
   * Fills in the ids of nodes that have not yet acknowledged their STORE
   * messages and should be sent a DELETE message. The output set will also
   * include nodes that replied with a failure code and need not be sent a
   * DELETE, but for now we will treat them the same.
   *
   * @param out Size of nodeids_out. This function will assert that
   *            nodeids_out is big enough to contain the maximum possible number
   *            of nodes that can receive a DELETE, which is equal to the number
   *            of extras.
   */
  void getDeleteSet(copyset_custsz_t<4>& out);

  // dump the failure state of the recipient set for debug
  std::string dumpRecipientSet();

  // Get the first shard in the copyset, that hasn't replied to the STORE
  // message and is in OUTSTANDING state. This can be used to graylist this
  // node.
  ShardID getFirstOutstandingRecipient();

  // Run the callback for each outstanding shard in the copyset. An outstanding
  // shard is a shard that hasn't replied to the STORE message and is in
  // OUTSTANDING state.
  void
  forEachOutstandingRecipient(folly::Function<void(const ShardID&)> cb) const;

  bool allRecipientsOutstanding();

  folly::fbvector<Recipient>& getRecipients() {
    return recipients_;
  }

 private:
  // Used in debug builds to check the consistency of this class' internals.
  void checkConsistency();

  // replication factor for the log at the time the Appender was started.
  // We cache it here to have a stable value of replication factor for the
  // duration of Appender run, even if log configuration changes in the mean
  // time.
  copyset_size_t replication_;

  // this vector contains Recipient entries in the following order:
  // - The `stored_` recipients that have acknowledged their copy;
  // - The rest are recipients that have not acknowledged their copy.
  //
  // onStored() takes care of reordering entries in this array.
  folly::fbvector<Recipient> recipients_;

  // The number of recipients that are currently selected to be the recipients
  // that will store a copy of the record. Should not exceed replication_.
  // The corresponding Recipient objects are placed at the front of recipients_
  // array.
  copyset_size_t stored_;
};

}} // namespace facebook::logdevice
