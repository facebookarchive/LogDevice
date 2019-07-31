/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Function.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/types.h"

namespace facebook { namespace logdevice {
namespace configuration { namespace nodes {
class NodesConfigLegacyConverter;
}} // namespace configuration::nodes

namespace membership {

/**
 * Membership is the part of cluster nodes configuration that usually requires
 * versioning, agreement between members and synchronization protocols.
 */

class Membership {
 public:
  class Update {
   public:
    virtual bool isValid() const = 0;
    virtual MembershipType getType() const = 0;
    virtual std::string toString() const = 0;
    virtual ~Update() {}

    // Unset the bootsrapping flag in the membership when applying the update.
    void finalizeBootstrapping() {
      finalize_bootstrapping = true;
    }

    bool finalize_bootstrapping{false};
  };

  explicit Membership(MembershipVersion::Type version) : version_(version) {}
  virtual ~Membership() {}

  virtual MembershipType getType() const = 0;

  /**
   * Apply a Membership::Update to the membership and output the new
   * membership.
   *
   * @param update          update to apply, must be valid
   * @new_membership_out    output parameter for the new membership
   *
   * @return           0 for success, and write the new membership to
   *                   *new_membership_out. -1 for failure, and err is
   *                   set (err code can differ with overrides).
   */
  virtual int applyUpdate(const Update& update,
                          Membership* new_membership_out) const = 0;

  /**
   * Perform validation of the membership and return true if the
   * membership is valid.
   */
  virtual bool validate() const = 0;

  /**
   * @return  a vector of all nodes (node_index_t) tracked in the membership.
   */
  virtual std::vector<node_index_t> getMembershipNodes() const = 0;

  /**
   * @return  if the membership contains the given node.
   */
  virtual bool hasNode(node_index_t node) const = 0;

  /**
   * @return  true if no nodes are tracked in the membership.
   */
  virtual bool isEmpty() const = 0;

  // run internal validate() checks in DEBUG mode
  void dcheckConsistency() const {
    ld_assert(validate());
  }

  MembershipVersion::Type getVersion() const {
    return version_;
  }

  bool isBootstrapping() const {
    return bootstrapping_;
  }

 protected:
  MembershipVersion::Type version_{MembershipVersion::EMPTY_VERSION};

  // If this flag is set, it means that this cluster is still new and being
  // bootstrapped. Maintenance manager won't enable the sequencers/shards as
  // long as this flag is set. Some special NCM transitions are only allowed
  // while this flag is set.
  bool bootstrapping_{true};

  friend class configuration::nodes::NodesConfigLegacyConverter;
  friend class MembershipThriftConverter;
};

} // namespace membership
}} // namespace facebook::logdevice
