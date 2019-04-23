/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/types.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

using facebook::logdevice::toString;

/**
 * @file PerRoleConfig packs the membership config and the mutable node
 * attributes config. There will be one PerRoleConfig for each node role.
 */

// convenient utility for making a copy of a config, applying an update and
// if success, return a const shared_ptr of the new config
template <typename Config>
std::shared_ptr<const Config>
applyConfigUpdate(const Config& config, const typename Config::Update& update) {
  auto new_config = std::make_shared<Config>(config);
  int rv = new_config->applyUpdate(update, new_config.get());
  return rv == 0 ? new_config : nullptr;
}

template <NodeRole Role, typename MembershipConfig, typename AttributesConfig>
class PerRoleConfig {
 public:
  struct Update {
    std::unique_ptr<typename MembershipConfig::Update> membership_update{
        nullptr};
    std::unique_ptr<typename AttributesConfig::Update> attributes_update{
        nullptr};

    bool isValid() const {
      if (membership_update == nullptr && attributes_update == nullptr) {
        // must provide at least one update
        return false;
      }
      return (membership_update == nullptr || membership_update->isValid()) &&
          (attributes_update == nullptr || attributes_update->isValid());
    }

    std::string toString() const {
      return folly::sformat(
          "[M:{}, A:{}]",
          membership_update ? membership_update->toString() : "",
          attributes_update ? attributes_update->toString() : "");
    }
  };

  NodeRole getRole() const {
    return Role;
  }

  explicit PerRoleConfig()
      : membership_(std::make_shared<MembershipConfig>()),
        attributes_(std::make_shared<AttributesConfig>()) {
    // empty config should always be valid
    ld_assert(validate());
  }

  PerRoleConfig(std::shared_ptr<const MembershipConfig> membership,
                std::shared_ptr<const AttributesConfig> attributes)
      : membership_(std::move(membership)), attributes_(std::move(attributes)) {
    ld_check(membership_ && attributes_);
  }

  const std::shared_ptr<const MembershipConfig>& getMembership() const {
    return membership_;
  }
  const std::shared_ptr<const AttributesConfig>& getAttributes() const {
    return attributes_;
  }

  // perform role-specific validation, return true if the config passed
  // validation. declared but not defined, we expect each instantiated class to
  // provide their own definitions
  bool roleSpecificValidate() const;

  bool validate() const {
    if (membership_ == nullptr || attributes_ == nullptr ||
        !membership_->validate() || !attributes_->validate()) {
      return false;
    }

    // for each node in the membership, it must have a valid attribute
    for (const node_index_t n : *membership_) {
      if (!attributes_->hasNode(n)) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        5,
                        "Validation failed for config for Role %s! Node %s in "
                        "membership but not has no attributes.",
                        nodes::toString(getRole()).str().c_str(),
                        nodes::toString(n).c_str());
        return false;
      }
    }
    return roleSpecificValidate();
  }

  std::shared_ptr<const PerRoleConfig> applyUpdate(const Update& update) const {
    ld_check(membership_ && attributes_);
    if (!update.isValid()) {
      err = E::INVALID_PARAM;
      return nullptr;
    }

    std::shared_ptr<const MembershipConfig> new_membership;
    std::shared_ptr<const AttributesConfig> new_attributes;
    // updating membership
    if (update.membership_update) {
      new_membership =
          applyConfigUpdate(*membership_, *update.membership_update);
      if (new_membership == nullptr) {
        return nullptr;
      }
    } else {
      // no update, use the existing membership w/o copy
      new_membership = membership_;
    }

    // updating attribtes
    if (update.attributes_update) {
      new_attributes =
          applyConfigUpdate(*attributes_, *update.attributes_update);
      if (new_attributes == nullptr) {
        return nullptr;
      }
    } else {
      new_attributes = attributes_;
    }

    auto res = std::make_shared<const PerRoleConfig>(
        std::move(new_membership), std::move(new_attributes));
    if (!res->validate()) {
      err = E::INVALID_CONFIG;
      return nullptr;
    }

    return res;
  }

  bool isEmpty() const {
    ld_check(membership_ && attributes_);
    return membership_->isEmpty() && attributes_->isEmpty();
  }

  bool operator==(const PerRoleConfig& rhs) const {
    return compare_obj_ptrs(membership_, rhs.membership_) &&
        compare_obj_ptrs(attributes_, rhs.attributes_);
  }

  std::shared_ptr<const PerRoleConfig> withIncrementedVersion(
      folly::Optional<membership::MembershipVersion::Type> new_version) const {
    ld_check(membership_);
    auto new_config = std::make_shared<PerRoleConfig>(*this);
    new_config->membership_ = membership_->withIncrementedVersion(new_version);
    if (!new_config->membership_) {
      return nullptr;
    }
    return new_config;
  }

 private:
  std::shared_ptr<const MembershipConfig> membership_{nullptr};
  std::shared_ptr<const AttributesConfig> attributes_{nullptr};

  friend class NodesConfigLegacyConverter;
  friend class NodesConfigurationThriftConverter;
};

}}}} // namespace facebook::logdevice::configuration::nodes
