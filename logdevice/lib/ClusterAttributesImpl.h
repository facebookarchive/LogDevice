/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Optional.h>
#include <folly/dynamic.h>
#include <folly/json.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/include/ClusterAttributes.h"

namespace facebook { namespace logdevice {

class ClusterAttributesImpl : public ClusterAttributes {
 public:
  static std::unique_ptr<ClusterAttributesImpl>
  create(std::shared_ptr<const ServerConfig> config) {
    return std::unique_ptr<ClusterAttributesImpl>(
        new ClusterAttributesImpl(config->getClusterName(),
                                  config->getClusterCreationTime(),
                                  config->getCustomFields()));
  }
  std::string getClusterName() const override {
    return clusterName_;
  }
  std::chrono::seconds getClusterCreationTime() const override {
    if (clusterCreationTime_) {
      return clusterCreationTime_.value();
    }
    return std::chrono::seconds::zero();
  }
  std::string getCustomField(const std::string& name) const override {
    ld_check(customFields_.isObject());
    auto it = customFields_.find(name);
    if (it == customFields_.items().end()) {
      return "";
    }
    if (it->second.isString() || it->second.isNumber()) {
      return it->second.asString();
    }
    return folly::toJson(it->second);
  }

 private:
  ClusterAttributesImpl(
      const std::string clusterName,
      folly::Optional<std::chrono::seconds> clusterCreationTime,
      folly::dynamic customFields)
      : clusterName_(clusterName),
        clusterCreationTime_(clusterCreationTime),
        customFields_(customFields) {}

  std::string clusterName_;
  folly::Optional<std::chrono::seconds> clusterCreationTime_;
  folly::dynamic customFields_;
};

}} // namespace facebook::logdevice
