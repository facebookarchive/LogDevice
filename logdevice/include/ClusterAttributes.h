/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <memory>
#include <string>

namespace facebook { namespace logdevice {

/**
 * All the info we can get on the client config
 */
class ClusterAttributes {
 public:
  /**
   * Returns the name of the cluster
   */
  virtual std::string getClusterName() const = 0;

  /**
   * Returns the creation time of the cluster,
   * std::chrono::seconds::zero() if unknown.
   */
  virtual std::chrono::seconds getClusterCreationTime() const = 0;

  /**
   * Returns an attribute that isn't recognized by logdevice with the name
   * specified, as a string, possibly JSON encoded if it is a complex type.
   */
  virtual std::string getCustomField(const std::string& name) const = 0;

  virtual ~ClusterAttributes() = default;

 protected:
  ClusterAttributes() = default;
  // non-copyable && non-assignable
  ClusterAttributes(const ClusterAttributes&) = delete;
  ClusterAttributes& operator=(const ClusterAttributes&) = delete;
};

}} // namespace facebook::logdevice
