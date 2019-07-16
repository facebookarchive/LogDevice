/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>

#include "logdevice/common/ClientID.h"
#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice {

/**
 * @file   an Address uniquely identifies a message destination.
 *
 */

static_assert(sizeof(ClientID) == sizeof(NodeID),
              "ClientID and NodeID sizes do not match");
static_assert(sizeof(NodeID) == sizeof(uint32_t),
              "ClientID and NodeID size must be 4 bytes");

struct Address {
  explicit Address(NodeID node_id) : id_(node_id) {}
  explicit Address(ClientID client_id) : id_(client_id) {}
  // Invalid address.
  Address() : Address(ClientID()) {}

  Address(const Address&) = default;
  Address& operator=(const Address&) = default;

  /**
   * @return true iff this address is a ClientID, including ClientID::INVALID.
   */
  bool isClientAddress() const {
    return !id_.node_.isNodeID();
  }

  inline bool isNodeAddress() const {
    return !isClientAddress();
  }

  /**
   * @return true iff this address is either a NodeID, or a valid ClientID
   *         false for ClientID::INVALID.
   */
  bool valid() const {
    return !isClientAddress() || id_.client_.valid();
  }

  bool operator==(const Address& other) const {
    return id_.flat_ == other.id_.flat_;
  }

  std::string toString() const {
    return isClientAddress() ? id_.client_.toString() : id_.node_.toString();
  }

  NodeID asNodeID() const {
    ld_check(valid() && !isClientAddress());
    return id_.node_;
  }

  ClientID asClientID() const {
    ld_check(valid() && isClientAddress());
    return id_.client_;
  }

  union u {
    u(NodeID node) : node_(node) {}
    u(ClientID client) : client_(client) {}

    NodeID node_;
    ClientID client_;
    uint32_t flat_;
  } id_;

  static_assert(sizeof(u) == sizeof(uint32_t), "invalid size of Address.id_");
};

}} // namespace facebook::logdevice
