/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <cstdint>
#include <string>

#include "logdevice/common/checks.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

/**
 * @file   a ClientID identifies an incoming connection that has been accepted
 *         by this Processor. It is a 31-bit unsigned monotonically increasing
 *         number that may wrap-around. Incoming connections are commonly called
 *         "client connections" in LD code, even though they do not have to
 *         originate at a client application.
 *
 *         Every incoming connection is assigned to a particular Worker object.
 *         Only that Worker thread can use the ClientID identifying the
 *         connection to send messages into it. We say that the Worker "owns"
 *         that ClientID.
 *
 *         The client id with all low 31 bits set to 0 is reserved as a special
 *         INVALID client id.
 */

struct ClientID {
  explicit ClientID(int32_t idx) : val_((1u << 31) | idx) {
    ld_check(ClientID::valid(idx));
  }

  explicit ClientID() : val_(1u << 31) {} // constructs ClientID::INVALID

  bool valid() const {
    return (val_ & (1u << 31)) && getIdx() != 0;
  }

  std::string toString() const {
    if (!(val_ & (1u << 31))) {
      return "[invalid ClientID: " + std::to_string(val_) + "]";
    } else if (getIdx() == 0) {
      return "[invalid ClientID: C0]";
    } else {
      return "C" + std::to_string(getIdx());
    }
  }

  int32_t getIdx() const {
    return val_ & 0x7fffffff;
  }

  explicit operator unsigned() {
    return val_;
  } // used in error messages

  static const ClientID INVALID; // invalid client id has 31 low bits = 0
  static const ClientID MIN;     // smallest valid client id (idx=1)

  struct Hash {
    std::size_t operator()(const ClientID& cid) const {
      return cid.val_;
    }
  };

  /**
   * Checks if idx is a valid client index suitable for passing to the
   * ClientID constructor.
   */
  static bool valid(int32_t idx) {
    // since idx is signed, this also checks that bit 31 is unset
    return idx > 0;
  }

 private:
  uint32_t val_; // 31-bit client ids are stored in the low 31 bits of
                 // a 32-bit unsigned, with bit 31 always set to 1
} __attribute__((__packed__));

bool operator==(const ClientID& a, const ClientID& b);

bool operator!=(const ClientID& a, const ClientID& b);

bool operator<(const ClientID& a, const ClientID& b);

}} // namespace facebook::logdevice
