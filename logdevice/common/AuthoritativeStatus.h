/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>
#include <vector>

namespace facebook { namespace logdevice {

/**
 * @file  Specify different status of a storage node/shard regarding the
 *        replication state of a record.
 */

enum class AuthoritativeStatus : uint8_t {
  // the shard participates in replication, it may have data and must be taken
  // into account for checking failure domain properties. Note that by default,
  // all shards are initialized in this state.
  FULLY_AUTHORITATIVE = 0,

  // UNDERREPLICATION and UNAVAILABLE both mean that this shard is unavailable
  // right now, and not all of its records were re-replicated to other nodes
  // yet. This means that the replication property cannot be safely assumed if
  // one considers the whole nodeset minus this shard.

  // This shard has definitely lost all records stored on it. Readers will ship
  // DATALOSS gaps if too many nodes are in this state.
  UNDERREPLICATION = 1,

  // the shard is `empty' in the sense that it never participates in
  // replication. It is safe to exclude the shard from the nodeset when
  // checking failure domain properties.
  AUTHORITATIVE_EMPTY = 2,

  // This shard is in rebuilding set in RESTORE mode, but may come back later
  // with its data intact. Readers will stall if too many nodes are in this
  // state. Recoveries, on the other hand, won't stall and will fall back to
  // non-authoritative recovery. This is needed because rebuilding can't proceed
  // until recoveries finish.
  UNAVAILABLE = 3,

  Count
};

std::string toString(const AuthoritativeStatus& st);
std::string toShortString(const AuthoritativeStatus& st);
std::vector<std::string> allAuthoritativeStatusStrings();
// Accepts both long and short names. Case sensitive.
// Returns false if there's no such status.
bool parseAuthoritativeStatus(const std::string& s,
                              AuthoritativeStatus& out_status);

}} // namespace facebook::logdevice
