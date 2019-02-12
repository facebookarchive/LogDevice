/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <vector>

#include <boost/multi_array.hpp>

#include "logdevice/common/NodeID.h"

namespace facebook { namespace logdevice {
/**
 * Each bucket should contains counts for the duration of
 * Settings::sequencer_boycotting::node_stats_controller_aggregation_period.
 * Buckets are ordered by newest first
 */
struct BucketedNodeStats {
  struct ClientNodeStats {
    uint32_t successes{0};
    uint32_t fails{0};

    explicit ClientNodeStats() noexcept {}

    ClientNodeStats(std::initializer_list<uint32_t> list) {
      if (list.size() != 2) {
        return;
      }

      auto it = list.begin();
      successes = *it++;
      fails = *it;
    }

    explicit ClientNodeStats(uint32_t successes, uint32_t fails) noexcept
        : successes{successes}, fails{fails} {}

    bool operator==(const ClientNodeStats& other) const {
      return successes == other.successes && fails == other.fails;
    }
  };

  struct SummedNodeStats {
    uint32_t client_count{0};
    uint32_t successes{0};
    uint32_t fails{0};

    bool operator==(const SummedNodeStats& other) const {
      return client_count == other.client_count &&
          successes == other.successes && fails == other.fails;
    }

    SummedNodeStats& operator+=(const SummedNodeStats& other) {
      client_count += other.client_count;
      successes += other.successes;
      fails += other.fails;
      return *this;
    }
    SummedNodeStats& operator+=(const ClientNodeStats& other) {
      ++client_count;
      successes += other.successes;
      fails += other.fails;
      return *this;
    }
  };

  explicit BucketedNodeStats() {}
  // expensive to copy
  BucketedNodeStats(const BucketedNodeStats&) = delete;
  BucketedNodeStats& operator=(const BucketedNodeStats&) = delete;

  // move is okay, that's why unique_ptrs are used
  BucketedNodeStats(BucketedNodeStats&& other) noexcept
      : node_ids(std::move(other.node_ids)),
        summed_counts(std::move(other.summed_counts)),
        client_counts(std::move(other.client_counts)) {}

  BucketedNodeStats& operator=(BucketedNodeStats&& other) {
    node_ids = std::move(other.node_ids);
    summed_counts = std::move(other.summed_counts);
    client_counts = std::move(other.client_counts);

    return *this;
  }

  std::vector<NodeID> node_ids;

  // use unique_ptrs to enable easy-move for the multi_arrays

  // first dimension is node count, second is bucket count
  std::unique_ptr<boost::multi_array<SummedNodeStats, 2>> summed_counts =
      std::make_unique<boost::multi_array<SummedNodeStats, 2>>();
  // first dimension is node count, second is bucket count, third is client
  // count
  std::unique_ptr<boost::multi_array<ClientNodeStats, 3>> client_counts =
      std::make_unique<boost::multi_array<ClientNodeStats, 3>>();

  bool operator==(const BucketedNodeStats& other) const {
    return node_ids == other.node_ids &&
        *summed_counts == *other.summed_counts &&
        *client_counts == *other.client_counts;
  }
};
}} // namespace facebook::logdevice
