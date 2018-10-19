/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <algorithm>
#include <string>
#include <vector>

namespace facebook { namespace logdevice {

/**
 * Utility functions to parse and interpret command line argument describing
 * how records should be distributed among logs.
 *
 * An example of argument: "10:0.7,10:.2,-". This means that we should:
 *  - pick 10 random logs, and spread 0.7 of the load among them evenly,
 *  - then pick 10 random of the remaining logs and spread 0.2 of the load among
 *    them evenly,
 *  - then pick 1000 random of the remaining logs and spread the remaining 0.1
 *    of the load evenly among the remaining logs.
 *
 * Such format allows concisely describing non-uniform distributions like power
 * law. Or just selecting a random subset of logs to read.
 *
 * The "random" selection of sets logs is actually deterministic to allow
 * using the same distribution for multiple readers and writers.
 * Moreover, the logs are picked sequentially from a deterministic pseudorandom
 * permutation of logs. This allows something like the following:
 *  - give loadtest "100:.9,10000:.1" as distribution; so we have 100 hot logs
 *    and a tail of 10k barely used logs,
 *  - give readtestapp "100:1" to read only from 100 hot logs,
 *  - or, give readtestapp "100:0,10000:1" to read only from the tail.
 */

// Parses a string like "10:0.7,10:.2,-" into something
// like {{10, 0.7}, {10, 0.2}, {-1, 0.1}}.
// @return whether the string is parsed successfully.
bool parse_distribution_spec(
    const std::string& str,
    std::vector<std::pair<int, double>>* out_distribution_spec) {
  ld_check(out_distribution_spec);
  std::vector<std::string> tokens;
  folly::split(',', str, tokens, true);
  out_distribution_spec->resize(tokens.size());
  double sum = 0;
  int remainder_index = -1;
  for (size_t i = 0; i < tokens.size(); ++i) {
    std::string& token = tokens[i];
    ld_check(!token.empty());
    if (token.size() < 3) {
      return false;
    }
    try {
      if (*token.rbegin() == '-') {
        if (token.substr(token.size() - 2) == ":-") {
          // "100:-" syntax: 100 logs get the rest of probability.
          (*out_distribution_spec)[i].first =
              folly::to<int>(token.substr(0, token.size() - 2));
        } else if (token == "-" || token == "-:-") {
          // "-" syntax: remaining logs get the rest of probability.
          (*out_distribution_spec)[i].first = -1;
        } else {
          return false;
        }
        if (remainder_index != -1) {
          return false;
        }
        remainder_index = static_cast<int>(i);
      } else {
        if (!folly::split(':',
                          token,
                          (*out_distribution_spec)[i].first,
                          (*out_distribution_spec)[i].second)) {
          return false;
        }
        if ((*out_distribution_spec)[i].second < 0) {
          return false;
        }
        sum += (*out_distribution_spec)[i].second;
      }
    } catch (std::range_error&) {
      return false;
    }
    if ((*out_distribution_spec)[i].first <= 0) {
      return false;
    }
  }

  const double epsilon = 1e-3;

  if (sum > 1 + epsilon) {
    return false;
  }

  if (remainder_index != -1) {
    (*out_distribution_spec)[remainder_index].second = std::max(0., 1 - sum);
  } else if (sum < 1 - epsilon) {
    return false;
  }

  return true;
}

// Pick pseudorandom logs and assign them probabilities as described above.
bool generate_distribution(
    size_t n,
    const std::vector<std::pair<int, double>>& distribution_spec,
    std::vector<double>* out_probabilities) {
  int sum_logs = 0;
  for (const auto& item : distribution_spec) {
    if (item.first != -1) {
      sum_logs += item.first;
    }
  }

  if (sum_logs > n) {
    return false;
  }

  out_probabilities->assign(n, 0);
  std::vector<int> perm(n);
  for (int i = 0; i < n; ++i) {
    perm[i] = i;
  }
  std::mt19937 rng; // initialised with default seed
  std::shuffle(perm.begin(), perm.end(), rng);
  size_t pos = 0;
  for (const auto& item : distribution_spec) {
    // Handle the "-" token.
    int count = item.first == -1 ? n - sum_logs : item.first;

    if (pos + count > n) {
      return false;
    }
    for (size_t i = pos; i < pos + count; ++i) {
      (*out_probabilities)[perm[i]] = item.second / count;
    }

    pos += count;
  }

  return true;
}

}} // namespace facebook::logdevice
