/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <boost/program_options.hpp>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

boost::program_options::typed_value<ShardID>*
shard_id_value(ShardID* out, bool default_value = true) {
  auto ret = boost::program_options::value<ShardID>(out);
  if (default_value) {
    ret->default_value(*out, out->toString());
  }
  return ret;
}

template <typename charT>
inline void validate(boost::any& v,
                     const std::vector<std::basic_string<charT>>& xs,
                     facebook::logdevice::ShardID*,
                     int) {
  using namespace boost::program_options;
  validators::check_first_occurrence(v);
  std::basic_string<charT> str(validators::get_single_string(xs));

  auto extract_nonnegative_int = [](const std::string& s, char prefix) {
    if (s.size() < 2 || s[0] != prefix) {
      return -1;
    }
    int rv = folly::to<int>(s.substr(1));
    return rv >= 0 ? rv : -1;
  };

  std::vector<std::string> tokens;
  folly::split(":", str, tokens, /* ignoreEmpty */ false);
  if (tokens.size() != 2) {
    throw validation_error(validation_error::invalid_option_value);
  }
  int index = extract_nonnegative_int(tokens[0], 'N');
  if (index < 0 || index > std::numeric_limits<node_index_t>::max()) {
    throw validation_error(validation_error::invalid_option_value);
  }
  int shard_id = extract_nonnegative_int(tokens[1], 'S');
  if (shard_id < 0 || shard_id > std::numeric_limits<shard_index_t>::max()) {
    throw validation_error(validation_error::invalid_option_value);
  }

  v = boost::any(facebook::logdevice::ShardID(index, shard_id));
}

}} // namespace facebook::logdevice
