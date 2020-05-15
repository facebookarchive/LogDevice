/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/ops/ldquery/Utils.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace ldquery {

std::string s(const folly::Optional<int>& val) {
  return s(val ? *val : 0);
}

std::string s(const bool& val) {
  return s((int)val & 1);
}

std::string s(const folly::Optional<std::chrono::seconds>& val) {
  return s(val ? val->count() : 0);
}

std::string s(const folly::Optional<std::chrono::milliseconds>& val) {
  return s(val ? val->count() : 0);
}


bool match_likexpr(std::string str, std::string likexpr) {

  ld_debug("str '%s', likexpr '%s'", str.c_str(), likexpr.c_str());
  // expr_index
  int ei = 0;
  int ej = 0;
  // str_index
  int si = 0;
  int sj = 0;

  bool has_percentage = false;
  size_t underscore_num = 0;
  std::string word;
  std::string wcards;

  while (ei < likexpr.size()) {
    ej = likexpr.find_first_of("%_", ei);
    if (ei == ej) {
      // entered wildcards subpart, like %%, %_, _%, etc
      // setup match conditions and continue the loop
      while(likexpr[ei] == '%' || likexpr[ei] == '_') {
        if (likexpr[ei] == '%') {
          has_percentage = true;
        }
        if (likexpr[ei] == '_') {
          underscore_num++;
        }
        ei++;
      }
    } else {
      // entered normal word subpart, like abc, cbd, etc
      // try to match the subword and contine the loop
      std::string to_match = ej == std::string::npos?
        likexpr.substr(ei) : likexpr.substr(ei, ej - ei);

      ld_debug("subword to match: '%s', has_percentage: %s, underscore_num: %d",
              to_match.c_str(), has_percentage? "true" : "false", underscore_num);

      // try matching
      // 1. skip the underscore_num of chars
      si += underscore_num;
      underscore_num = 0;

      // 2. find the matched substr
      if (has_percentage) {
        ld_debug("try to find '%s' from index %d", to_match.c_str(), si);
        sj = str.find(to_match, si);
        if (sj == std::string::npos) {
          return false;
        }
        has_percentage = false;
      } else {
        ld_debug("try to match '%s' from %d", to_match.c_str(), si);
        if (!str.rfind(to_match, si) == si) {
          return false;
        }
      }

      // 3. update indexes
      // avoid to use ej directly
      si = sj + to_match.size();
      ei += to_match.size();
    }
  }

  si += underscore_num;
  if (si == str.size()) {
    return true;
  } else if (si > str.size()) {
    return false;
  } else if (has_percentage) {
    // si < str.size() and has_percentage
    return true;
  } else {
    // si < str.size() and there is no percentage symbol
    return false;
  }

}

}}} // namespace facebook::logdevice::ldquery
