/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/AuthoritativeStatus.h"

#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

std::string toString(const AuthoritativeStatus& st) {
  switch (st) {
    case AuthoritativeStatus::FULLY_AUTHORITATIVE:
      return "FULLY_AUTHORITATIVE";
    case AuthoritativeStatus::UNDERREPLICATION:
      return "UNDERREPLICATION";
    case AuthoritativeStatus::AUTHORITATIVE_EMPTY:
      return "AUTHORITATIVE_EMPTY";
    case AuthoritativeStatus::UNAVAILABLE:
      return "UNAVAILABLE";
    case AuthoritativeStatus::Count:
      break;
  }
  ld_check(false);
  return "invalid";
}

std::string toShortString(const AuthoritativeStatus& st) {
  switch (st) {
    case AuthoritativeStatus::FULLY_AUTHORITATIVE:
      return "FA";
    case AuthoritativeStatus::UNDERREPLICATION:
      return "UR";
    case AuthoritativeStatus::AUTHORITATIVE_EMPTY:
      return "AE";
    case AuthoritativeStatus::UNAVAILABLE:
      return "UA";
    case AuthoritativeStatus::Count:
      break;
  }
  ld_check(false);
  return "invalid";
}

std::vector<std::string> allAuthoritativeStatusStrings() {
  std::vector<std::string> res;
  for (int i = 0; i < (int)AuthoritativeStatus::Count; ++i) {
    res.push_back(toString((AuthoritativeStatus)i));
  }
  return res;
}

bool parseAuthoritativeStatus(const std::string& s,
                              AuthoritativeStatus& out_status) {
  for (int i = 0; i < (int)AuthoritativeStatus::Count; ++i) {
    auto a = (AuthoritativeStatus)i;
    if (s == toString(a) || s == toShortString(a)) {
      out_status = a;
      return true;
    }
  }
  return false;
}

}} // namespace facebook::logdevice
