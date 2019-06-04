/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/HELLOPrincipalParser.h"

#include <cstring>

#include "folly/String.h"
#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/checks.h"

namespace facebook { namespace logdevice {

PrincipalIdentity HELLOPrincipalParser::getPrincipal(const void* data,
                                                     size_t size) const {
  const char* credentials = reinterpret_cast<const char*>(data);
  ld_check(data != nullptr);
  size_t str_size = strnlen(credentials, size);
  if (str_size == 0) {
    return PrincipalIdentity(Principal::UNAUTHENTICATED);
  }

  std::string creds(credentials, str_size);
  std::string idType, identity;
  if (folly::split(':', creds, idType, identity) &&
      PrincipalIdentity::isValidIdentityType(idType)) {
    return PrincipalIdentity(
        Principal::AUTHENTICATED,
        std::make_pair(std::move(idType), std::move(identity)));
  }

  return PrincipalIdentity(
      Principal::AUTHENTICATED,
      std::make_pair(PrincipalIdentity::IDENTITY_USER, std::move(creds)));
}
}} // namespace facebook::logdevice
