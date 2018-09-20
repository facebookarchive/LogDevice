/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/HELLOPrincipalParser.h"

#include <cstring>
#include "logdevice/common/checks.h"
#include "logdevice/common/PrincipalIdentity.h"

namespace facebook { namespace logdevice {

PrincipalIdentity HELLOPrincipalParser::getPrincipal(const void* data,
                                                     size_t size) const {
  const char* credentials = reinterpret_cast<const char*>(data);
  ld_check(data != nullptr);
  size_t str_size = strnlen(credentials, size);

  return str_size == 0
      ? PrincipalIdentity(Principal::UNAUTHENTICATED)
      : PrincipalIdentity(Principal::AUTHENTICATED,
                          std::make_pair(PrincipalIdentity::IDENTITY_USER,
                                         std::string(credentials, str_size)));
}
}} // namespace facebook::logdevice
