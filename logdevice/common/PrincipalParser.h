/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/SecurityInformation.h"

namespace facebook { namespace logdevice {

/**
 * @file an abstract interface used to obtain the principal for a connection
 *       after authentication has occurred.
 */

class PrincipalParser {
 public:
  virtual ~PrincipalParser(){};

  /**
   * Parses the data obtained from authentication, for example the subject of a
   * X509 certificate or the credential field of a HELLO_Message, and extracts
   * the embedded principal or assigned a principle to the output argument.
   *
   * @param data  The data obtained from authentication
   * @param size  The size of the data field
   *
   * @return the assigned principal.
   * If a principal cannot be extracted, then principals::INVALID is returned in
   * type field.
   */
  virtual PrincipalIdentity getPrincipal(const void* data,
                                         size_t size) const = 0;

  /**
   * Returns the AuthenticationTyoe that the PrincipalParser is intended to
   * work with.
   */
  virtual AuthenticationType getAuthenticationType() const = 0;
};

}} // namespace facebook::logdevice
