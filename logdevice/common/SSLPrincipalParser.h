/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <string>

#include <folly/ssl/OpenSSLPtrTypes.h>

#include "logdevice/common/PrincipalIdentity.h"

namespace facebook { namespace logdevice {

/**
 * @file an abstract interface used to obtain the principal for a connection
 *       from its SSL certficate after authentication has occurred.
 */

class SSLPrincipalParser {
 public:
  virtual ~SSLPrincipalParser(){};

  /**
   * Parses the SSL authentication obtained from authentication and extracts the
   * embedded principal or assigned a principle to the output argument.
   *
   * @param cert  The SSL certificate from the connection
   *
   * @return the assigned principal.
   * If a principal cannot be extracted, then principals::INVALID is returned in
   * type field.
   */
  virtual PrincipalIdentity getPrincipal(X509* cert) const = 0;

  /**
   * An API that's called after SSL certificates are no longer needed and can be
   * evicted from memory. It's an optimization to save server/client memory and
   * it doesn't affect the correctness of the program if it's not implemented.
   */
  virtual void clearCertificates(SSL* ssl) const {}
};

}} // namespace facebook::logdevice
