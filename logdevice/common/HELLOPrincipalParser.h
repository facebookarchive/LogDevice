/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/PrincipalParser.h"

namespace facebook { namespace logdevice {

/**
 * @file This is an implementation of PrincipalParser. It parses the credential
 * field of the HELLO_Message and determines the appropriate principle for the
 * connection
 */

class HELLOPrincipalParser : public PrincipalParser {
 public:
  explicit HELLOPrincipalParser() : PrincipalParser() {}

  ~HELLOPrincipalParser() override{};

  /**
   * See PrincipalParser.h
   *
   * If the credential field is empty then the Principal::UNAUTHENTICATED will
   * be returned. If the credential field is non-empty then the output will be
   * the value of credentials.
   */
  PrincipalIdentity getPrincipal(const void* data, size_t size) const override;

  AuthenticationType getAuthenticationType() const override {
    return AuthenticationType::SELF_IDENTIFICATION;
  }
};

}} // namespace facebook::logdevice
