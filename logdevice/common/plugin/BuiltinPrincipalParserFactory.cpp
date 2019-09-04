/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/plugin/BuiltinPrincipalParserFactory.h"

#include "logdevice/common/HELLOPrincipalParser.h"
#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice {

std::unique_ptr<PrincipalParser> BuiltinPrincipalParserFactory::
operator()(AuthenticationType type) {
  std::unique_ptr<PrincipalParser> principal_parser = nullptr;

  switch (type) {
    case AuthenticationType::MAX:
      ld_check(false);
      break;
    case AuthenticationType::NONE:
      break;
    case AuthenticationType::SELF_IDENTIFICATION:
      principal_parser.reset(new HELLOPrincipalParser());
      break;
    case AuthenticationType::SSL:
      ld_critical("AuthenticationType::SSL is not supported in builtin "
                  "PrincipalParser factory.");
      break;
  }

  return principal_parser;
}

}} // namespace facebook::logdevice
