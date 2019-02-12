/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/ClientHelloInfoTracer.h"

#include <algorithm>
#include <memory>
#include <string>

#include <folly/String.h>

#include "logdevice/common/Sockaddr.h"
#include "logdevice/include/LogAttributes.h"

namespace facebook { namespace logdevice {

ClientHelloInfoTracer::ClientHelloInfoTracer(
    std::shared_ptr<TraceLogger> logger)
    : SampledTracer(std::move(logger)) {}

void ClientHelloInfoTracer::traceClientHelloInfo(
    const folly::Optional<folly::dynamic>& json,
    const PrincipalIdentity& principal,
    ConnectionType conn_type,
    const Status status) {
  if (json.hasValue() && !(*json).isObject()) {
    RATELIMIT_ERROR(std::chrono::seconds(1),
                    10,
                    "Invalid client build info. "
                    "Expecting folly dynamic object");
    return;
  }

  auto sample_builder = [&]() -> std::unique_ptr<TraceSample> {
    auto sample = std::make_unique<TraceSample>();

    // include basic client socket description
    sample->addNormalValue("client_address", principal.client_address);

    // extra information on the hello / status
    sample->addNormalValue("ack_status_code", error_name(status));

    sample->addNormalValue(
        "connection_type", connectionTypeToString(conn_type));

    std::vector<std::string> identities;
    for (auto& identity : principal.identities) {
      identities.push_back(identity.first + ":" + identity.second);
    }

    sample->addNormVectorValue("identities", identities);
    sample->addNormalValue("csid", principal.csid);

    if (json.hasValue()) {
      // dynamically build the trace based on the field name prefixes
      for (const auto& pair : (*json).items()) {
        auto key_piece = pair.first.stringPiece();
        folly::dynamic val = pair.second;

        // do not care about logging null / empty values
        if (val.isNull()) {
          continue;
        }

        if (key_piece.startsWith("str_") && val.isString()) {
          sample->addNormalValue(pair.first.asString(), val.asString());
        } else if (key_piece.startsWith("int_") && val.isInt()) {
          sample->addIntValue(pair.first.asString(), val.asInt());
        } else {
          RATELIMIT_WARNING(std::chrono::seconds(1),
                            10,
                            "Unknown build info field '%s' from '%s'",
                            pair.first.c_str(),
                            principal.client_address.c_str());
        }
      }
    }
    return sample;
  };
  publish(CLIENTINFO_TRACER, sample_builder);
}
}} // namespace facebook::logdevice
