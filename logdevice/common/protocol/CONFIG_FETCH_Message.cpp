/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "CONFIG_FETCH_Message.h"

#include <folly/Memory.h>

#include "logdevice/common/configuration/Configuration.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/protocol/CONFIG_CHANGED_Message.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition CONFIG_FETCH_Message::onReceived(const Address& from) {
  ld_info("CONFIG_FETCH received from %s",
          Sender::describeConnection(from).c_str());

  if (header_.config_type == CONFIG_FETCH_Header::ConfigType::LOGS_CONFIG) {
    ld_error("CONFIG_FETCH for logs config is currently not supported.");
    err = E::BADMSG;
    return Disposition::ERROR;
  }

  Worker* worker = Worker::onThisThread();
  auto server_config = worker->getConfig()->serverConfig();

  ServerConfig::ConfigMetadata metadata =
      server_config->getMainConfigMetadata();
  CONFIG_CHANGED_Header hdr = {
      static_cast<uint64_t>(metadata.modified_time.count()),
      server_config->getVersion(),
      server_config->getServerOrigin(),
      CONFIG_CHANGED_Header::ConfigType::MAIN_CONFIG,
      CONFIG_CHANGED_Header::Action::UPDATE};
  metadata.hash.copy(hdr.hash, sizeof hdr.hash);
  std::unique_ptr<CONFIG_CHANGED_Message> msg =
      std::make_unique<CONFIG_CHANGED_Message>(
          hdr, server_config->toString(nullptr, true));

  int rv = worker->sender().sendMessage(std::move(msg), from);
  if (rv != 0) {
    ld_error("Sending CONFIG_CHANGED_Message to %s failed with error %s",
             from.toString().c_str(),
             error_description(err));
  }
  return Disposition::NORMAL;
}

}} // namespace facebook::logdevice
