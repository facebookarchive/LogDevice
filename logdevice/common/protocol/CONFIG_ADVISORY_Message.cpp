/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/protocol/CONFIG_ADVISORY_Message.h"

#include "logdevice/common/Processor.h"
#include "logdevice/common/Sender.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/configuration/UpdateableConfig.h"
#include "logdevice/common/protocol/CONFIG_FETCH_Message.h"

namespace facebook { namespace logdevice {

template <>
Message::Disposition CONFIG_ADVISORY_Message::onReceived(const Address& from) {
  ld_debug("CONFIG_ADVISORY received from %s",
           Sender::describeConnection(from).c_str());
  Worker* worker = Worker::onThisThread();
  Sender& sender = worker->sender();
  sender.setPeerConfigVersion(from, *this, header_.version);

  auto updateable_server_config =
      worker->getUpdateableConfig()->updateableServerConfig();
  config_version_t current_version =
      updateable_server_config->get()->getVersion();
  // If our config version is less than the one received, update the last known
  // config version and fetch an updated version from the sender
  if (current_version < header_.version &&
      updateable_server_config->updateLastKnownVersion(header_.version)) {
    ld_info("Received CONFIG_ADVISORY from %s with more recent config version "
            "(%u > %u)",
            Sender::describeConnection(from).c_str(),
            header_.version.val(),
            current_version.val());
    if (Worker::settings().client_config_fetch_allowed) {
      ld_info("Fetching new config from %s",
              Sender::describeConnection(from).c_str());
      CONFIG_FETCH_Header header{CONFIG_FETCH_Header::ConfigType::MAIN_CONFIG};
      int rv = sender.sendMessage(
          std::make_unique<CONFIG_FETCH_Message>(header), from);
      if (rv != 0) {
        ld_warning("Failed to fetch config from %s with error %s",
                   Sender::describeConnection(from).c_str(),
                   error_description(err));
      }
    } else {
      ld_info("Fetching new config from source");
      int rv = updateable_server_config->getUpdater()->fetchFromSource();
      if (rv != 0) {
        ld_warning("Failed to fetch config from source with error %s",
                   error_description(err));
      }
    }
  }
  return Disposition::NORMAL;
}

template <>
bool CONFIG_ADVISORY_Message::allowUnencrypted() const {
  return true;
}

}} // namespace facebook::logdevice
