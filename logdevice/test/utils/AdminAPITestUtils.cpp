/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/AdminAPITestUtils.h"

namespace facebook { namespace logdevice {

std::unique_ptr<thrift::AdminAPIAsyncClient>
create_admin_client(folly::EventBase* eventBase,
                    IntegrationTestUtils::Cluster* cluster,
                    node_index_t node_id) {
  // This is a very bad way of handling Folly AsyncSocket flakiness in
  // combination with unix sockets.
  for (auto i = 0; i < 5; i++) {
    folly::SocketAddress address = cluster->getNode(node_id).getAdminAddress();
    auto transport =
        apache::thrift::async::TAsyncSocket::newSocket(eventBase, address);
    auto channel = apache::thrift::HeaderClientChannel::newChannel(transport);
    channel->setTimeout(5000);
    if (channel->good()) {
      return std::make_unique<thrift::AdminAPIAsyncClient>(std::move(channel));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5000));
  }
  return nullptr;
}

void retry_until_ready(int32_t attempts,
                       std::chrono::seconds delay,
                       folly::Function<void()> operation) {
  do {
    // let's keep trying until the server hits that version
    try {
      operation();
      // No exception thrown.
      return;
    } catch (thrift::NodeNotReady& e) {
      ld_info("Got NodeNotReady exception, attempts left: %i", attempts);
      // retry
      std::this_thread::sleep_for(delay);
      attempts--;
    }
  } while (attempts > 0);
}
}} // namespace facebook::logdevice
