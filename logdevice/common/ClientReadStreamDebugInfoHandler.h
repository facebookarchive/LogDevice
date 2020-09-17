/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>

#include <folly/container/F14Map.h>
#include <folly/io/async/AsyncTimeout.h>
#include <folly/io/async/EventBase.h>

#include "logdevice/common/ReadStreamDebugInfoSamplingConfig.h"
#include "logdevice/common/client_read_stream/AllClientReadStreams.h"
#include "logdevice/common/client_read_stream/ClientReadStream.h"
#include "logdevice/common/plugin/PluginRegistry.h"
#include "logdevice/include/types.h"

namespace facebook::logdevice {

/**
 * The debug handler is responsible for keeping track of requests
 * for sampling information for specific read streams. These sampling requests
 * are defined in AllReadStreamsDebugConfigs and specify for which
 * csid/reader name to sample information and for how long.
 * This class is not thread-safe.  Each worker thread is meant to have
 * its own instance
 */
class ClientReadStreamDebugInfoHandler {
 public:
  using TimeoutType = folly::TimeoutManager::timeout_type;

  ClientReadStreamDebugInfoHandler(
      const std::string& csid,
      folly::EventBase* eb,
      TimeoutType sampling_interval,
      std::shared_ptr<PluginRegistry> plugin_regsitry,
      const std::string& config_path,
      AllClientReadStreams& allClientReadStreams);

  virtual ~ClientReadStreamDebugInfoHandler();

 protected:
  using StreamContainer = folly::F14FastMap<read_stream_id_t,
                                            std::chrono::seconds,
                                            read_stream_id_t::Hash>;

  virtual void sampleDebugInfo() noexcept;
  void addStream(const ClientReadStream&);
  void removeStream(const ClientReadStream&);

  const StreamContainer& streams() const {
    return streams_;
  }

 private:
  using Config = configuration::all_read_streams_debug_config::thrift::
      AllReadStreamsDebugConfigs;

  void schedule();
  void eraseExpired() noexcept;
  void buildIndex(const Config& config);
  void registerStream(const read_stream_id_t& id,
                      const std::string& reader_name);

  const std::string csid_;
  folly::F14FastMap<std::string, std::chrono::seconds> rules_;

  folly::EventBase* eb_;
  TimeoutType sampling_interval_;

  ReadStreamDebugInfoSamplingConfig config_;
  AllClientReadStreams& allClientReadStreams_;

  std::unique_ptr<folly::AsyncTimeout> sampleDebugInfoTimer_;
  StreamContainer streams_;
};

} // namespace facebook::logdevice
