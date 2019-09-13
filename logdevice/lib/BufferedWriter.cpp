/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/include/BufferedWriter.h"

#include <folly/Memory.h>

#include "logdevice/common/StreamWriterAppendSink.h"
#include "logdevice/common/buffered_writer/BufferedWriterImpl.h"
#include "logdevice/common/util.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/lib/ClientProcessor.h"

namespace facebook { namespace logdevice {

using LogOptions = BufferedWriter::LogOptions;

std::unique_ptr<BufferedWriter>
BufferedWriter::create(std::shared_ptr<Client> client,
                       AppendCallback* callback,
                       Options options) {
  ClientImpl* client_impl = checked_downcast<ClientImpl*>(client.get());
  if (options.retry_initial_delay.count() < 0) {
    options.retry_initial_delay = 2 * client_impl->getTimeout();
  }

  auto memory_limit_mb = options.memory_limit_mb;

  bool is_stream = (options.mode == BufferedWriter::Options::Mode::STREAM);
  BufferedWriterAppendSink* sink;
  if (is_stream) {
    sink = new StreamWriterAppendSink(
        std::static_pointer_cast<Processor>(client_impl->getProcessorPtr()),
        std::make_unique<ClientBridgeImpl>(client_impl),
        client_impl->getTimeout(),
        chrono_expbackoff_t<std::chrono::milliseconds>(
            options.retry_initial_delay, options.retry_max_delay));
  } else {
    sink = (BufferedWriterAppendSink*)client_impl;
  }
  auto get_log_options = [opts = std::move(options)](logid_t) -> LogOptions {
    return opts;
  };
  auto buffered_writer = std::make_unique<BufferedWriterImpl>(
      new ProcessorProxy(&client_impl->getProcessor()),
      callback,
      get_log_options,
      memory_limit_mb,
      sink,
      client_impl->stats());
  buffered_writer->pinClient(client);
  if (is_stream) {
    buffered_writer->ownAppendSink();
  }
  return std::move(buffered_writer);
}

int BufferedWriter::append(logid_t log_id,
                           std::string&& payload,
                           AppendCallback::Context cb_context,
                           AppendAttributes&& attrs) {
  return impl()->append(
      log_id, std::move(payload), std::move(cb_context), std::move(attrs));
}

std::vector<Status> BufferedWriter::append(std::vector<Append>&& appends) {
  return impl()->append(std::move(appends));
}

int BufferedWriter::flushAll() {
  return impl()->flushAll();
}

BufferedWriterImpl* BufferedWriter::impl() {
  return static_cast<BufferedWriterImpl*>(this);
}

}} // namespace facebook::logdevice
