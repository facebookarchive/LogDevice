/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/event_log/EventLogWriter.h"

#include <chrono>
#include <functional>

#include "logdevice/common/AppendRequest.h"
#include "logdevice/common/BackoffTimer.h"
#include "logdevice/common/EventLoop.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/RebuildingTypes.h"
#include "logdevice/common/Worker.h"
#include "logdevice/common/WorkerCallbackHelper.h"
#include "logdevice/common/event_log/EventLogRecord.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

EventLogWriter::EventLogWriter(EventLogStateMachine* event_log)
    : event_log_(event_log) {
  callbackHelper_ =
      std::make_unique<WorkerCallbackHelper<EventLogWriter>>(this);
}

EventLogWriter::~EventLogWriter() = default;

std::unique_ptr<BackoffTimer>
EventLogWriter::createAppendRetryTimer(std::function<void()> callback) {
  auto timer = std::make_unique<ExponentialBackoffTimer>(
      callback, std::chrono::milliseconds(200), std::chrono::seconds(10));
  return std::move(timer);
}

void EventLogWriter::writeNextEventInQueue() {
  if (!appendRequestRetryTimer_) {
    appendRequestRetryTimer_ =
        createAppendRetryTimer([this]() { writeNextEventInQueue(); });
  }

  ld_check(!appendQueue_.empty());
  ld_info("(Queue size: %lu) Writing event %s",
          appendQueue_.size(),
          appendQueue_.front()->describe().c_str());

  auto callback_ticket = callbackHelper_->ticket();

  auto cb = [=](Status st, lsn_t lsn, const std::string& /* unused */) {
    callback_ticket.postCallbackRequest([=](EventLogWriter* writer) {
      if (!writer) {
        // We shut down before the AppendRequest completed.
        return;
      }
      ld_check(!appendQueue_.empty());
      if (st != E::OK) {
        ld_error("Could not write event record: %s. Will retry after %ldms",
                 error_description(st),
                 appendRequestRetryTimer_->getNextDelay().count());
        appendRequestRetryTimer_->activate();
      } else {
        ld_info("Wrote record with lsn %s", lsn_to_string(lsn).c_str());
        appendRequestRetryTimer_->reset();
        // pop the message from the queue and send the next one, if any.
        appendQueue_.pop();
        if (!appendQueue_.empty()) {
          writeNextEventInQueue();
        }
      }
    });
  };

  // Using this mode because we don't care if the event is actually applied.
  auto mode = EventLogStateMachine::WriteMode::CONFIRM_APPEND_ONLY;
  event_log_->writeDelta(*appendQueue_.front(), cb, mode);
}

void EventLogWriter::writeEvent(std::unique_ptr<EventLogRecord> event) {
  ld_info("(Queue size: %lu) Enqueueing event %s",
          appendQueue_.size(),
          event->describe().c_str());

  const bool was_empty = appendQueue_.empty();
  appendQueue_.push(std::move(event));

  if (!was_empty) {
    // There was already events enqueued for appends. This means there is either
    // an append in flight or we are waiting for the backoff timer to expire.
    return;
  }

  writeNextEventInQueue();
}

}} // namespace facebook::logdevice
