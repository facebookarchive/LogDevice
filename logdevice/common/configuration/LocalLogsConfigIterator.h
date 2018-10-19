/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <utility>

#include <boost/concept/assert.hpp>
#include <boost/container/flat_map.hpp>
#include <boost/iterator/iterator_adaptor.hpp>
#include <boost/type_traits/is_same.hpp>
#include <folly/Memory.h>
#include <folly/Optional.h>
#include <folly/Range.h>
#include <folly/dynamic.h>
#include <folly/synchronization/CallOnce.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/configuration/LogsConfig.h"
#include "logdevice/common/configuration/ServerConfig.h"
#include "logdevice/common/configuration/logs/LogsConfigTree.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"
#include "logdevice/include/types.h"

namespace facebook { namespace logdevice { namespace configuration {

class LocalLogsConfigIterator
    : public boost::iterator_adaptor<
          LocalLogsConfigIterator,
          logsconfig::LogsConfigTree::const_iterator> {
 public:
  LocalLogsConfigIterator() : iterator_adaptor_() {}
  explicit LocalLogsConfigIterator(const InternalLogs* internal_logs,
                                   const logsconfig::LogsConfigTree* user_logs,
                                   const iterator_adaptor_::base_type& p,
                                   bool past_config_tree)
      : LocalLogsConfigIterator::iterator_adaptor_(p),
        internal_logs_(internal_logs),
        user_logs_(user_logs),
        past_config_tree_(past_config_tree) {
    if (!past_config_tree_) {
      if (base_reference() == user_logs_->logsEnd()) {
        past_config_tree_ = true;
        base_reference() = internal_logs_->logsBegin();
      }
    }
  }

  void increment() {
    if (!past_config_tree_) {
      ld_assert(base_reference() != user_logs_->logsEnd());
      ++base_reference();
      if (base_reference() == user_logs_->logsEnd()) {
        past_config_tree_ = true;
        base_reference() = internal_logs_->logsBegin();
      }
    } else {
      if (base_reference() != internal_logs_->logsEnd()) {
        ++base_reference();
      }
    }
  }

 private:
  friend class boost::iterator_core_access;
  const InternalLogs* internal_logs_;
  const logsconfig::LogsConfigTree* user_logs_;
  bool past_config_tree_{false};
};

class LocalLogsConfigReverseIterator
    : public boost::iterator_adaptor<
          LocalLogsConfigReverseIterator,
          logsconfig::LogsConfigTree::const_reverse_iterator> {
 public:
  LocalLogsConfigReverseIterator() : iterator_adaptor_() {}
  explicit LocalLogsConfigReverseIterator(
      const InternalLogs* internal_logs,
      const logsconfig::LogsConfigTree* user_logs,
      const iterator_adaptor_::base_type& p,
      bool past_config_tree)
      : LocalLogsConfigReverseIterator::iterator_adaptor_(p),
        internal_logs_(internal_logs),
        user_logs_(user_logs),
        past_config_tree_(past_config_tree) {
    if (!past_config_tree_) {
      if (base_reference() == user_logs_->logsREnd()) {
        past_config_tree_ = true;
        base_reference() = internal_logs_->logsRBegin();
      }
    }
  }

  void increment() {
    if (!past_config_tree_) {
      ld_assert(base_reference() != user_logs_->logsREnd());
      ++base_reference();
      if (base_reference() == user_logs_->logsREnd()) {
        past_config_tree_ = true;
        base_reference() = internal_logs_->logsRBegin();
      }
    } else {
      if (base_reference() != internal_logs_->logsREnd()) {
        ++base_reference();
      }
    }
  }

 private:
  friend class boost::iterator_core_access;
  const InternalLogs* internal_logs_;
  const logsconfig::LogsConfigTree* user_logs_;
  bool past_config_tree_{false};
};

}}} // namespace facebook::logdevice::configuration
