/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <folly/Memory.h>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/request_util.h"
#include "logdevice/server/admincommands/AdminCommand.h"
#include "logdevice/server/locallogstore/IteratorTracker.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoIterators : public AdminCommand {
 private:
  folly::Optional<logid_t> logid_;
  bool json_ = false;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "json", boost::program_options::bool_switch(&json_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
  }
  std::string getUsage() override {
    return "info iterators [--json] [log_id]";
  }

  void run() override {
    InfoIteratorsTable table(!json_,
                             "Column family",
                             "Log ID",
                             "Is tailing",
                             "Is blocking",
                             "Type",
                             "Rebuilding",
                             "High Level ID",
                             "Created timestamp",
                             "More context",
                             "Last seek LSN",
                             "Last seek timestamp",
                             "Version");

    auto info = IteratorTracker::get()->getDebugInfo();

    auto type_to_string = [](TrackableIterator::IteratorType v) -> std::string {
      switch (v) {
        case TrackableIterator::IteratorType::DATA:
          return "DATA";
        case TrackableIterator::IteratorType::CSI:
          return "CSI";
        case TrackableIterator::IteratorType::CSI_WRAPPER:
          return "CSI_WRAPPER";
        case TrackableIterator::IteratorType::PARTITIONED:
          return "PARTITIONED";
        case TrackableIterator::IteratorType::PARTITIONED_ALL_LOGS:
          return "PARTITIONED_ALL_LOGS";
      }
      ld_check(false);
      return std::string();
    };

    for (auto& row : info) {
      table.next()
          .set<0>(row.imm.column_family_name)
          .set<1>(row.imm.log_id)
          .set<2>(row.imm.tailing)
          .set<3>(row.imm.blocking)
          .set<4>(type_to_string(row.imm.type))
          .set<5>(row.imm.created_by_rebuilding)
          .set<6>(row.imm.high_level_id)
          .set<7>(row.imm.created.toMilliseconds().count());
      if (row.mut.more_context) {
        table.set<8>(std::string(row.mut.more_context));
      }
      if (row.mut.last_seek_lsn != LSN_INVALID) {
        table.set<9>(row.mut.last_seek_lsn)
            .set<10>(row.mut.last_seek_time)
            .set<11>(row.mut.last_seek_version);
      }
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
