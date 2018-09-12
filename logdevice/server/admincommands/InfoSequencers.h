/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <unordered_map>
#include <vector>

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/AllSequencers.h"
#include "logdevice/common/MetaDataLogWriter.h"
#include "logdevice/common/Processor.h"
#include "logdevice/server/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

// TODO 7467469: need to modify for per-epoch sequencers to expose more info

typedef AdminCommandTable<logid_t,                  // data log id
                          logid_t,                  // metadata log id
                          std::string,              // state description
                          epoch_t,                  // epoch
                          admin_command_table::LSN, // next lsn
                          admin_command_table::LSN, // last released
                          admin_command_table::LSN, // meta last released
                          admin_command_table::LSN, // last known good
                          size_t,       // number of appends in flight
                          std::string,  // last used (ms)
                          std::string,  // state duration (ms)
                          std::string,  // nodeset state
                          epoch_t,      // preempted epoch
                          node_index_t, // prempted by
                          epoch_t,      // draining epoch
                          bool,         // metadata log written
                          admin_command_table::LSN, // trim point
                          std::string               // last byte offset
                          >
    InfoSequencersTable;

class InfoSequencers : public AdminCommand {
  logid_t::raw_type logid_{LOGID_INVALID};
  bool json_ = false;

 public:
  void getOptions(boost::program_options::options_description& opts) override {
    opts.add_options()("json", boost::program_options::bool_switch(&json_));
    opts.add_options()(
        "logid", boost::program_options::value<logid_t::raw_type>(&logid_));
  }

  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("logid", 1);
  }

  std::string getUsage() override {
    return "info sequencers [data_logid/metadata_logid] [--json]";
  }

  std::string getNodesetState(const Sequencer& seq) {
    // There's a small chance of "torn read" here but it's not a big deal.
    std::shared_ptr<CopySetManager> csm = seq.getCurrentCopySetManager();
    std::shared_ptr<const EpochMetaData> meta = seq.getCurrentMetaData();

    if (csm == nullptr || meta == nullptr) {
      return "";
    }
    const NodeSetState* nodeset_state = csm->getNodeSetState().get();
    ld_check(nodeset_state);

    std::unordered_map<uint8_t, std::vector<ShardID>> map;
    for (auto& shard : meta->shards) {
      if (!nodeset_state->containsShard(shard)) {
        RATELIMIT_ERROR(std::chrono::seconds(10),
                        10,
                        "Something unlikely happened (torn read of copyset "
                        "manager and epoch "
                        "metadata). If you see this often, there's a bug.");
        continue;
      }
      auto reason = nodeset_state->getNotAvailableReason(shard);
      map[static_cast<uint8_t>(reason)].push_back(shard);
    }

    std::string res;

    auto add = [&](NodeSetState::NotAvailableReason r, char n) {
      auto it = map.find(static_cast<uint8_t>(r));
      if (it == map.end()) {
        return;
      }
      if (!res.empty()) {
        res += " ";
      }
      res += n;
      res += ": " + toString(it->second);
    };

    add(NodeSetState::NotAvailableReason::NONE, 'H');
    add(NodeSetState::NotAvailableReason::LOW_WATERMARK_NOSPC, 'L');
    add(NodeSetState::NotAvailableReason::OVERLOADED, 'O');
    add(NodeSetState::NotAvailableReason::NO_SPC, 'S');
    add(NodeSetState::NotAvailableReason::UNROUTABLE, 'U');
    add(NodeSetState::NotAvailableReason::STORE_DISABLED, 'D');
    add(NodeSetState::NotAvailableReason::SLOW, 'G'); // in gray list
    add(NodeSetState::NotAvailableReason::PROBING, 'P');
    static_assert((int)NodeSetState::NotAvailableReason::Count == 8,
                  "Added something to NodeSetState::NotAvailableReason enum? "
                  "Add it here too!");

    return res;
  }

  void dumpSequencer(InfoSequencersTable& table, const Sequencer& seq) {
    auto last_append = seq.getTimeSinceLastAppend();
    MetaDataLogWriter* meta_writer = seq.getMetaDataLogWriter();

    table.next()
        .set<0>(seq.getLogID())
        .set<1>(MetaDataLog::metaDataLogID(seq.getLogID()))
        .set<2>(seq.stateString(seq.getState()))
        .set<3>(seq.getCurrentEpoch())
        .set<4>(seq.getNextLSN())
        .set<5>(seq.getLastReleased());
    if (meta_writer) {
      table.set<6>(meta_writer->getLastReleased(false /* recover */));
    }
    table.set<7>(seq.getLastKnownGood()).set<8>(seq.getNumAppendsInFlight());
    if (last_append != std::chrono::milliseconds::max()) {
      table.set<9>(folly::to<std::string>(last_append.count()));
    }

    auto state_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch() -
        seq.getLastStateChangeTimestamp());
    table.set<10>(folly::to<std::string>(state_duration.count()));
    table.set<11>(getNodesetState(seq));

    const Seal seal = seq.getSealRecord();
    if (seal.valid()) {
      table.set<12>(seal.epoch);
      table.set<13>(seal.seq_node.index());
    }

    table.set<14>(seq.getDrainingEpoch());
    auto metadata = seq.getCurrentMetaData();
    table.set<15>(metadata ? metadata->writtenInMetaDataLog() : false);
    table.set<16>(seq.getTrimPoint());

    auto tailRecord = seq.getTailRecord();
    uint64_t bo;
    if (tailRecord == nullptr) {
      bo = BYTE_OFFSET_INVALID;
    } else {
      bo = tailRecord->header.u.byte_offset;
    }

    if (bo == BYTE_OFFSET_INVALID) {
      table.set<17>("BYTE_OFFSET_INVALID");
    } else {
      table.set<17>(std::to_string(bo));
    }
  }

  void run() override {
    InfoSequencersTable table(!json_,
                              "Log ID",
                              "MetaData Log ID",
                              "State",
                              "Epoch",
                              "Next LSN",
                              "Last released",
                              "Meta last released",
                              "Last known good",
                              "In flight",
                              "Last used (ms)",
                              "State duration (ms)",
                              "Nodeset state",
                              "Preempted epoch",
                              "Preempted by",
                              "Draining epoch",
                              "Metadata log written",
                              "Trim point",
                              "Last Byte Offset");

    if (logid_t(logid_) != LOGID_INVALID) {
      // MetaData log and Data log have the same sequencer.
      const logid_t datalog_id = MetaDataLog::dataLogID(logid_t(logid_));
      std::shared_ptr<const Sequencer> seq =
          server_->getProcessor()->allSequencers().findSequencer(datalog_id);
      if (seq == nullptr) {
        if (!json_) {
          out_.printf("Cannot find sequencer for log %lu\r\n", logid_);
        }
      } else {
        dumpSequencer(table, *seq);
      }
    } else {
      for (const auto& seq :
           server_->getProcessor()->allSequencers().accessAll()) {
        dumpSequencer(table, seq);
      }
    }

    json_ ? table.printJson(out_) : table.print(out_);
  }
};

}}} // namespace facebook::logdevice::commands
