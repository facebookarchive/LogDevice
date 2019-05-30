/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/AdminCommandTable.h"
#include "logdevice/common/Processor.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class InfoConfig : public AdminCommand {
 public:
  void getOptions(boost::program_options::options_description& opts) override {
    using namespace boost::program_options;

    // clang-format off
    opts.add_options()
      ("metadata",
       bool_switch(&metadata_only_)
         ->default_value(metadata_only_),
       "print metadata instead of the whole config")

      ("json",
       bool_switch(&json_)->default_value(json_),
       "output json format")

      ("hash",
       bool_switch(&hash_only_)
         ->default_value(hash_only_),
       "print just a hash instead of the whole config")

      ;
    // clang-format on
  }

  void run() override {
    std::shared_ptr<Configuration> config =
        server_->getProcessor()->config_->get();
    if (metadata_only_) {
      metadata(*config);
    } else if (hash_only_) {
      hash(*config);
    } else {
      out_.write(config->toString());
      out_.printf("\r\n");
    }
  }

 private:
  bool metadata_only_ = false;
  bool hash_only_ = false;
  bool json_ = false;

  void metadata(const Configuration& config) {
    const ServerConfig::ConfigMetadata& main_config_metadata =
        config.serverConfig()->getMainConfigMetadata();
    const ServerConfig::ConfigMetadata& included_config_metadata =
        config.serverConfig()->getIncludedConfigMetadata();

    InfoConfigTable table(
        !json_, "URI", "Source", "Hash", "Last Modified", "Last Loaded");

    table.next()
        .set<0>(main_config_metadata.uri)
        .set<1>(config.serverConfig()->getServerOrigin().index())
        .set<2>(main_config_metadata.hash)
        .set<3>(main_config_metadata.modified_time)
        .set<4>(main_config_metadata.loaded_time);
    if (!included_config_metadata.uri.empty()) {
      table.next()
          .set<0>(included_config_metadata.uri)
          // included config doesn't get propagated through config
          // synchronization set source to my node id
          .set<1>(server_->getProcessor()->getMyNodeID().index())
          .set<2>(included_config_metadata.hash)
          .set<3>(included_config_metadata.modified_time)
          .set<4>(included_config_metadata.loaded_time);
    }
    json_ ? table.printJson(out_) : table.print(out_);
  }

  void hash(const Configuration& config) {
    const char* SOURCE_DELIMITER = ":";

    const ServerConfig::ConfigMetadata& main_config_metadata =
        config.serverConfig()->getMainConfigMetadata();
    const ServerConfig::ConfigMetadata& included_config_metadata =
        config.serverConfig()->getIncludedConfigMetadata();

    std::string combined_hash;
    std::string main_uri = main_config_metadata.uri;
    size_t pos = main_uri.find(SOURCE_DELIMITER);
    ld_check(pos != std::string::npos);
    std::string main_source = main_uri.substr(0, pos);
    combined_hash += main_source + ':';
    combined_hash += main_config_metadata.hash;
    if (!included_config_metadata.uri.empty()) {
      combined_hash += '+';
      std::string included_uri = included_config_metadata.uri;
      pos = included_uri.find(SOURCE_DELIMITER);
      ld_check(pos != std::string::npos);
      std::string included_source = included_uri.substr(0, pos);
      if (included_source != main_source) {
        combined_hash += included_source + ':';
      }
      combined_hash += included_config_metadata.hash;
    }

    out_.write(combined_hash);
    out_.printf("\r\n");
  }
};

}}} // namespace facebook::logdevice::commands
