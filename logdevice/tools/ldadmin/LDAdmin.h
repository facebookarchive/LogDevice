// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <iostream>

#include <boost/program_options.hpp>

#include "logdevice/tools/ldadmin/TransportOptionPlugins.h"

namespace facebook { namespace logdevice {

class LDAdmin {
 public:
  LDAdmin() {
    using boost::program_options::value;
    // clang-format off
    opt_dscr_.add_options()
      ("help,h", "Produce this help message and exit")
      ("command",
        value<std::vector<std::string>>(&commands_), "command")
      ("timeout",
        value<uint64_t>(&timeout_sec_)->default_value(timeout_sec_),
        "Connection and command timeout in seconds. A value of 0 means no timeout.")
      ("ssl",
        value<bool>(&ssl_)->default_value(ssl_),
        "Whether to use SSL for the connection or not.");
    positional_.add("command", /* max_count= */ -1);
  }

  bool registerTransportOptionPlugin(TransportOptionPlugin& plugin) {
    using boost::program_options::value;
    try {
      auto name = plugin.getOptionName().c_str();
      auto& val = plugin.getOptionValue();
      auto descr = plugin.getOptionDescription().c_str();
      transport_options_.emplace(std::make_pair(name, std::ref(plugin)));
      opt_dscr_.add_options()(name, value<std::string>(&val), descr);
    } catch (const boost::program_options::error& ex) {
      print_usage();
      std::cerr << "Error in registring TransportOptionPlugin: " << ex.what()
                << std::endl;
      return false;
    } catch (const std::exception& e) {
      std::cerr << "RegisterTransportOptionPlugin: " << plugin.getOptionName()
                << std::endl;
      std::cerr << e.what() << std::endl;
      return false;
    }
    return true;
  }

  bool run();

  bool parse_command_line(int argc, const char* argv[]);

 private:
  void print_usage();

  uint64_t timeout_sec_ {10};
  bool ssl_ {true};
  std::vector<std::string> commands_;

  std::map<std::string, std::reference_wrapper<TransportOptionPlugin>>
      transport_options_;
  boost::program_options::variables_map vm_;
  boost::program_options::options_description opt_dscr_{"Options"};
  boost::program_options::positional_options_description positional_;
};
}} // namespace facebook::logdevice
