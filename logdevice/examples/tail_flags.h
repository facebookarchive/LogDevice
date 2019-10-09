#pragma once
#include <iostream>

#include <boost/program_options.hpp>

#include "logdevice/common/commandline_util_chrono.h"

const char* USAGE =
    R"DOC(Usage: tail [options...] CONFIG LOG

Tail a LogDevice log.  Records are printed to stdout, one per line.  By
default, prints records written in the last minute; the time period can be
adjusted with the -t option.

The CONFIG argument points the tool to the cluster.  The LOG argument says
which log to write to.  See below for formats.

)DOC";

namespace {
struct Config {
  std::string config_path;
  std::string target_log;
  // The reader would start reading logs from thse written 'time' milliseconds
  // ago.
  std::chrono::milliseconds time{std::chrono::minutes(1)};
  bool follow = false;
};
} // namespace

void parse_command_line(int argc,
                        const char** argv,
                        Config& command_line_options) {
  using boost::program_options::bool_switch;
  using boost::program_options::value;
  namespace style = boost::program_options::command_line_style;
  try {
    boost::program_options::options_description desc("Options");
    // clang-format off
    desc.add_options()

    ("help,h",
     "print help and exit")

    ("config,c",
     value<std::string>(&command_line_options.config_path)
       ->required(),
     "location of the cluster config to use; can be a file or more generally "
     "[scheme:]<path-to-config>")

    ("log,l",
     value<std::string>(&command_line_options.target_log)
       ->required(),
     "log to append records to; can be a numeric log ID, a log name from the "
     "config, or log group name with an offset (e.g. \"my_log_group[0]\")")

    ("time,t",
     // Borrowing LogDevice's internal `chrono_value' parser for convenience
     facebook::logdevice::chrono_value(&command_line_options.time),
     "how far back to start reading (e.g. \"0\" for now, \"1s\", \"5min\" etc)")

    ("follow,f",
     bool_switch(&command_line_options.follow),
     "continue reading after the current tail is reached")

      ;
    // clang-format on

    boost::program_options::positional_options_description positional;
    positional.add("config", /* max_count */ 1);
    positional.add("log", /* max_count */ 1);

    boost::program_options::command_line_parser parser(argc, argv);
    boost::program_options::variables_map parsed;
    boost::program_options::store(
        parser.options(desc)
            .positional(positional)
            .style(style::unix_style & ~style::allow_guessing)
            .run(),
        parsed);
    if (parsed.count("help")) {
      std::cout << USAGE << "\n" << desc;
      exit(0);
    }
    boost::program_options::notify(parsed);
  } catch (const boost::program_options::error& ex) {
    std::cerr << argv[0] << ": " << ex.what() << '\n';
    exit(1);
  }
}
