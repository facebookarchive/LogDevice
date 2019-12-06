/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <chrono>
#include <sstream>

#include <boost/python.hpp>

#include "logdevice/lib/ClientSettingsImpl.h"
#include "logdevice/test/ldbench/worker/Options.h"

using namespace boost::python;
using namespace facebook::logdevice;
using namespace facebook::logdevice::ldbench;

static object validateWorkerOptions(std::string bench_name, list& args) {
  std::vector<const char*> arg_vec(len(args) + 2);
  arg_vec[0] = "";
  arg_vec[1] = bench_name.c_str();
  for (int i = 0; i < len(args); ++i) {
    arg_vec[i + 2] = extract<const char*>(args[i]);
  }
  ClientSettingsImpl client_settings;
  Options options;
  std::stringstream ss;
  folly::Optional<int> rv = parse_commandline_options(
      client_settings, options, (int)arg_vec.size(), arg_vec.data(), ss);
  if (rv.hasValue()) {
    if (rv.value() == 0) {
      ss.str("");
      ss << "Unexpected --help.";
    }
    return str(ss.str());
  }
  return object();
}

static std::string getHelp(bool verbose = false) {
  const char* argv[] = {"", "--help", "--verbose"};
  ClientSettingsImpl client_settings;
  Options options;
  std::stringstream ss;
  folly::Optional<int> rv = parse_commandline_options(
      client_settings, options, verbose ? 3 : 2, argv, ss);
  ld_check(rv.hasValue());
  ld_check(rv.value() == 0);
  return ss.str();
}

BOOST_PYTHON_FUNCTION_OVERLOADS(getHelp_overloads, getHelp, 0, 1)

BOOST_PYTHON_MODULE(worker_options) {
  def("validate_worker_options",
      &validateWorkerOptions,
      args("bench_name, args"),
      "Check if the ldbench worker command line options are good (existing, "
      "compatible with each other, compatible with worker type, all required "
      "options are passed). Argument: a list of strings, e.g. ['--bench=read', "
      "'--fanout', '3']. If the options are valid, returns None. Otherwise "
      "returns a string containing the error message.");
  def("help",
      &getHelp,
      getHelp_overloads(args("verbose"),
                        "Get the descriptions of the command line options. If "
                        "verbose is True, "
                        "also include logdevice Client options."));
}
