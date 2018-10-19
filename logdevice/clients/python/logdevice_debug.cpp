/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/clients/python/util/util.h"
#include "logdevice/common/debug.h"

using namespace boost::python;
using namespace facebook::logdevice;
using namespace facebook::logdevice::dbg;

BOOST_PYTHON_MODULE(debug) {
  def("parse_log_level_option", &parseLoglevelOption, args("value"));
  scope().attr("NONE") = std::string("none");
  scope().attr("CRITICAL") = std::string("critical");
  scope().attr("ERROR") = std::string("error");
  scope().attr("WARNING") = std::string("warning");
  scope().attr("NOTIFY") = std::string("notify");
  scope().attr("INFO") = std::string("info");
  scope().attr("DEBUG") = std::string("debug");
  scope().attr("SPEW") = std::string("spew");
}
