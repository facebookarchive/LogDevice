/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/admin/toString.h"

#include <sstream>

#include <thrift/lib/cpp/util/EnumUtils.h>

namespace facebook { namespace logdevice {
std::string toString(const thrift::SocketAddressFamily& family) {
  return apache::thrift::util::enumNameSafe(family);
}

std::string toString(const thrift::SocketAddress& address) {
  std::ostringstream ss;
  ss << "SocketAddress(";
  ss << "family=" << toString(address.get_address_family());
  if (address.address_ref().has_value()) {
    ss << ",";
    ss << "address=\"" << address.address_ref().value() << "\"";
  }
  if (address.port_ref().has_value()) {
    ss << ",";
    ss << "port=" << address.port_ref().value();
  }
  ss << ")";
  return ss.str();
}

std::string toString(const thrift::NodeID& node_id) {
  bool add_comma = false;
  std::ostringstream ss;
  ss << "NodeID(";
  if (node_id.node_index_ref().has_value()) {
    ss << "node_index=" << node_id.node_index_ref().value();
    add_comma = true;
  }
  if (add_comma) {
    ss << ", ";
  }
  if (node_id.address_ref().has_value()) {
    ss << "address=" << toString(node_id.address_ref().value()) << ",";
    add_comma = true;
  }
  if (add_comma) {
    ss << ", ";
  }
  if (node_id.name_ref().has_value()) {
    ss << "name=\"" << node_id.name_ref().value() << "\"";
  }
  ss << ")";
  return ss.str();
}
}} // namespace facebook::logdevice
