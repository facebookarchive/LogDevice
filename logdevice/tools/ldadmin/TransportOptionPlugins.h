// Copyright 2004-present Facebook. All Rights Reserved.

#pragma once

#include <folly/Conv.h>
#include <folly/String.h>
#include <folly/io/async/AsyncSocket.h>

namespace facebook { namespace logdevice {

class TransportOptionPlugin {
 public:
  TransportOptionPlugin(std::string name, std::string descr, std::string dflt)
      : opt_name_(name), opt_descr_(descr), opt_value_(dflt) {}

  virtual ~TransportOptionPlugin() {}

  const std::string& getOptionName() const {
    return opt_name_;
  }

  const std::string& getOptionDescription() const {
    return opt_descr_;
  }

  std::string& getOptionValue() {
    return opt_value_;
  }

  virtual folly::SocketAddress getAddress() = 0;

 protected:
  std::string opt_name_;
  std::string opt_descr_;
  std::string opt_value_;
};

class HostOptionPlugin : public TransportOptionPlugin {
 public:
  using TransportOptionPlugin::TransportOptionPlugin;

  folly::SocketAddress getAddress() override {
    std::string addr = opt_value_;
    // @TODO: adapt for IPv6 addresses
    if (addr.find(':') == std::string::npos) {
      addr += ":6440";
    }
    folly::SocketAddress address;
    address.setFromHostPort(addr);
    return address;
  }
};

class UnixPathOptionPlugin : public TransportOptionPlugin {
 public:
  using TransportOptionPlugin::TransportOptionPlugin;
  folly::SocketAddress getAddress() override {
    folly::SocketAddress addr;
    addr.setFromPath(opt_value_);
    return addr;
  }
};

}} // namespace facebook::logdevice
