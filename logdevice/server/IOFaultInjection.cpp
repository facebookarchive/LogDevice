/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/IOFaultInjection.h"

#include "folly/Singleton.h"

namespace facebook { namespace logdevice {

namespace {

struct PrivateTag {};
static folly::LeakySingleton<IOFaultInjection> ioFaultInjection;

} // namespace

IOFaultInjection& IOFaultInjection::instance() {
  return ioFaultInjection.get();
}

using FaultType = IOFaultInjection::FaultType;

void IOFaultInjection::init(shard_size_t nshards) {
  ld_check(nshards > 0);
  shard_settings_ = std::vector<folly::Synchronized<Settings>>(nshards);
}

bool IOFaultInjection::Settings::match(
    IOFaultInjection::DataType data_type,
    IOFaultInjection::IOType io_type,
    IOFaultInjection::FaultTypeBitSet fault_types) const {
  return (fault_types & faultType()) && mode() != InjectMode::OFF &&
      (dataType() == data_type || dataType() == DataType::ALL) &&
      (ioType() == io_type || ioType() == IOType::ALL);
}

FaultType IOFaultInjection::getInjectedFault(shard_index_t shard_idx,
                                             IOType io_type,
                                             FaultTypeBitSet fault_types,
                                             DataType d_type) {
  if (!enable_fault_injection_.load(std::memory_order_relaxed)) {
    return FaultType::NONE;
  }

  if (shard_settings_.empty()) {
    // Should only be true in unit tests.
    return FaultType::NONE;
  }

  ld_check(shard_idx < shard_settings_.size());
  auto settings = shard_settings_[shard_idx].rlock();
  if (settings->match(d_type, io_type, fault_types) &&
      (settings->chance() == UINT32_MAX ||
       folly::Random::rand32() < settings->chance())) {
    switch (settings->mode()) {
      case InjectMode::PERSISTENT: {
        if (settings->match(d_type, io_type, fault_types)) {
          return settings->faultType();
        }
        break;
      }
      case InjectMode::SINGLE_SHOT: {
        settings.unlock();
        auto uSettings = shard_settings_[shard_idx].ulock();
        if (uSettings->match(d_type, io_type, fault_types)) {
          auto error = uSettings->faultType();
          if (uSettings->mode() == InjectMode::SINGLE_SHOT) {
            auto wSettings = uSettings.moveFromUpgradeToWrite();
            *wSettings = Settings();
          }
          return error;
        }
        break;
      }
      case InjectMode::OFF:
        break;
      default:
        ld_check(false);
        break;
    }
  }
  return FaultType::NONE;
}

void IOFaultInjection::setFaultInjection(shard_index_t shard_idx,
                                         DataType data_type,
                                         IOType io_type,
                                         FaultType fault_type,
                                         InjectMode mode,
                                         double percent_chance,
                                         std::chrono::milliseconds latency) {
  uint32_t chance =
      std::min((double)UINT32_MAX, percent_chance / 100 * UINT32_MAX);

  if (io_type == IOType::NONE || fault_type == FaultType::NONE ||
      data_type == DataType::NONE) {
    mode = InjectMode::OFF;
  }

  ld_check(shard_idx < shard_settings_.size());
  auto settings = shard_settings_[shard_idx].wlock();
  enable_fault_injection_.store(true, std::memory_order_relaxed);
  if (mode == InjectMode::OFF) {
    // Disable everything
    *settings = Settings();
  } else {
    *settings = Settings(data_type, io_type, fault_type, mode, chance, latency);
  }
}

std::chrono::milliseconds
IOFaultInjection::getLatencyToInject(shard_index_t shard_idx) {
  if (shard_settings_.empty()) {
    // Should only be true in unit tests.
    return std::chrono::milliseconds(0);
  }
  ld_check(shard_idx < shard_settings_.size());
  return shard_settings_[shard_idx]->latency();
}

template <>
const std::string& EnumMap<FaultType, std::string>::invalidValue() {
  static const std::string invalidFaultType("invalid");
  return invalidFaultType;
}

template <>
void EnumMap<FaultType, std::string>::setValues() {
  static_assert(static_cast<int>(FaultType::MAX) == 5,
                "Please update faultTypeNames() after modifying FaultType");
  set(FaultType::NONE, "none");
  set(FaultType::IO_ERROR, "io_error");
  set(FaultType::CORRUPTION, "corruption");
  set(FaultType::LATENCY, "latency");
}

EnumMap<FaultType, std::string> fault_type_names;
}} // namespace facebook::logdevice
