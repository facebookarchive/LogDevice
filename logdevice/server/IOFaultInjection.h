/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>

#include <folly/Random.h>
#include <folly/Synchronized.h>

#include "logdevice/common/types_internal.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice {

class IOFaultInjection {
 public:
  static IOFaultInjection& instance();
  enum class DataType {
    NONE,
    // User and metadata log data.
    DATA,
    // Metadata records. See common/Metadata.h
    METADATA,
    // Match against any data type.
    ALL
  };

  using FaultTypeBitSet = uint8_t;
  enum FaultType : uint8_t {
    NONE = 0x00,
    IO_ERROR = 0x01,
    CORRUPTION = 0x02,
    LATENCY = 0x04,
    MAX,
    INVALID
  };

  enum class IOType { NONE, READ, WRITE, ALL };
  enum class InjectMode { OFF, SINGLE_SHOT, PERSISTENT };

  void init(shard_size_t nshards);

  class Settings {
   public:
    Settings() {}
    Settings(DataType d,
             IOType i,
             FaultType f,
             InjectMode m,
             uint32_t c,
             std::chrono::milliseconds l)
        : data_type_(d),
          io_type_(i),
          fault_type_(f),
          mode_(m),
          chance_(c),
          latency_(l) {}

    DataType dataType() const {
      return data_type_;
    }
    IOType ioType() const {
      return io_type_;
    }
    FaultType faultType() const {
      return fault_type_;
    }
    InjectMode mode() const {
      return mode_;
    }
    uint32_t chance() const {
      return chance_;
    }
    std::chrono::milliseconds latency() const {
      return latency_;
    }

    bool match(DataType data_type,
               IOType io_type,
               FaultTypeBitSet fault_types) const;

   private:
    DataType data_type_{DataType::NONE};
    IOType io_type_{IOType::NONE};
    FaultType fault_type_{FaultType::NONE};
    InjectMode mode_{InjectMode::OFF};
    uint32_t chance_{UINT32_MAX};
    std::chrono::milliseconds latency_{std::chrono::milliseconds(0)};
  };

  // Configure fault injection to occur on one or all shards.
  void setFaultInjection(shard_index_t shard_idx,
                         DataType d_type,
                         IOType io_type,
                         FaultType fault_type,
                         InjectMode mode,
                         double percent_chance,
                         std::chrono::milliseconds latency);

  // @param  fault_types    a bitset containing all FaultTypes injectable at
  //                        the callsite
  // @param d_type     is only relevant in the case of IOType::READ  with
  //                   FaultType::IO_ERROR | FaultType::CORRUPTION. In these
  //                   cases DataType may be used to separately target faults
  //                   in reading metadata or data. For all other fault
  //                   scenarios (the default) DataType::ALL should be used.
  //
  // @return FaultType::NONE if no fault injection should occur on the
  //         specified shard. Otherwise a fault type that should be returned
  //         for the current operation.
  FaultType getInjectedFault(shard_index_t shard_idx,
                             IOType io_type,
                             FaultTypeBitSet fault_types,
                             DataType d_type = DataType::ALL);

  std::chrono::milliseconds getLatencyToInject(shard_index_t shard_idx);

 private:
  std::vector<folly::Synchronized<Settings>> shard_settings_;
  std::atomic<bool> enable_fault_injection_{false};
};

extern EnumMap<IOFaultInjection::FaultType, std::string> fault_type_names;
}} // namespace facebook::logdevice
