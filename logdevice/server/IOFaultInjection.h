/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <bitset>

#include <folly/Random.h>
#include <folly/SharedMutex.h>

#include "logdevice/include/Err.h"
#include "logdevice/common/types_internal.h"

namespace facebook { namespace logdevice {

class IOFaultInjection {
 public:
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
      return load_relaxed(data_type_);
    }
    IOType ioType() const {
      return load_relaxed(io_type_);
    }
    FaultType faultType() const {
      return load_relaxed(fault_type_);
    }
    InjectMode mode() const {
      return load_relaxed(mode_);
    }
    uint32_t chance() const {
      return load_relaxed(chance_);
    }
    std::chrono::milliseconds latency() const {
      return load_relaxed(latency_);
    }

    bool match(DataType data_type,
               IOType io_type,
               FaultTypeBitSet fault_types) const;

    const Settings& copyFrom(const Settings& rhs) {
      if (this != &rhs) {
        copy_relaxed(data_type_, rhs.data_type_);
        copy_relaxed(io_type_, rhs.io_type_);
        copy_relaxed(fault_type_, rhs.fault_type_);
        copy_relaxed(mode_, rhs.mode_);
        copy_relaxed(chance_, rhs.chance_);
        copy_relaxed(latency_, rhs.latency_);
      }
      return *this;
    }

    folly::SharedMutex mutex;

   private:
    std::atomic<DataType> data_type_{DataType::NONE};
    std::atomic<IOType> io_type_{IOType::NONE};
    std::atomic<FaultType> fault_type_{FaultType::NONE};
    std::atomic<InjectMode> mode_{InjectMode::OFF};
    std::atomic<uint32_t> chance_{UINT32_MAX};
    std::atomic<std::chrono::milliseconds> latency_{
        std::chrono::milliseconds(0)};
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
  template <typename T>
  static void copy_relaxed(T& dst, const T& src) {
    dst.store(src.load(std::memory_order_relaxed), std::memory_order_relaxed);
  }

  template <typename T>
  static T load_relaxed(const std::atomic<T>& src) {
    return src.load(std::memory_order_relaxed);
  }

  std::vector<Settings> shard_settings_;
};

extern EnumMap<IOFaultInjection::FaultType, std::string> fault_type_names;

extern IOFaultInjection io_fault_injection;
}} // namespace facebook::logdevice
