/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/server/IOFaultInjection.h"

#include <thread>
#include <unordered_set>

#include <gtest/gtest.h>
namespace facebook { namespace logdevice {

using namespace testing;
using DataType = IOFaultInjection::DataType;
using FaultType = IOFaultInjection::FaultType;
using FaultTypeBitSet = IOFaultInjection::FaultTypeBitSet;
using IOType = IOFaultInjection::IOType;
using InjectMode = IOFaultInjection::InjectMode;
using Settings = IOFaultInjection::Settings;

std::ostream& operator<<(std::ostream& oss, FaultType val) {
  return oss << fault_type_names[val];
}

constexpr shard_size_t shard_size{5};
constexpr shard_index_t faulty_shard_idx{0};
constexpr shard_index_t working_shard_idx{1};
constexpr std::chrono::milliseconds kZeroMs{0};

struct TestData {
  // Fault injection settings; use shared_ptr to overcome the fact that Settings
  // doesn't have a copy constructor and the TestWithParam<> template requites
  // TestData to be copyable.
  std::shared_ptr<Settings> settings;
  // Check if fault would be injected against these inputs..
  struct {
    shard_index_t shard_idx;
    IOType io_type;
    FaultTypeBitSet accepted_fault_types;
    DataType data_type;
  } inject_context;
  // Expected FaultType
  FaultType expectedFaultType;
};

const TestData kFilterPasses{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::READ,
                               FaultType::IO_ERROR,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx, IOType::READ, FaultType::IO_ERROR, DataType::ALL},
    FaultType::IO_ERROR};

const TestData kDifferentShardIdx{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::READ,
                               FaultType::IO_ERROR,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {working_shard_idx, IOType::READ, FaultType::IO_ERROR, DataType::ALL},
    FaultType::NONE};

const TestData kDifferentIOType{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::READ,
                               FaultType::IO_ERROR,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx, IOType::WRITE, FaultType::IO_ERROR, DataType::ALL},
    FaultType::NONE};

const TestData kIOTypeALLPasses{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::ALL,
                               FaultType::IO_ERROR,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx, IOType::WRITE, FaultType::IO_ERROR, DataType::ALL},
    FaultType::IO_ERROR};

const TestData kIOTypeNone{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::NONE,
                               FaultType::IO_ERROR,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx, IOType::WRITE, FaultType::IO_ERROR, DataType::ALL},
    FaultType::NONE};

const TestData kFaultTypeOROperation{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::ALL,
                               FaultType::CORRUPTION,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx,
     IOType::READ,
     FaultType::IO_ERROR | FaultType::CORRUPTION,
     DataType::ALL},
    FaultType::CORRUPTION};

const TestData kFaultTypeDifferent{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::ALL,
                               FaultType::LATENCY,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx,
     IOType::READ,
     FaultType::IO_ERROR | FaultType::CORRUPTION,
     DataType::ALL},
    FaultType::NONE};

const TestData kFaultTypeNone{
    std::make_shared<Settings>(DataType::ALL,
                               IOType::ALL,
                               FaultType::NONE,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx, IOType::READ, FaultType::LATENCY, DataType::ALL},
    FaultType::NONE};

const TestData kDataTypeDifferent{
    std::make_shared<Settings>(DataType::DATA,
                               IOType::ALL,
                               FaultType::IO_ERROR,
                               InjectMode::PERSISTENT,
                               100,
                               kZeroMs),
    {faulty_shard_idx, IOType::READ, FaultType::LATENCY, DataType::METADATA},
    FaultType::NONE};

const TestData kInjectModeSingleShot{
    std::make_shared<Settings>(DataType::DATA,
                               IOType::ALL,
                               FaultType::IO_ERROR,
                               InjectMode::SINGLE_SHOT,
                               UINT32_MAX,
                               kZeroMs),
    {faulty_shard_idx, IOType::ALL, FaultType::IO_ERROR, DataType::DATA},
    FaultType::IO_ERROR};

struct IOFaultInjectionFixture : public TestWithParam<TestData> {};

INSTANTIATE_TEST_CASE_P(IOFaultInjectionTest,
                        IOFaultInjectionFixture,
                        Values(kFilterPasses,
                               kDifferentShardIdx,
                               kDifferentIOType,
                               kIOTypeALLPasses,
                               kIOTypeNone,
                               kFaultTypeOROperation,
                               kFaultTypeDifferent,
                               kFaultTypeNone,
                               kDataTypeDifferent,
                               kInjectModeSingleShot));

TEST_P(IOFaultInjectionFixture, Test) {
  auto& settings = GetParam().settings;
  auto& io_fault_injection = IOFaultInjection::instance();
  io_fault_injection.init(shard_size);

  io_fault_injection.setFaultInjection(faulty_shard_idx,
                                       settings->dataType(),
                                       settings->ioType(),
                                       settings->faultType(),
                                       settings->mode(),
                                       settings->chance(),
                                       settings->latency());
  auto& inject_context = GetParam().inject_context;
  auto expectedFaultType = FaultType::NONE;
  expectedFaultType = GetParam().expectedFaultType;

  EXPECT_EQ(
      expectedFaultType,
      io_fault_injection.getInjectedFault(inject_context.shard_idx,
                                          inject_context.io_type,
                                          inject_context.accepted_fault_types,
                                          inject_context.data_type));
}
}} // namespace facebook::logdevice
