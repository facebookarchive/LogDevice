/**
 * Copyright (c) 2019-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <gmock/gmock.h>

#include "logdevice/include/Client.h"

namespace facebook { namespace logdevice {
class MockClient : public Client {
 public:
  MOCK_METHOD4(
      appendSync,
      lsn_t(logid_t, std::string, AppendAttributes, std::chrono::milliseconds));
  MOCK_METHOD4(
      appendSync,
      lsn_t(logid_t, Payload, AppendAttributes, std::chrono::milliseconds));
  MOCK_METHOD4(append,
               int(logid_t, std::string, append_callback_t, AppendAttributes));
  MOCK_METHOD4(append,
               int(logid_t, Payload, append_callback_t, AppendAttributes));
  MOCK_METHOD2(createReader, std::unique_ptr<Reader>(size_t, ssize_t));
  MOCK_METHOD1(createAsyncReader, std::unique_ptr<AsyncReader>(ssize_t));
  MOCK_METHOD1(setTimeout, void(std::chrono::milliseconds timeout));
  MOCK_METHOD2(trimSync, int(logid_t logid, lsn_t lsn));
  MOCK_METHOD3(trim, int(logid_t logid, lsn_t lsn, trim_callback_t cb));
  MOCK_METHOD1(addWriteToken, void(std::string));
  MOCK_METHOD4(
      findTimeSync,
      lsn_t(logid_t, std::chrono::milliseconds, Status, FindKeyAccuracy));
  MOCK_METHOD3(findKeySync,
               FindKeyResult(logid_t, std::string, FindKeyAccuracy));
  MOCK_METHOD4(findTime,
               int(logid_t,
                   std::chrono::milliseconds,
                   find_time_callback_t,
                   FindKeyAccuracy));
  MOCK_METHOD4(findKey,
               int(logid_t, std::string, find_key_callback_t, FindKeyAccuracy));
  MOCK_METHOD2(isLogEmptySync, int(logid_t logid, bool* empty));
  MOCK_METHOD2(isLogEmpty, int(logid_t logid, is_empty_callback_t cb));
  MOCK_METHOD2(isLogEmptyV2Sync, int(logid_t logid, bool* empty));
  MOCK_METHOD2(isLogEmptyV2, int(logid_t logid, is_empty_callback_t cb));
  MOCK_METHOD5(dataSizeSync,
               int(logid_t logid,
                   std::chrono::milliseconds start,
                   std::chrono::milliseconds end,
                   DataSizeAccuracy accuracy,
                   size_t* size));
  MOCK_METHOD5(dataSize,
               int(logid_t logid,
                   std::chrono::milliseconds start,
                   std::chrono::milliseconds end,
                   DataSizeAccuracy accuracy,
                   data_size_callback_t cb));
  MOCK_METHOD1(getTailLSNSync, lsn_t(logid_t logid));
  MOCK_METHOD2(getTailLSN, int(logid_t logid, get_tail_lsn_callback_t cb));
  MOCK_METHOD1(getTailAttributesSync,
               std::unique_ptr<LogTailAttributes>(logid_t logid));
  MOCK_METHOD2(getTailAttributes,
               int(logid_t logid, get_tail_attributes_callback_t cb));
  MOCK_METHOD1(getHeadAttributesSync,
               std::unique_ptr<LogHeadAttributes>(logid_t logid));
  MOCK_METHOD2(getHeadAttributes,
               int(logid_t logid, get_head_attributes_callback_t cb));
  MOCK_METHOD1(getLogRangeByName, logid_range_t(const std::string& name));
  MOCK_METHOD2(getLogRangeByName,
               void(const std::string& name,
                    get_log_range_by_name_callback_t cb));
  MOCK_METHOD0(getLogNamespaceDelimiter, std::string());
  // The following line won't really compile, as the return
  // type has multiple template arguments.  To fix it, use a
  // typedef for the return type.
  MOCK_METHOD1(getLogRangesByNamespace,
               std::map<std::string, logid_range_t>(const std::string& ns));
  MOCK_METHOD2(getLogRangesByNamespace,
               void(const std::string& ns,
                    get_log_ranges_by_namespace_callback_t cb));
  MOCK_METHOD1(getLogGroupSync,
               std::unique_ptr<client::LogGroup>(const std::string& path));
  MOCK_METHOD2(getLogGroup,
               void(const std::string& path, get_log_group_callback_t cb));
  MOCK_METHOD1(getLogGroupByIdSync,
               std::unique_ptr<client::LogGroup>(const logid_t logid));
  MOCK_METHOD2(getLogGroupById,
               void(const logid_t logid, get_log_group_callback_t cb));
  MOCK_METHOD4(makeDirectory,
               int(const std::string& path,
                   bool mk_intermediate_dirs,
                   const client::LogAttributes& attrs,
                   make_directory_callback_t cb));
  MOCK_METHOD4(makeDirectorySync,
               std::unique_ptr<client::Directory>(std::string,
                                                  bool,
                                                  client::LogAttributes,
                                                  std::string));
  MOCK_METHOD3(removeDirectory,
               int(const std::string& path,
                   bool recursive,
                   logsconfig_status_callback_t));
  MOCK_METHOD3(removeDirectorySync, bool(std::string, bool, uint64_t));
  MOCK_METHOD2(removeLogGroupSync, bool(std::string, uint64_t));
  MOCK_METHOD2(removeLogGroup,
               int(const std::string& path, logsconfig_status_callback_t cb));
  MOCK_METHOD3(rename,
               int(const std::string& from_path,
                   const std::string& to_path,
                   logsconfig_status_callback_t cb));
  MOCK_METHOD4(renameSync,
               bool(std::string, std::string, uint64_t, std::string));
  MOCK_METHOD5(makeLogGroup,
               int(const std::string& path,
                   const logid_range_t& range,
                   const client::LogAttributes& attrs,
                   bool mk_intermediate_dirs,
                   make_log_group_callback_t cb));
  MOCK_METHOD5(makeLogGroupSync,
               std::unique_ptr<client::LogGroup>(std::string,
                                                 logid_range_t,
                                                 client::LogAttributes,
                                                 bool,
                                                 std::string));
  MOCK_METHOD3(setAttributes,
               int(const std::string& path,
                   const client::LogAttributes& attrs,
                   logsconfig_status_callback_t cb));
  MOCK_METHOD4(setAttributesSync,
               bool(std::string, client::LogAttributes, uint64_t, std::string));
  MOCK_METHOD3(setLogGroupRange,
               int(const std::string& path,
                   const logid_range_t& range,
                   logsconfig_status_callback_t));
  MOCK_METHOD4(setLogGroupRangeSync,
               bool(std::string, logid_range_t, uint64_t, std::string));
  MOCK_METHOD2(getDirectory,
               int(const std::string& path, get_directory_callback_t));
  MOCK_METHOD1(getDirectorySync,
               std::unique_ptr<client::Directory>(const std::string& path));
  MOCK_METHOD1(syncLogsConfigVersion, bool(uint64_t version));
  MOCK_METHOD2(notifyOnLogsConfigVersion,
               ConfigSubscriptionHandle(uint64_t version,
                                        std::function<void()>));
  MOCK_METHOD0(getClusterAttributes, std::unique_ptr<ClusterAttributes>());
  MOCK_METHOD1(subscribeToConfigUpdates,
               ConfigSubscriptionHandle(config_update_callback_t));
  MOCK_METHOD0(getMaxPayloadSize, size_t());
  MOCK_METHOD0(settings, ClientSettings&());
  MOCK_METHOD0(getAllReadStreamsDebugInfo, std::string());
  MOCK_METHOD5(
      publishEvent,
      void(Severity, std::string, std::string, std::string, std::string));
};
}} // namespace facebook::logdevice
