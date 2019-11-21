/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <chrono>

#include <gtest/gtest.h>

#include "logdevice/common/Semaphore.h"
#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/configuration/InternalLogs.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/include/Client.h"
#include "logdevice/lib/ClientImpl.h"
#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;

// First valid lsn
const lsn_t e1n1 = compose_lsn(EPOCH_MIN, ESN_MIN);

class ConfigPermissionTest : public IntegrationTestBase {};

// Read from zero and verify the results for trim test
static void trim_test_read_verify(ssize_t num_records_read,
                                  lsn_t trim_upto,
                                  std::shared_ptr<Client> client,
                                  lsn_t first_record_to_read,
                                  logid_t logid = logid_t(2)) {
  const logid_t LOG_ID(logid);

  std::unique_ptr<Reader> reader = client->createReader(1);
  ASSERT_EQ(0, reader->startReading(LOG_ID, 0));

  std::vector<std::unique_ptr<DataRecord>> read_data;
  GapRecord gap;
  ssize_t nread;

  // The first call should yield a bridge gap to the first epoch
  nread = reader->read(num_records_read, &read_data, &gap);
  ASSERT_EQ(-1, nread);
  EXPECT_EQ(0, gap.lo);
  EXPECT_EQ(e1n1 - 1, gap.hi);
  EXPECT_EQ(GapType::BRIDGE, gap.type);

  if (trim_upto == LSN_INVALID) {
    while ((nread = reader->read(num_records_read, &read_data, &gap)) == -1) {
      EXPECT_EQ(GapType::BRIDGE, gap.type);
    }
  } else {
    nread = reader->read(num_records_read, &read_data, &gap);
    ASSERT_EQ(-1, nread);
    EXPECT_EQ(e1n1, gap.lo);
    EXPECT_EQ(trim_upto, gap.hi);
    EXPECT_EQ(GapType::TRIM, gap.type);
    nread = reader->read(num_records_read, &read_data, &gap);
  }

  // The second should return data.
  EXPECT_EQ(num_records_read, nread);
  EXPECT_EQ(read_data[0]->attrs.lsn, first_record_to_read);
}

// Verifies that configPermissionChecker blocks unauthorized users from
// appending. Creates three clients with different credentials. One with no
// credentials, one with "allPass" credentials and one with "appendFail"
// credentials. The client with "appendFail" and no credentials should be unable
// to write to log 1. All three should be able to write to log 2
TEST_F(ConfigPermissionTest, Append) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_permission_test.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid gaps
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .useTcp() // required for ip server authentication
          .setParam("--handshake-timeout", chrono_string(this->testTimeout()))
          .setParam("--require-permission-message-types", "all")
          .create(*config);

  cluster->getSequencerNode().suspend();

  auto client_settings = [&]() {
    std::unique_ptr<ClientSettings> res(ClientSettings::create());
    ld_check_eq(0,
                res->set("handshake-timeout",
                         chrono_string(this->testTimeout()).c_str()));
    return res;
  };

  // create a client with different credentials
  auto guest_client =
      cluster->createClient(this->testTimeout(), client_settings());
  auto auth_client =
      cluster->createClient(this->testTimeout(), client_settings(), "allPass");
  auto noAppend_client = cluster->createClient(
      this->testTimeout(), client_settings(), "appendFail");
  auto admin_client = cluster->createClient(
      this->testTimeout(), client_settings(), "admin_user_id");

  char data[128]; // send the contents of this array as payload
  std::atomic<int> cb_called(0);

  auto check_status_cb = [&](Status st, const DataRecord& /*r*/) {
    cb_called++;
    EXPECT_EQ(E::OK, st);
  };
  auto check_status_cb_access = [&](Status st, const DataRecord& /*r*/) {
    cb_called++;
    EXPECT_EQ(E::ACCESS, st);
  };

  Payload payload1(data, 1);

  // Checks that an unauthorized client can not write to logs with
  // special permissions
  guest_client->append(logid_t(1), payload1, check_status_cb_access);
  // Check that an unauthroized Client can write to logs with guest permissions
  guest_client->append(logid_t(2), payload1, check_status_cb);

  // checks to see that the authorized client can write to both logs
  auth_client->append(logid_t(1), payload1, check_status_cb);
  auth_client->append(logid_t(2), payload1, check_status_cb);

  // admin client should be able to append to both logs
  admin_client->append(logid_t(1), payload1, check_status_cb);
  admin_client->append(logid_t(2), payload1, check_status_cb);

  // check to see that the "appendFail" principal stop users from writing into
  // log1, but still has write access to log2
  noAppend_client->append(logid_t(1), payload1, check_status_cb_access);
  noAppend_client->append(logid_t(2), payload1, check_status_cb);

  cluster->getSequencerNode().resume();

  wait_until(
      "callback is called enough times", [&] { return cb_called.load() >= 8; });
  guest_client.reset(); // this blocks until all Worker threads shut down
  auth_client.reset();
  noAppend_client.reset();
  admin_client.reset();
}

// Verifies that configPermissionChecker blocks unauthorized users from
// trimming. Creates three clients with different credentials, similar to
// ConfigPermissionAppendTest. The client with no credentials and "trimFail"
// credentials should not be able to trim from log 1, but all three
// should be able to trim from log 2
TEST_F(ConfigPermissionTest, Trim) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_permission_test.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid gaps
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .setParam("--require-permission-message-types", "all")
          .useTcp() // required for ip server authentication
          .create(*config);

  // create a client with different credentials
  auto guest_client = cluster->createClient(this->testTimeout());
  auto auth_client = cluster->createClient(
      this->testTimeout(), std::unique_ptr<ClientSettings>(), "allPass");
  auto noTrim_client = cluster->createClient(
      this->testTimeout(), std::unique_ptr<ClientSettings>(), "trimFail");
  auto admin_client = cluster->createClient(
      this->testTimeout(), std::unique_ptr<ClientSettings>(), "admin_user_id");

  const size_t num_records = 50;
  const size_t num_records_to_trim = 10;

  ASSERT_TRUE(num_records_to_trim < num_records);
  ASSERT_TRUE(num_records_to_trim > 0);

  // Write the given number of records
  std::vector<lsn_t> lsn_written_log1;
  std::vector<lsn_t> lsn_written_log2;

  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    // write data to log 1
    lsn_t lsn =
        auth_client->appendSync(logid_t(1), Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    lsn_written_log1.push_back(lsn);

    // write data to log 2
    lsn =
        auth_client->appendSync(logid_t(2), Payload(data.data(), data.size()));
    ASSERT_NE(LSN_INVALID, lsn);
    lsn_written_log2.push_back(lsn);
  }
  cluster->waitForRecovery();
  cluster->getSequencerNode().suspend();

  char data[128]; // send the contents of this array as payload
  std::atomic<int> cb_called(0);

  auto check_status_cb = [&](Status st) {
    cb_called++;
    EXPECT_EQ(E::OK, st);
  };
  auto check_status_cb_access = [&](Status st) {
    cb_called++;
    EXPECT_EQ(E::ACCESS, st);
  };

  lsn_t trim_upto = lsn_written_log1[num_records_to_trim - 1];
  lsn_t trim_upto_log2 = lsn_written_log2[num_records_to_trim - 1];

  // Verify that guest and noTrim clients cannot trim from log 1
  int rv = guest_client->trim(logid_t(1), trim_upto, check_status_cb_access);
  ASSERT_EQ(0, rv);
  rv = noTrim_client->trim(logid_t(1), trim_upto, check_status_cb_access);
  ASSERT_EQ(0, rv);

  // verify that guest can trim from log 2
  rv = guest_client->trim(logid_t(2), trim_upto_log2, check_status_cb);
  ASSERT_EQ(0, rv);
  cluster->getSequencerNode().resume();

  wait_until(
      "callback is called enough times", [&] { return cb_called.load() >= 3; });
  // verify that no records were trimed in log 1 by guest or noTrim clients
  trim_test_read_verify(
      num_records, LSN_INVALID, auth_client, lsn_written_log1[0], logid_t(1));
  // verify guest could trim from log 2
  trim_test_read_verify(num_records - num_records_to_trim,
                        trim_upto,
                        auth_client,
                        lsn_written_log2[num_records_to_trim],
                        logid_t(2));

  cluster->getSequencerNode().suspend();

  // verify that an authorized client can trim the log
  rv = admin_client->trim(logid_t(1), trim_upto, check_status_cb);
  ASSERT_EQ(0, rv);

  cluster->getSequencerNode().resume();

  wait_until(
      "callback is called enough times", [&] { return cb_called.load() >= 4; });
  // verify logs were actually trimeed
  trim_test_read_verify(num_records - num_records_to_trim,
                        trim_upto,
                        auth_client,
                        lsn_written_log1[num_records_to_trim],
                        logid_t(1));

  // trim another 10 records
  trim_upto = lsn_written_log1[num_records_to_trim + 10 - 1];
  rv = admin_client->trim(logid_t(1), trim_upto, check_status_cb);

  wait_until(
      "callback is called enough times", [&] { return cb_called.load() >= 5; });

  // verify that we have actually trimmed.
  trim_test_read_verify(num_records - num_records_to_trim - 10,
                        trim_upto,
                        admin_client,
                        lsn_written_log1[num_records_to_trim + 10],
                        logid_t(1));

  guest_client.reset(); // this blocks until all Worker threads shut down
  noTrim_client.reset();
  auth_client.reset();
  admin_client.reset();
}

// Verifies that configPermissionChecker blocks unauthorized users from reading.
// Creates two clients with different credentials. One with no credentials,
// one with "allPass" credentials. The client with no credentials should be
// unable to read form log 1 but still able to read form log 2
TEST_F(ConfigPermissionTest, AsyncRead) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_permission_test.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid gaps
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .setParam("--require-permission-message-types", "all")
          .useTcp() // required for ip server authentication
          .create(*config);
  auto client = cluster->createClient(this->testTimeout());
  auto auth_client = cluster->createClient(
      this->testTimeout(), std::unique_ptr<ClientSettings>(), "allPass");
  auto admin_client = cluster->createClient(
      this->testTimeout(), std::unique_ptr<ClientSettings>(), "admin_user_id");

  logid_t logid(2);
  const size_t num_records = 10;
  // a map <lsn, data> for appended records
  std::map<lsn_t, std::string> lsn_map_log1;
  std::map<lsn_t, std::string> lsn_map_log2;
  std::map<lsn_t, std::string>* lsn_map = &lsn_map_log2;

  lsn_t first_lsn_log1 = LSN_INVALID;
  lsn_t first_lsn_log2 = LSN_INVALID;
  lsn_t first_lsn;
  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn =
        auth_client->appendSync(logid, Payload(data.data(), data.size()));
    EXPECT_NE(LSN_INVALID, lsn);
    if (first_lsn_log2 == LSN_INVALID) {
      first_lsn_log2 = lsn;
    }
    EXPECT_EQ(lsn_map_log2.end(), lsn_map_log2.find(lsn));
    lsn_map_log2[lsn] = data;

    lsn =
        auth_client->appendSync(logid_t(1), Payload(data.data(), data.size()));
    EXPECT_NE(LSN_INVALID, lsn);
    if (first_lsn_log1 == LSN_INVALID) {
      first_lsn_log1 = lsn;
    }
    EXPECT_EQ(lsn_map_log1.end(), lsn_map_log1.find(lsn));
    lsn_map_log1[lsn] = data;
  }

  cluster->waitForRecovery();

  auto it = lsn_map->cbegin();
  bool has_payload = true;
  Semaphore sem;
  bool expectingGap = false;

  auto gap_cb = [&](const GapRecord& g) {
    if (!expectingGap) {
      ADD_FAILURE() << "didnt expect gaps";
    }
    EXPECT_EQ(logid, g.logid);
    EXPECT_EQ(GapType::ACCESS, g.type);
    EXPECT_EQ(first_lsn, g.lo);
    EXPECT_EQ(LSN_MAX, g.hi);

    sem.post();
    return true;
  };
  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    EXPECT_EQ(logid, r->logid);
    EXPECT_NE(lsn_map->cend(), it);
    EXPECT_EQ(it->first, r->attrs.lsn);
    const Payload& p = r->payload;
    if (has_payload) {
      EXPECT_NE(nullptr, p.data());
      EXPECT_EQ(it->second.size(), p.size());
      EXPECT_EQ(it->second, p.toString());
    } else {
      EXPECT_EQ(nullptr, p.data());
      EXPECT_EQ(0, p.size());
    }
    if (++it == lsn_map->cend()) {
      sem.post();
    }
    return true;
  };

  // verify that the unauthorized client can still read from log2
  first_lsn = first_lsn_log2;
  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(logid, first_lsn);
  sem.wait();

  Semaphore stop_sem;
  int rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();

  lsn_map = &lsn_map_log1;
  it = lsn_map->cbegin();
  logid = logid_t(1);
  first_lsn = first_lsn_log1;

  // verify that an authorized client can read from log 1
  std::unique_ptr<AsyncReader> auth_reader(auth_client->createAsyncReader());
  auth_reader->setGapCallback(gap_cb);
  auth_reader->setRecordCallback(record_cb);
  auth_reader->startReading(logid, first_lsn);
  sem.wait();

  rv = auth_reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();

  // reset settings
  lsn_map = &lsn_map_log1;
  it = lsn_map->cbegin();
  logid = logid_t(1);
  first_lsn = first_lsn_log1;

  // verify that admin can read form log 1
  std::unique_ptr<AsyncReader> admin_reader(admin_client->createAsyncReader());
  admin_reader->setGapCallback(gap_cb);
  admin_reader->setRecordCallback(record_cb);
  admin_reader->startReading(logid, first_lsn);
  sem.wait();

  rv = admin_reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();

  // verify that the unauthorized client can not read from log 1
  it = lsn_map->cbegin();
  expectingGap = true;
  logid = logid_t(1);
  reader->startReading(logid, first_lsn);
  sem.wait();

  rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();
}

// This test that when clients do not provide credentials when authentication is
// required, then the connection is closed and the client will receive the
// expected errors.
TEST_F(ConfigPermissionTest, RequireAuthentication) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_permission_test_req_auth.conf"));
  ASSERT_NE(nullptr, config);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData()
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .setParam("--require-permission-message-types", "all")
          .useTcp() // required for ip server authentication
          .create(*config);

  std::unique_ptr<ClientSettings> settings(ClientSettings::create());

  // Temporarily disabling check for trim requests that trim past the tail
  // because the underlying implementation is not properly propagating the
  // error up the stack when access is denied. This can be re-enabled when
  // the fix arrives.
  ASSERT_EQ(0, settings->set("disable-trim-past-tail-check", true));

  // client was created with out any credential, it should not be allowed to
  // connect
  auto client = cluster->createClient(this->testTimeout(), std::move(settings));

  std::atomic<int> cb_called(0);
  auto check_append_status_cb = [&](Status st, const DataRecord& /*r*/) {
    cb_called++;
    EXPECT_EQ(E::ACCESS, st);
  };
  auto check_trim_status_cb = [&](Status st) {
    cb_called++;
    EXPECT_EQ(E::ACCESS, st);
  };

  char data[128];
  Payload payload1(data, 1);

  // both of these should fail as client did not provide any credentials
  client->append(logid_t(2), payload1, check_append_status_cb);
  client->trim(logid_t(2), lsn_t(1), check_trim_status_cb);

  wait_until(
      "callback is called enough times", [&] { return cb_called.load() >= 2; });

  auto gap_cb = [&](const GapRecord& g) {
    EXPECT_EQ(logid_t(2), g.logid);

    if (g.type == GapType::BRIDGE) {
      EXPECT_EQ(lsn_t(1), g.lo);
      EXPECT_EQ(e1n1 - 1, g.hi);
    } else {
      EXPECT_EQ(GapType::ACCESS, g.type);
      EXPECT_EQ(e1n1, g.lo); // epoch 1, esn 1
      EXPECT_EQ(LSN_MAX, g.hi);
    }

    cb_called++;
    return true;
  };
  auto record_cb = [&](std::unique_ptr<DataRecord>& /*r*/) {
    ADD_FAILURE() << "Not Expecting Any Records";
    return true;
  };

  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(logid_t(2), lsn_t(1));

  wait_until(
      "callback is called enough times", [&] { return cb_called.load() >= 4; });
}

// This test that when enable_permission_checking is false permission checking
// is truly disabled, even when permission_checker_type is still set to "config"
TEST_F(ConfigPermissionTest, PermissionCheckingDisabled) {
  auto config = Configuration::fromJsonFile(TEST_CONFIG_FILE(
      "conf_permission_test_permission_checking_disabled.conf"));
  ASSERT_NE(nullptr, config);

  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid gaps
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .useTcp() // required for ip server authentication
          .create(*config);

  // client was created with out any credential, it should be allowed to
  // connect and perform all operations
  auto client = cluster->createClient(this->testTimeout());

  logid_t logid(1);
  const size_t num_records = 20;
  const size_t num_records_to_trim = 10;
  // a map <lsn, data> for appended records
  std::map<lsn_t, std::string> lsn_map;
  lsn_t trim_upto = LSN_INVALID;
  lsn_t first_lsn = LSN_INVALID;

  for (int i = 0; i < num_records; ++i) {
    std::string data("data" + std::to_string(i));
    lsn_t lsn = client->appendSync(logid, Payload(data.data(), data.size()));
    EXPECT_NE(LSN_INVALID, lsn);
    if (first_lsn == LSN_INVALID) {
      first_lsn = lsn;
    }
    EXPECT_EQ(lsn_map.end(), lsn_map.find(lsn));
    lsn_map[lsn] = data;

    if (i == num_records_to_trim - 1) {
      trim_upto = lsn;
    }
  }

  cluster->waitForRecovery();

  auto it = lsn_map.cbegin();
  Semaphore sem;

  auto gap_cb = [](const GapRecord& /*g*/) {
    ADD_FAILURE() << "Not expecting gaps";
    return true;
  };
  auto record_cb = [&](std::unique_ptr<DataRecord>& r) {
    EXPECT_EQ(logid, r->logid);
    EXPECT_NE(lsn_map.cend(), it);
    EXPECT_EQ(it->first, r->attrs.lsn);
    const Payload& p = r->payload;
    EXPECT_NE(nullptr, p.data());
    EXPECT_EQ(it->second.size(), p.size());
    EXPECT_EQ(it->second, p.toString());
    if (++it == lsn_map.cend()) {
      sem.post();
    }
    return true;
  };

  auto check_status_cb = [&](Status st) {
    sem.post();
    EXPECT_EQ(E::OK, st);
  };

  // verify that the unauthorized client can read from log1
  std::unique_ptr<AsyncReader> reader(client->createAsyncReader());
  reader->setGapCallback(gap_cb);
  reader->setRecordCallback(record_cb);
  reader->startReading(logid, first_lsn);
  sem.wait();

  Semaphore stop_sem;
  int rv = reader->stopReading(logid, [&]() { stop_sem.post(); });
  EXPECT_EQ(0, rv);
  stop_sem.wait();

  rv = client->trim(logid_t(1), trim_upto, check_status_cb);
  ASSERT_EQ(0, rv);

  // wait for the call back
  sem.wait();

  trim_test_read_verify(num_records - num_records_to_trim,
                        trim_upto,
                        client,
                        trim_upto + 1,
                        logid_t(1));
}

TEST_F(ConfigPermissionTest, ACL) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_permission_acl_test.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData() // to avoid gaps
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .setParam("--require-permission-message-types", "all")
          .useTcp() // required for ip server authentication
          .create(*config);
}

// Verifies that configPermissionChecker works with internal logs.
TEST_F(ConfigPermissionTest, InternalLogs) {
  auto config = Configuration::fromJsonFile(
      TEST_CONFIG_FILE("conf_internal_logs_permission_test.conf"));
  ASSERT_NE(nullptr, config);
  auto cluster =
      IntegrationTestUtils::ClusterFactory()
          .doPreProvisionEpochMetaData()        // to avoid gaps
          .setParam("--admin-enabled", "false") // disabling admin API with TCP
                                                // because of port allocation
                                                // failures in tests
          .useTcp() // required for ip server authentication
          .setParam("--handshake-timeout", chrono_string(this->testTimeout()))
          .setParam("--require-permission-message-types", "all")
          .create(*config);

  auto client_settings = [&]() {
    std::unique_ptr<ClientSettings> res(ClientSettings::create());
    ld_check_eq(0,
                res->set("handshake-timeout",
                         chrono_string(this->testTimeout()).c_str()));
    return res;
  };

  // create a client with different credentials
  auto guest_client =
      cluster->createClient(this->testTimeout(), client_settings());
  dynamic_cast<ClientImpl*>(guest_client.get())->allowWriteInternalLog();
  auto auth_client =
      cluster->createClient(this->testTimeout(), client_settings(), "allPass");
  dynamic_cast<ClientImpl*>(auth_client.get())->allowWriteInternalLog();
  auto noAppend_client = cluster->createClient(
      this->testTimeout(), client_settings(), "appendFail");
  dynamic_cast<ClientImpl*>(noAppend_client.get())->allowWriteInternalLog();
  auto admin_client = cluster->createClient(
      this->testTimeout(), client_settings(), "admin_user_id");
  dynamic_cast<ClientImpl*>(admin_client.get())->allowWriteInternalLog();

  char data[128]; // send the contents of this array as payload
  // The payload is not a valid record for event log deltas or snapshots, but we
  // only care about whether the write is allowed or not.
  Payload payload1(data, 1);

  using facebook::logdevice::configuration::InternalLogs;
  // Checks that an unauthorized client can not write to logs with
  // special permissions
  auto lsn = guest_client->appendSync(InternalLogs::EVENT_LOG_DELTAS, payload1);
  EXPECT_EQ(lsn, LSN_INVALID);
  EXPECT_EQ(err, E::ACCESS);
  // Check that an unauthroized Client can write to logs with guest permissions
  lsn = guest_client->appendSync(InternalLogs::EVENT_LOG_SNAPSHOTS, payload1);
  EXPECT_NE(lsn, LSN_INVALID);

  // checks to see that the authorized client can write to both logs
  lsn = auth_client->appendSync(InternalLogs::EVENT_LOG_DELTAS, payload1);
  EXPECT_NE(lsn, LSN_INVALID);
  lsn = auth_client->appendSync(InternalLogs::EVENT_LOG_SNAPSHOTS, payload1);
  EXPECT_NE(lsn, LSN_INVALID);

  // admin client should be able to append to both logs
  lsn = admin_client->appendSync(InternalLogs::EVENT_LOG_DELTAS, payload1);
  EXPECT_NE(lsn, LSN_INVALID);
  lsn = admin_client->appendSync(InternalLogs::EVENT_LOG_SNAPSHOTS, payload1);
  EXPECT_NE(lsn, LSN_INVALID);

  // check to see that the "appendFail" principal stop users from writing into
  // log1, but still has write access to log2
  lsn = noAppend_client->appendSync(InternalLogs::EVENT_LOG_DELTAS, payload1);
  EXPECT_EQ(lsn, LSN_INVALID);
  EXPECT_EQ(err, E::ACCESS);
  lsn =
      noAppend_client->appendSync(InternalLogs::EVENT_LOG_SNAPSHOTS, payload1);
  EXPECT_NE(lsn, LSN_INVALID);

  guest_client.reset(); // this blocks until all Worker threads shut down
  auth_client.reset();
  noAppend_client.reset();
  admin_client.reset();
}
