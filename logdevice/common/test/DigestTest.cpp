/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/Digest.h"

#include <memory>

#include <folly/Memory.h>
#include <gtest/gtest.h>

#include "logdevice/common/protocol/RECORD_Message.h"
#include "logdevice/common/test/DigestTestUtil.h"
#include "logdevice/common/test/NodeSetTestUtil.h"
#include "logdevice/include/Record.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::NodeSetTestUtil;

namespace {

using RecordType = DigestTestUtil::RecordType;

// Convenient shortcuts for writting ShardIDs.
#define N0 ShardID(0, 0)
#define N1 ShardID(1, 0)
#define N2 ShardID(2, 0)
#define N3 ShardID(3, 0)
#define N4 ShardID(4, 0)
#define N5 ShardID(5, 0)
#define N6 ShardID(6, 0)
#define N7 ShardID(7, 0)
#define N8 ShardID(8, 0)
#define N9 ShardID(9, 0)
#define N10 ShardID(10, 0)

constexpr epoch_t TEST_EPOCH(42);
constexpr logid_t TEST_LOG(2);

std::unique_ptr<DataRecordOwnsPayload> create_record(
    esn_t esn,
    RecordType type,
    uint32_t wave_or_seal_epoch,
    std::chrono::milliseconds timestamp = std::chrono::milliseconds(0),
    size_t payload_size = 128,
    OffsetMap offsets_within_epoch = OffsetMap(),
    OffsetMap offsets = OffsetMap()) {
  return DigestTestUtil::create_record(TEST_LOG,
                                       DigestTestUtil::lsn(TEST_EPOCH, esn),
                                       type,
                                       wave_or_seal_epoch,
                                       timestamp,
                                       payload_size,
                                       std::move(offsets_within_epoch),
                                       std::move(offsets));
}

class DigestTest : public ::testing::Test {
 public:
  void SetUp() override {
    dbg::assertOnData = true;

    // initialize the cluster config
    Configuration::Nodes nodes;
    addNodes(&nodes, 1, 1, "rg1.dc0.cl0.ro0.rk0", 1); // 0
    addNodes(&nodes, 4, 1, "rg1.dc0.cl0.ro0.rk1", 2); // 1-4
    addNodes(&nodes, 2, 1, "rg1.dc0.cl0..", 1);       // 5-6
    addNodes(&nodes, 2, 1, "rg2.dc0.cl0.ro0.rk1", 1); // 7-8
    addNodes(&nodes, 2, 1, "....", 1);                // 9-10

    Configuration::NodesConfig nodes_config(std::move(nodes));

    config_ =
        ServerConfig::fromDataTest("digest_test", std::move(nodes_config));
  }

  void setUp() {
    ld_check(config_ != nullptr);
    digest_ = std::make_unique<Digest>(
        TEST_LOG,
        TEST_EPOCH,
        EpochMetaData(nodeset_, replication_),
        seal_epoch_,
        config_->getNodesConfigurationFromServerConfigSource(),
        Digest::Options({bridge_for_empty_epoch_}));
  }

 public:
  const logid_t TEST_LOG{2};
  std::shared_ptr<ServerConfig> config_;
  StorageSet nodeset_{N0, N1, N2, N3};
  ReplicationProperty replication_{{NodeLocationScope::NODE, 3}};
  epoch_t seal_epoch_{50};
  esn_t lng_ = ESN_INVALID;
  esn_t tail_ = ESN_INVALID;

  bool bridge_for_empty_epoch_ = false;

  std::unique_ptr<Digest> digest_;

  // check mutation result
  bool needs_mutation_;
  std::set<ShardID> successfully_stored_;
  std::set<ShardID> amend_metadata_;
  std::set<ShardID> conflict_copies_;

  void onRecord(ShardID shard,
                esn_t::raw_type esn,
                RecordType type,
                uint32_t wave_or_seal_epoch,
                size_t payload_size = 128.,
                OffsetMap offsets = OffsetMap()) {
    ASSERT_NE(nullptr, digest_);
    digest_->onRecord(shard,
                      create_record(esn_t(esn),
                                    type,
                                    wave_or_seal_epoch,
                                    std::chrono::milliseconds(0),
                                    payload_size,
                                    std::move(offsets)));
  }

  void checkMutation(Digest::Entry& entry, bool complete_digest = false) {
    successfully_stored_.clear();
    amend_metadata_.clear();
    conflict_copies_.clear();

    ASSERT_NE(nullptr, digest_);
    needs_mutation_ = digest_->needsMutation(entry,
                                             &successfully_stored_,
                                             &amend_metadata_,
                                             &conflict_copies_,
                                             complete_digest);
  }

  esn_t applyBridgeRecords() {
    return digest_->applyBridgeRecords(lng_, &tail_);
  }
};

#define ASSERT_RESULT_SET(_set_, ...)                     \
  do {                                                    \
    ASSERT_EQ(std::set<ShardID>({__VA_ARGS__}), (_set_)); \
  } while (0)

#define ASSERT_SUCCESS_SET(...) \
  ASSERT_RESULT_SET(successfully_stored_, __VA_ARGS__);
#define ASSERT_AMEND_SET(...) ASSERT_RESULT_SET(amend_metadata_, __VA_ARGS__);
#define ASSERT_CONFLICT_SET(...) \
  ASSERT_RESULT_SET(conflict_copies_, __VA_ARGS__);

#define ASSERT_RECORD(_entry_, _rtype_, _wave_)                        \
  do {                                                                 \
    ASSERT_NE(nullptr, (_entry_).record);                              \
    ASSERT_NE(nullptr, (_entry_).record->extra_metadata_);             \
    ASSERT_EQ(_wave_, (_entry_).record->extra_metadata_->header.wave); \
    auto flags = (_entry_).record->flags_;                             \
    switch (_rtype_) {                                                 \
      case RecordType::NORMAL:                                         \
        ASSERT_FALSE(flags& RECORD_Header::WRITTEN_BY_RECOVERY);       \
        ASSERT_FALSE(flags& RECORD_Header::HOLE);                      \
        ASSERT_TRUE((_entry_).getPayload().size() > 0);                \
        break;                                                         \
      case RecordType::MUTATED:                                        \
        ASSERT_TRUE(flags& RECORD_Header::WRITTEN_BY_RECOVERY);        \
        ASSERT_FALSE(flags& RECORD_Header::HOLE);                      \
        ASSERT_TRUE((_entry_).getPayload().size() > 0);                \
        break;                                                         \
      case RecordType::HOLE:                                           \
        ASSERT_TRUE(flags& RECORD_Header::WRITTEN_BY_RECOVERY);        \
        ASSERT_TRUE(flags& RECORD_Header::HOLE);                       \
        ASSERT_TRUE((_entry_).isHolePlug());                           \
        break;                                                         \
      case RecordType::BRIDGE:                                         \
        ASSERT_TRUE(flags& RECORD_Header::WRITTEN_BY_RECOVERY);        \
        ASSERT_TRUE(flags& RECORD_Header::HOLE);                       \
        ASSERT_TRUE(flags& RECORD_Header::BRIDGE);                     \
        ASSERT_TRUE((_entry_).isHolePlug());                           \
        break;                                                         \
    }                                                                  \
  } while (0)

TEST_F(DigestTest, Basic) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  setUp();

  // enough copies for esn 1, but records not written by recovery
  onRecord(N0, /*esn=*/1, RecordType::NORMAL, /*wave=*/1);
  onRecord(N2, 1, RecordType::NORMAL, 4);
  onRecord(N3, 1, RecordType::NORMAL, 3);

  // not enough copies for esn 2
  onRecord(N1, 2, RecordType::NORMAL, 1);

  // same thing for esn 3, but with two holes, one hole written by this recovery
  // instance
  onRecord(N1, 3, RecordType::HOLE, 45);
  onRecord(N3, 3, RecordType::HOLE, seal_epoch_.val_);

  // esn 4, hole/copy conflict
  onRecord(N0, 4, RecordType::NORMAL, 99);
  onRecord(N1, 4, RecordType::HOLE, 27);
  onRecord(N2, 4, RecordType::MUTATED, 49);
  onRecord(N3, 4, RecordType::HOLE, 45);

  // esn 5, no mutation needed
  onRecord(N0, 5, RecordType::MUTATED, seal_epoch_.val_);
  onRecord(N1, 5, RecordType::MUTATED, seal_epoch_.val_);
  onRecord(N2, 5, RecordType::MUTATED, seal_epoch_.val_);

  size_t nentries = 0;
  for (auto& it : *digest_) {
    Digest::Entry& entry = it.second;
    switch (it.first.val_) {
      case 1:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0, N2, N3);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET();
        // 4 is the largest wave received
        ASSERT_RECORD(entry, RecordType::NORMAL, 4);
        checkMutation(entry, /*complete_digest*/ true);
        ASSERT_TRUE(needs_mutation_);
        break;
      case 2:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N1);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::NORMAL, 1);
        break;
      case 3:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N1);
        ASSERT_SUCCESS_SET(N3);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::HOLE, seal_epoch_.val_);
        break;
      case 4:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0, N2);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET(N1, N3);
        ASSERT_RECORD(entry, RecordType::MUTATED, 49);
        break;
      case 5:
        checkMutation(entry);
        ASSERT_FALSE(needs_mutation_);
        ASSERT_AMEND_SET();
        ASSERT_SUCCESS_SET(N0, N1, N2);
        ASSERT_CONFLICT_SET();
        // 4 is the largest wave received
        ASSERT_RECORD(entry, RecordType::MUTATED, seal_epoch_.val_);
        break;
      default:
        break;
    }
    ++nentries;
  }

  EXPECT_EQ(5, nentries);
}

// in this test, our primary focus is on the failure doamin properties,
// so all digested records are written by recovery and with their seal epoch
// the same as the seal epoch of the digest
TEST_F(DigestTest, FailureDomainBasic) {
  nodeset_ = {N0, N1, N2, N3, N4, N5, N6, N7, N8, N9, N10};
  replication_.assign(
      {{NodeLocationScope::NODE, 3}, {NodeLocationScope::RACK, 2}});

  {
    setUp();

    // 1-4 are on the same rack
    onRecord(N1, 1, RecordType::MUTATED, seal_epoch_.val_);
    onRecord(N2, 1, RecordType::MUTATED, seal_epoch_.val_);

    auto& entry = digest_->begin()->second;
    checkMutation(entry);
    ASSERT_TRUE(needs_mutation_);

    onRecord(N3, 1, RecordType::MUTATED, seal_epoch_.val_);
    onRecord(N4, 1, RecordType::MUTATED, seal_epoch_.val_);
    checkMutation(entry);
    ASSERT_TRUE(needs_mutation_);

    onRecord(N5, 1, RecordType::MUTATED, seal_epoch_.val_);
    checkMutation(entry);
    ASSERT_TRUE(needs_mutation_);

    onRecord(N9, 1, RecordType::MUTATED, seal_epoch_.val_);
    checkMutation(entry);
    ASSERT_TRUE(needs_mutation_);

    // 7 is from a different rack, at this time digest does not need
    // mutation
    onRecord(N7, 1, RecordType::MUTATED, seal_epoch_.val_);
    checkMutation(entry);
    ASSERT_FALSE(needs_mutation_);

    // receive a hole from another node, mutation is needed again
    onRecord(N10, 1, RecordType::HOLE, 40);
    checkMutation(entry);
    ASSERT_TRUE(needs_mutation_);
  }

  {
    // start a new digest
    setUp();

    // receive two hole plugs on two racks
    onRecord(N1, 1, RecordType::HOLE, seal_epoch_.val_);
    onRecord(N7, 1, RecordType::HOLE, seal_epoch_.val_);
    auto& entry = digest_->begin()->second;
    // mutation is still needed since replication factor is 3
    checkMutation(entry);
    ASSERT_TRUE(needs_mutation_);

    // receive another hole plugs from a node that does not specify
    // its rack
    onRecord(N10, 1, RecordType::HOLE, seal_epoch_.val_);

    // the plug is fully repliated, no need for muatation
    checkMutation(entry);
    ASSERT_FALSE(needs_mutation_);
  }
}

TEST_F(DigestTest, CompleteDigest) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  setUp();

  // enough copies for esn 1 with the same wave
  onRecord(N0, /*esn=*/1, RecordType::NORMAL, /*wave=*/1);
  onRecord(N2, 1, RecordType::NORMAL, 1);
  onRecord(N3, 1, RecordType::NORMAL, 1);

  // enough copies, written by different recovery instance
  onRecord(N0, 2, RecordType::HOLE, 2);
  onRecord(N1, 2, RecordType::HOLE, 2);
  onRecord(N2, 2, RecordType::HOLE, 3);

  // enough copies (N0, N1, N2) written by sequencer with same wave, but one
  // copy (N3) with different wave
  onRecord(N0, 3, RecordType::NORMAL, 3);
  onRecord(N1, 3, RecordType::NORMAL, 3);
  onRecord(N2, 3, RecordType::NORMAL, 3);
  onRecord(N3, 3, RecordType::NORMAL, 1);

  // enough copies written by the same recovery instance, but one other copy
  // but still no conflict
  onRecord(N0, 4, RecordType::MUTATED, 7);
  onRecord(N1, 4, RecordType::MUTATED, 8);
  onRecord(N2, 4, RecordType::MUTATED, 8);
  onRecord(N3, 4, RecordType::MUTATED, 8);

  // same case as above but has a conflict
  onRecord(N0, 5, RecordType::HOLE, 1);
  onRecord(N1, 5, RecordType::MUTATED, 8);
  onRecord(N2, 5, RecordType::MUTATED, 8);
  onRecord(N3, 5, RecordType::MUTATED, 8);

  size_t nentries = 0;
  for (auto& it : *digest_) {
    Digest::Entry& entry = it.second;
    switch (it.first.val_) {
      case 1:
        checkMutation(entry, /*complete_digest*/ true);
        ASSERT_FALSE(needs_mutation_);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::NORMAL, 1);
        checkMutation(entry, /*complete_digest*/ false);
        ASSERT_TRUE(needs_mutation_);
        break;
      case 2:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0, N1, N2);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::HOLE, 3);
        break;
      case 3:
        checkMutation(entry, true);
        ASSERT_FALSE(needs_mutation_);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::NORMAL, 3);
        checkMutation(entry, /*complete_digest*/ false);
        ASSERT_TRUE(needs_mutation_);
        break;
      case 4:
        checkMutation(entry, true);
        ASSERT_FALSE(needs_mutation_);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::MUTATED, 8);
        checkMutation(entry, /*complete_digest*/ false);
        ASSERT_TRUE(needs_mutation_);
        break;
      case 5:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N1, N2, N3);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET(N0);
        ASSERT_RECORD(entry, RecordType::MUTATED, 8);
        break;
      default:
        break;
    }
    ++nentries;
  }

  EXPECT_EQ(5, nentries);
}

// test the case that epoch recovery filters out mutation set from the
// set of nodes finished digesting
TEST_F(DigestTest, FilterDigest) {
  nodeset_ = {N0, N1, N2, N3, N4, N5};
  replication_.assign({{NodeLocationScope::NODE, 3}});

  seal_epoch_ = epoch_t(50);
  setUp();

  // esn 1, only one copy from N0
  onRecord(N0, /*esn=*/1, RecordType::NORMAL, /*wave=*/1);

  // esn 2, one copy from N0, N2, one hole from N1;
  // copy from N0 has precedence
  onRecord(N0, 2, RecordType::MUTATED, 7);
  onRecord(N1, 2, RecordType::HOLE, 5);
  onRecord(N2, 2, RecordType::NORMAL, 28);

  // remove N0 from the digest
  digest_->removeNode(N0);

  // target result should use copies from N0 even if it is removed
  size_t nentries = 0;
  for (auto& it : *digest_) {
    Digest::Entry& entry = it.second;
    switch (it.first.val_) {
      case 1:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::NORMAL, 1);
        break;
      case 2:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N2);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET(N1);
        ASSERT_RECORD(entry, RecordType::MUTATED, 7);
        break;
      default:
        break;
    }
    ++nentries;
  }

  EXPECT_EQ(2, nentries);
}

TEST_F(DigestTest, BridgeRecordBasic) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = esn_t(1);
  setUp();

  onRecord(N0, /*esn=*/2, RecordType::NORMAL, /*wave=*/1);
  // apply and compute the new bridge ESN
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(esn_t(3), bridge_esn);
  ASSERT_EQ(esn_t(2), tail_);
}

TEST_F(DigestTest, BridgeRecordBasic2) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = esn_t(1);
  setUp();

  onRecord(N0, /*esn=*/2, RecordType::NORMAL, /*wave=*/1);
  onRecord(N1, /*esn=*/3, RecordType::HOLE, /*wave=*/2);
  onRecord(N2, /*esn=*/4, RecordType::BRIDGE, /*wave=*/3);
  // apply and compute the new bridge ESN
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(esn_t(3), bridge_esn);
  ASSERT_EQ(esn_t(2), tail_);
}

// x_w: normal record, x(s) mutated record, o: hole, b: bridge
//
//      2      3     4     5     6
// N0  b(2)
// N1  x_3    b(7)
// N2                    x(9)
// N3         x(4)       o(3)  x(6)
TEST_F(DigestTest, BridgeRecord) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = esn_t(1);
  setUp();

  onRecord(N0, /*esn=*/2, RecordType::BRIDGE, /*wave=*/2);
  onRecord(N1, 2, RecordType::NORMAL, 3);

  onRecord(N1, 3, RecordType::BRIDGE, 7);
  onRecord(N3, 3, RecordType::MUTATED, 4);

  onRecord(N2, 5, RecordType::MUTATED, 9);
  onRecord(N3, 5, RecordType::HOLE, 3);

  onRecord(N3, 6, RecordType::MUTATED, 6);

  // apply and compute the new bridge ESN
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(esn_t(6), bridge_esn);
  ASSERT_EQ(esn_t(5), tail_);

  size_t nentries = 0;
  for (auto& it : *digest_) {
    Digest::Entry& entry = it.second;
    switch (it.first.val_) {
      case 2:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET(N1);
        ASSERT_RECORD(entry, RecordType::HOLE, 2);
        break;
      case 3:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_SUCCESS_SET();
        ASSERT_AMEND_SET(N1);
        ASSERT_CONFLICT_SET(N3);
        // the original bridge becomes a hole instead
        ASSERT_RECORD(entry, RecordType::HOLE, 7);
        break;
      case 5:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_SUCCESS_SET();
        ASSERT_AMEND_SET(N2);
        // only N3 needs to fix the conflict! N0 and N1 has a
        // implicit hole because of the bridge but should not need
        // any real fix
        ASSERT_CONFLICT_SET(N3);
        ASSERT_RECORD(entry, RecordType::MUTATED, 9);
        break;
      case 6:
        checkMutation(entry, true);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET();
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET(N3);
        // this should be the new bridge record
        ASSERT_RECORD(entry, RecordType::BRIDGE, 7);
        break;
      default:
        FAIL();
        break;
    }
    ++nentries;
  }

  EXPECT_EQ(4, nentries);
}

// do not plug bridge record for empty epoch
TEST_F(DigestTest, BridgeRecordEmptyEpoch1) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = ESN_INVALID;
  bridge_for_empty_epoch_ = false;
  setUp();
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(ESN_INVALID, bridge_esn);
  ASSERT_EQ(ESN_INVALID, tail_);
}

TEST_F(DigestTest, BridgeRecordEmptyEpoch2) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = ESN_INVALID;
  bridge_for_empty_epoch_ = true;
  setUp();
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(0, digest_->size());
  ASSERT_AMEND_SET();
  ASSERT_SUCCESS_SET();
  ASSERT_CONFLICT_SET();
  ASSERT_EQ(ESN_MIN, bridge_esn);
  ASSERT_EQ(ESN_INVALID, tail_);
}

// plug bridge for another case of empty epoch
// x_w: normal record, x(s) mutated record, o: hole, b: bridge
//      1      2
// N0  b(6)   x(1)
// N1  o(2)
// N2
// N3         o(9)
TEST_F(DigestTest, BridgeRecordEmptyEpoch3) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = ESN_INVALID;
  bridge_for_empty_epoch_ = true;
  setUp();
  onRecord(N0, /*esn=*/1, RecordType::BRIDGE, /*wave_or_recovery_epoch=*/6);
  onRecord(N0, 2, RecordType::MUTATED, 1);
  onRecord(N1, 1, RecordType::HOLE, 2);
  onRecord(N3, 3, RecordType::HOLE, 9);
  esn_t bridge_esn = applyBridgeRecords();

  auto* entry = const_cast<Digest::Entry*>(digest_->findEntry(ESN_MIN));
  ASSERT_NE(nullptr, entry);
  checkMutation((*entry), true);
  ASSERT_TRUE(needs_mutation_);
  ASSERT_AMEND_SET(N0, N1);
  ASSERT_SUCCESS_SET();
  ASSERT_CONFLICT_SET();
  ASSERT_RECORD(*entry, RecordType::BRIDGE, 6);

  ASSERT_EQ(ESN_MIN, bridge_esn);
  ASSERT_EQ(ESN_INVALID, tail_);
}

// plug bridge record at LNG + 1 for non-empty epochs
TEST_F(DigestTest, BridgeRecordNonEmptyEpochEmptyDigest) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  lng_ = esn_t(2333);
  setUp();
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(esn_t(lng_.val_ + 1), bridge_esn);
  ASSERT_EQ(lng_, tail_);
}

// do not plug bridge record if the last record is ESN_MAX
TEST_F(DigestTest, BridgeRecordESNMAX) {
  nodeset_ = {N0, N1, N2, N3};
  lng_ = esn_t(ESN_MAX.val_ - 1024);
  setUp();
  onRecord(N0, /*esn=*/ESN_MAX.val_, RecordType::NORMAL, /*wave=*/1);
  esn_t bridge_esn = applyBridgeRecords();
  ASSERT_EQ(ESN_INVALID, bridge_esn);
  ASSERT_EQ(ESN_MAX, tail_);
}

TEST_F(DigestTest, recomputeOffsetWithinEpoch) {
  nodeset_ = {N0, N1, N2, N3};
  seal_epoch_ = epoch_t(50);
  setUp();

  digest_->setDigestStartEsn(esn_t(1));

  size_t payload_size = 128;
  OffsetMap payload_size_map({{BYTE_OFFSET, payload_size}});
  // enough copies for esn 1, but records not written by recovery
  onRecord(N0,
           /*esn=*/1,
           RecordType::NORMAL,
           /*wave=*/1,
           payload_size,
           /*offsets*/ payload_size_map);
  onRecord(N2, 1, RecordType::NORMAL, 4, payload_size, payload_size_map);
  onRecord(N3, 1, RecordType::NORMAL, 3, payload_size, payload_size_map);

  onRecord(N0,
           2,
           RecordType::MUTATED,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 2);
  onRecord(N2,
           2,
           RecordType::MUTATED,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 2);
  onRecord(N3,
           2,
           RecordType::MUTATED,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 2);

  // number for the recovery epoch, just used for creating tailRecord
  lng_ = esn_t(2);

  // record after lng_ or tail_record may have the wrong byteoffset number

  // same thing for esn 3, but with two holes, one hole written by this recovery
  // instance
  // byteoffset should be payload_size * 2
  onRecord(N1, 3, RecordType::HOLE, 45, payload_size, payload_size_map * 2);
  onRecord(N3,
           3,
           RecordType::HOLE,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 2);

  // esn 4, hole/copy conflict
  // byteoffset should be payload_size * 3
  onRecord(N0, 4, RecordType::NORMAL, 99, payload_size, payload_size_map * 4);
  onRecord(N1, 4, RecordType::HOLE, 27, payload_size, payload_size_map * 3);
  onRecord(N2, 4, RecordType::MUTATED, 49, payload_size, payload_size_map * 3);
  onRecord(N3, 4, RecordType::HOLE, 45, payload_size, payload_size_map * 3);

  // esn 5,
  // byteoffset should be payload_size * 4
  // since we changed byteoffset of 5 during recovery, we still needs mutation
  // for its metadata
  onRecord(N0,
           5,
           RecordType::MUTATED,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 5);
  onRecord(N1,
           5,
           RecordType::MUTATED,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 5);
  onRecord(N2,
           5,
           RecordType::MUTATED,
           seal_epoch_.val_,
           payload_size,
           payload_size_map * 5);

  digest_->recomputeOffsetsWithinEpoch(lng_);

  size_t nentries = 0;
  for (auto& it : *digest_) {
    Digest::Entry& entry = it.second;
    switch (it.first.val_) {
      case 1:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0, N2, N3);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET();
        // 4 is the largest wave received
        ASSERT_RECORD(entry, RecordType::NORMAL, 4);
        checkMutation(entry, /*complete_digest*/ true);
        ASSERT_TRUE(needs_mutation_);
        EXPECT_EQ(
            payload_size,
            entry.record->extra_metadata_->offsets_within_epoch.getCounter(
                BYTE_OFFSET));
        break;
      case 2:
        checkMutation(entry);
        ASSERT_FALSE(needs_mutation_);
        ASSERT_AMEND_SET();
        ASSERT_SUCCESS_SET(N0, N2, N3);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::MUTATED, seal_epoch_.val_);
        EXPECT_EQ(
            payload_size * 2,
            entry.record->extra_metadata_->offsets_within_epoch.getCounter(
                BYTE_OFFSET));
        break;
      case 3:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N1);
        ASSERT_SUCCESS_SET(N3);
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::HOLE, seal_epoch_.val_);
        EXPECT_EQ(
            payload_size * 2,
            entry.record->extra_metadata_->offsets_within_epoch.getCounter(
                BYTE_OFFSET));
        break;
      case 4:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0, N2);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET(N1, N3);
        ASSERT_RECORD(entry, RecordType::MUTATED, 49);
        EXPECT_EQ(
            payload_size * 3,
            entry.record->extra_metadata_->offsets_within_epoch.getCounter(
                BYTE_OFFSET));
        break;
      case 5:
        checkMutation(entry);
        ASSERT_TRUE(needs_mutation_);
        ASSERT_AMEND_SET(N0, N1, N2);
        ASSERT_SUCCESS_SET();
        ASSERT_CONFLICT_SET();
        ASSERT_RECORD(entry, RecordType::MUTATED, seal_epoch_.val_);
        EXPECT_EQ(
            payload_size * 4,
            entry.record->extra_metadata_->offsets_within_epoch.getCounter(
                BYTE_OFFSET));
        break;
      default:
        break;
    }
    ++nentries;
  }

  EXPECT_EQ(5, nentries);
}

} // namespace
