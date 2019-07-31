/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/common/membership/SequencerMembership.h"

#include <gtest/gtest.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/membership/MembershipThriftConverter.h"
#include "logdevice/common/membership/SequencerMembership.h"
#include "logdevice/common/test/TestUtil.h"
#include "thrift/lib/cpp2/protocol/BinaryProtocol.h"
#include "thrift/lib/cpp2/protocol/Serializer.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::membership;
using namespace facebook::logdevice::membership::MembershipVersion;
using namespace facebook::logdevice::membership::MaintenanceID;

namespace {

constexpr MaintenanceID::Type DUMMY_MAINTENANCE{2333};

class SequencerMembershipTest : public ::testing::Test {
 public:
  static SequencerMembership::Update
  genUpdateOneNode(node_index_t node,
                   uint64_t base_ver,
                   SequencerMembershipTransition transition,
                   bool enabled,
                   double weight) {
    SequencerMembership::Update res{MembershipVersion::Type(base_ver)};
    MaintenanceID::Type maintenance = DUMMY_MAINTENANCE;
    int rv = res.addNode(node, {transition, enabled, weight, maintenance});
    EXPECT_EQ(0, rv);
    return res;
  }

  // add a set of nodes of the same transition into an existing update
  static void addNodes(SequencerMembership::Update* update,
                       const std::set<node_index_t>& nodes,
                       SequencerMembershipTransition transition,
                       bool enabled,
                       double weight) {
    ld_check(update != nullptr);
    MaintenanceID::Type maintenance = DUMMY_MAINTENANCE;
    for (auto node : nodes) {
      int rv =
          update->addNode(node, {transition, enabled, weight, maintenance});
      EXPECT_EQ(0, rv);
    }
  }

  static SequencerMembership::Update
  genUpdateNodes(const std::set<node_index_t>& nodes,
                 uint64_t base_ver,
                 SequencerMembershipTransition transition,
                 bool enabled,
                 double weight) {
    SequencerMembership::Update res{MembershipVersion::Type(base_ver)};
    addNodes(&res, nodes, transition, enabled, weight);
    return res;
  }

  inline void checkCodecSerialization(const SequencerMembership& m) {
    auto got = MembershipThriftConverter::fromThrift(
        MembershipThriftConverter::toThrift(m));

    ASSERT_NE(nullptr, got);
    ASSERT_EQ(m, *got);
  }
};

#define ASSERT_NODE_STATE(_m, _node, _enabled, _weight)    \
  do {                                                     \
    auto res = _m.getNodeState((_node));                   \
    EXPECT_TRUE(res.hasValue());                           \
    EXPECT_EQ(_enabled, res->sequencer_enabled);           \
    EXPECT_EQ(_weight, res->getConfiguredWeight());        \
    EXPECT_EQ(DUMMY_MAINTENANCE, res->active_maintenance); \
  } while (0)

#define ASSERT_NO_NODE(_m, _node)      \
  do {                                 \
    auto res = _m.getNodeState(_node); \
    EXPECT_FALSE(res.hasValue());      \
  } while (0)

#define ASSERT_MEMBERSHIP_NODES(_m, ...)                      \
  do {                                                        \
    auto nodes = _m.getMembershipNodes();                     \
    auto expected = std::vector<node_index_t>({__VA_ARGS__}); \
    std::sort(nodes.begin(), nodes.end());                    \
    std::sort(expected.begin(), expected.end());              \
    EXPECT_EQ(expected, nodes);                               \
  } while (0)

TEST_F(SequencerMembershipTest, EmptySequencerMembershipValid) {
  ASSERT_TRUE(SequencerMembership().validate());
  ASSERT_EQ(EMPTY_VERSION, SequencerMembership().getVersion());
  ASSERT_EQ(0, SequencerMembership().numNodes());
  ASSERT_TRUE(SequencerMembership().isEmpty());
  ASSERT_MEMBERSHIP_NODES(SequencerMembership());
}

TEST_F(SequencerMembershipTest, NodeLifeCycle) {
  SequencerMembership m{};
  // add one node N1 with weight 0.5
  int rv =
      m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                     EMPTY_VERSION.val(),
                                     SequencerMembershipTransition::ADD_NODE,
                                     true,
                                     0.5),
                    &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.numNodes());
  // version should have bumped to 1
  ASSERT_EQ(1, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), true, 0.5);

  // add another node N2, N1's state should stay intact
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(2), 1, SequencerMembershipTransition::ADD_NODE, true, 0),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(2, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), true, 0.5);
  ASSERT_NODE_STATE(m, node_index_t(2), true, 0);

  // reset N2's weight to 3.2
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(2),
                                      2,
                                      SequencerMembershipTransition::SET_WEIGHT,
                                      false /* shouldn't affect the state */,
                                      3.2),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(3, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), true, 0.5);
  ASSERT_NODE_STATE(m, node_index_t(2), true, 3.2);

  rv = m.applyUpdate(
      genUpdateOneNode(node_index_t(1),
                       3,
                       SequencerMembershipTransition::SET_ENABLED_FLAG,
                       false,
                       0.0 /* shouldn't affect the state */),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(4, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), false, 0.5);
  ASSERT_NODE_STATE(m, node_index_t(2), true, 3.2);

  // remove N1
  rv =
      m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                     4,
                                     SequencerMembershipTransition::REMOVE_NODE,
                                     true,
                                     0),
                    &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.numNodes());
  ASSERT_EQ(5, m.getVersion().val());
  ASSERT_NO_NODE(m, node_index_t(1));
  ASSERT_NODE_STATE(m, node_index_t(2), true, 3.2);

  // Add a disabled N3
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(3),
                                      5,
                                      SequencerMembershipTransition::ADD_NODE,
                                      false,
                                      1.2),
                     &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(6, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(2), true, 3.2);
  ASSERT_NODE_STATE(m, node_index_t(3), false, 1.2);

  ASSERT_MEMBERSHIP_NODES(m, 2, 3);
  checkCodecSerialization(m);
}

// test various invalid transitions
TEST_F(SequencerMembershipTest, InvalidTransitions) {
  SequencerMembership m{};
  int rv;
  // add one node N1
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                      EMPTY_VERSION.val(),
                                      SequencerMembershipTransition::ADD_NODE,
                                      true,
                                      0.1),
                     &m);
  ASSERT_EQ(0, rv);

  // remove one node that doesn't exist
  rv =
      m.applyUpdate(genUpdateOneNode(node_index_t(2),
                                     1,
                                     SequencerMembershipTransition::REMOVE_NODE,
                                     true,
                                     0),
                    &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::NOTINCONFIG, err);

  // add a node that already exists
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                      1,
                                      SequencerMembershipTransition::ADD_NODE,
                                      true,
                                      24),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::EXISTS, err);

  // try to apply an invalid update by setting
  // a negative weight
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                      2,
                                      SequencerMembershipTransition::SET_WEIGHT,
                                      true,
                                      -1.0),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);

  // try to apply an update with wrong base version
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                      3,
                                      SequencerMembershipTransition::SET_WEIGHT,
                                      true,
                                      1.0),
                     &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::VERSION_MISMATCH, err);
  ASSERT_MEMBERSHIP_NODES(m, 1);
  checkCodecSerialization(m);

  {
    // Try to finalize bootstrapping on a non bootstrapping membership
    SequencerMembership m2{};
    SequencerMembership::Update up{m2.getVersion()};
    up.finalizeBootstrapping();
    ASSERT_EQ(0, m2.applyUpdate(up, &m2));

    up = SequencerMembership::Update{m2.getVersion()};
    up.finalizeBootstrapping();
    ASSERT_EQ(-1, m2.applyUpdate(up, &m2));
    ASSERT_EQ(E::ALREADY, err);
  }
}

TEST_F(SequencerMembershipTest, FinalizeBootstrapping) {
  SequencerMembership m{};
  ASSERT_EQ(MembershipVersion::EMPTY_VERSION, m.getVersion());
  EXPECT_TRUE(m.isBootstrapping());
  checkCodecSerialization(m);
  SequencerMembership::Update update{m.getVersion()};
  update.finalizeBootstrapping();
  int rv = m.applyUpdate(update, &m);
  EXPECT_EQ(0, rv);
  EXPECT_FALSE(m.isBootstrapping());
  EXPECT_EQ(MembershipVersion::Type{MembershipVersion::EMPTY_VERSION.val() + 1},
            m.getVersion());
  checkCodecSerialization(m);
}

//////////  Testing the flatbuffers Codec ////////////////

TEST_F(SequencerMembershipTest, CodecEmptyMembership) {
  // serialize and deserialize an empty membership
  SequencerMembership m{};
  checkCodecSerialization(m);
}

TEST_F(SequencerMembershipTest, CodecBasic) {
  SequencerMembership m{};
  int rv;
  rv = m.applyUpdate(
      genUpdateNodes({node_index_t(5), node_index_t(6), node_index_t(7)},
                     EMPTY_VERSION.val(),
                     SequencerMembershipTransition::ADD_NODE,
                     true,
                     23.1),
      &m);
  ASSERT_EQ(0, rv);

  rv = m.applyUpdate(
      genUpdateNodes(
          {node_index_t(1), node_index_t(2), node_index_t(3), node_index_t(4)},
          1,
          SequencerMembershipTransition::ADD_NODE,
          true,
          0.0),
      &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(7, m.numNodes());
  checkCodecSerialization(m);
}

} // namespace
