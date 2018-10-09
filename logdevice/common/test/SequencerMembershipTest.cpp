/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include "logdevice/common/NodeID.h"
#include "logdevice/common/membership/SequencerMembership.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/test/TestUtil.h"

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
                   double weight) {
    SequencerMembership::Update res{MembershipVersion::Type(base_ver)};
    MaintenanceID::Type maintenance = DUMMY_MAINTENANCE;
    int rv = res.addNode(node, {transition, weight, maintenance});
    EXPECT_EQ(0, rv);
    return res;
  }

  // add a set of nodes of the same transition into an existing update
  static void addNodes(SequencerMembership::Update* update,
                       const std::set<node_index_t>& nodes,
                       SequencerMembershipTransition transition,
                       double weight) {
    ld_check(update != nullptr);
    MaintenanceID::Type maintenance = DUMMY_MAINTENANCE;
    for (auto node : nodes) {
      int rv = update->addNode(node, {transition, weight, maintenance});
      EXPECT_EQ(0, rv);
    }
  }

  static SequencerMembership::Update
  genUpdateNodes(const std::set<node_index_t>& nodes,
                 uint64_t base_ver,
                 SequencerMembershipTransition transition,
                 double weight) {
    SequencerMembership::Update res{MembershipVersion::Type(base_ver)};
    addNodes(&res, nodes, transition, weight);
    return res;
  }
};

#define ASSERT_NODE_STATE(_m, _node, _weight)                    \
  do {                                                           \
    auto res = _m.getNodeState((_node));                         \
    EXPECT_TRUE(res.first);                                      \
    EXPECT_EQ(_weight, res.second.weight);                       \
    EXPECT_EQ(DUMMY_MAINTENANCE, res.second.active_maintenance); \
  } while (0)

#define ASSERT_NO_NODE(_m, _node)      \
  do {                                 \
    auto res = _m.getNodeState(_node); \
    EXPECT_FALSE(res.first);           \
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
  SequencerMembership m;
  // add one node N1 with weight 0.5
  int rv =
      m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                     EMPTY_VERSION.val(),
                                     SequencerMembershipTransition::ADD_NODE,
                                     0.5),
                    &m);

  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.numNodes());
  // version should have bumped to 1
  ASSERT_EQ(1, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), 0.5);

  // add another node N2, N1's state should stay intact
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(2), 1, SequencerMembershipTransition::ADD_NODE, 0),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(2, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), 0.5);
  ASSERT_NODE_STATE(m, node_index_t(2), 0);

  // reset N2's weight to 3.2
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(2), 2, SequencerMembershipTransition::SET_WEIGHT, 3.2),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(2, m.numNodes());
  ASSERT_EQ(3, m.getVersion().val());
  ASSERT_NODE_STATE(m, node_index_t(1), 0.5);
  ASSERT_NODE_STATE(m, node_index_t(2), 3.2);

  // remove N1
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(1), 3, SequencerMembershipTransition::REMOVE_NODE, 0),
      &m);
  ASSERT_EQ(0, rv);
  ASSERT_EQ(1, m.numNodes());
  ASSERT_EQ(4, m.getVersion().val());
  ASSERT_NO_NODE(m, node_index_t(1));
  ASSERT_NODE_STATE(m, node_index_t(2), 3.2);
  ASSERT_MEMBERSHIP_NODES(m, 2);
}

// test various invalid transitions
TEST_F(SequencerMembershipTest, InvalidTransitions) {
  SequencerMembership m;
  int rv;
  // add one node N1
  rv = m.applyUpdate(genUpdateOneNode(node_index_t(1),
                                      EMPTY_VERSION.val(),
                                      SequencerMembershipTransition::ADD_NODE,
                                      0.1),
                     &m);
  ASSERT_EQ(0, rv);

  // remove one node that doesn't exist
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(2), 1, SequencerMembershipTransition::REMOVE_NODE, 0),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::NOTINCONFIG, err);

  // add a node that already exists
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(1), 1, SequencerMembershipTransition::ADD_NODE, 24),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::EXISTS, err);

  // try to apply an invalid update by setting
  // a negative weight
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(1), 2, SequencerMembershipTransition::SET_WEIGHT, -1.0),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::INVALID_PARAM, err);

  // try to apply an update with wrong base version
  rv = m.applyUpdate(
      genUpdateOneNode(
          node_index_t(1), 3, SequencerMembershipTransition::SET_WEIGHT, 1.0),
      &m);
  ASSERT_EQ(-1, rv);
  ASSERT_EQ(E::VERSION_MISMATCH, err);
  ASSERT_MEMBERSHIP_NODES(m, 1);
}

} // namespace
