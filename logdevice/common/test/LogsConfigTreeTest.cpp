/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/configuration/logs/LogsConfigTree.h"

#include <iostream>
#include <set>
#include <thread>

#include <gtest/gtest.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/configuration/logs/DefaultLogAttributes.h"
#include "logdevice/include/LogAttributes.h"

using namespace facebook::logdevice::logsconfig;
using namespace facebook::logdevice;

TEST(AttributeTest, OperatorsTest) {
  Attribute<int> attr1(1, true);
  Attribute<int> attr2(2, true);

  ASSERT_EQ(attr1, attr1);
  ASSERT_EQ(attr2, attr2);
  ASSERT_FALSE(attr1 == attr2);

  ASSERT_TRUE(attr1 < attr2);
  ASSERT_FALSE(attr2 < attr1);

  ASSERT_FALSE(attr1 < attr1);

  Attribute<int> attr3a(3, false);
  Attribute<int> attr3b(3, true);

  ASSERT_FALSE(attr3a == attr3b);
  ASSERT_TRUE(attr3a < attr3b);
  ASSERT_FALSE(attr3b < attr3a);

  Attribute<int> attr4(4, false);

  ASSERT_TRUE(attr3a < attr4);
  ASSERT_TRUE(attr3b < attr4);
  ASSERT_FALSE(attr4 < attr3a);
  ASSERT_FALSE(attr4 < attr3b);

  Attribute<int> empty;

  ASSERT_EQ(empty, empty);
  ASSERT_TRUE(empty < attr1);
  ASSERT_TRUE(empty < attr2);
  ASSERT_TRUE(empty < attr3a);
  ASSERT_TRUE(empty < attr3b);
  ASSERT_TRUE(empty < attr4);
  ASSERT_FALSE(empty < empty);
}

TEST(AttributeTest, StdSet) {
  std::set<Attribute<int>> a_set;

  Attribute<int> attr1(1, true);
  Attribute<int> attr2(2, false);

  ASSERT_TRUE(a_set.emplace(attr1).second);
  ASSERT_FALSE(a_set.emplace(attr1).second);
  ASSERT_TRUE(a_set.emplace(attr2).second);
  ASSERT_FALSE(a_set.emplace(attr2).second);

  ASSERT_EQ(1, a_set.count(attr1));
  ASSERT_EQ(1, a_set.count(attr2));

  Attribute<int> attr3t(3, true);
  Attribute<int> attr3f(3, false);

  ASSERT_TRUE(a_set.emplace(attr3t).second);
  ASSERT_TRUE(a_set.emplace(attr3f).second);

  Attribute<int> empty;

  ASSERT_TRUE(a_set.emplace(empty).second);
  ASSERT_EQ(1, a_set.count(empty));

  ASSERT_EQ(5, a_set.size());

  a_set.erase(attr1);
  a_set.erase(attr2);
  a_set.erase(attr3f);
  a_set.erase(attr3t);
  a_set.erase(empty);
  ASSERT_EQ(0, a_set.size());
}

TEST(DeduplicationTest, CommonValuesRegistry) {
  CommonValuesRegistry registry;
  LogAttributes::ExtrasMap extras = {{"K", "V"}};
  auto shadow = LogAttributes::Shadow{"ld.test", 0.1};

  LogAttributes attr1 =
      LogAttributes()
          .with_replicationFactor(14)
          .with_stickyCopySets(true)
          .with_extras(extras)
          .with_extraCopies(2)
          .with_deliveryLatency(std::chrono::milliseconds(150))
          .with_maxWritesInFlight(15)
          .with_writeToken(folly::Optional<std::string>("Hola"))
          .with_shadow(shadow);

  uintptr_t ptr1 =
      reinterpret_cast<uintptr_t>(attr1.getCommonValuesPtr().get());

  registry.deduplicate(attr1);

  uintptr_t ptr2 =
      reinterpret_cast<uintptr_t>(attr1.getCommonValuesPtr().get());

  ASSERT_EQ(ptr1, ptr2);
  ASSERT_EQ(1, registry.size());

  auto attr2 = attr1;

  registry.deduplicate(attr2);
  ptr2 = reinterpret_cast<uintptr_t>(attr2.getCommonValuesPtr().get());
  ASSERT_EQ(ptr1, ptr2);

  ASSERT_EQ(1, registry.size());

  LogAttributes attr3;
  LogAttributes attr4;
  ASSERT_NE(attr3.getCommonValuesPtr(), attr4.getCommonValuesPtr());

  registry.deduplicate(attr3);
  ASSERT_EQ(2, registry.size());

  registry.deduplicate(attr4);
  ASSERT_EQ(2, registry.size());

  ASSERT_EQ(attr3.getCommonValuesPtr(), attr4.getCommonValuesPtr());

  registry.deduplicate(attr4); // no-op
  ASSERT_EQ(2, registry.size());

  ASSERT_EQ(attr3.getCommonValuesPtr(), attr4.getCommonValuesPtr());
}

TEST(DeduplicationTest, LogsConfigTreeNodes) {
  CommonValuesRegistry registry;

  LogAttributes attrs1 = LogAttributes().with_syncedCopies(4);
  auto attrs2 = attrs1.with_replicationFactor(7);

  std::string failure;
  auto node1 = std::make_unique<DirectoryNode>("1a", attrs1);

  auto* node2 = node1->addChild("name2", attrs1);
  node2->addLogGroup(std::make_shared<LogGroupNode>(), true, failure);
  node2->addLogGroup(std::make_shared<LogGroupNode>(), true, failure);
  node1->addChild("name3", attrs2);

  node1->deduplicateRecursively(
      registry, [](const DirectoryNode*, LogGroupNodePtr) {});

  ASSERT_EQ(registry.size(), 3);
}

std::unique_ptr<LogsConfigTree> createTestTree() {
  LogAttributes::ExtrasMap extras = {{"K", "V"}};
  LogAttributes attr1 = LogAttributes()
                            .with_replicationFactor(4)
                            .with_extras(extras)
                            .with_extraCopies(1);
  LogAttributes attr2 = attr1.with_stickyCopySets(false).with_extraCopies(2);
  LogAttributes attr3 = attr2.with_replicationFactor(1).with_extraCopies(3);
  LogAttributes attr4 = attr3.with_singleWriter(true).with_extraCopies(4);

  auto defaults = DefaultLogAttributes();
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();

  auto dir1 = tree->addDirectory(tree->root(), "normal_logs", attr1);
  tree->addLogGroup(
      dir1, "normal_log1", logid_range_t{logid_t(1), logid_t(2)}, attr2);
  auto dir2 = tree->addDirectory(tree->root(), "normal_log2", attr3);
  tree->addLogGroup(
      dir2, "normal_log3", logid_range_t{logid_t(6), logid_t(9)}, attr1);

  return tree;
}

class DeduplicationTestWithParams
    : public testing::TestWithParam<std::tuple<bool, bool>> {
  // see the test body for param details
};

INSTANTIATE_TEST_CASE_P(DeduplicationTest,
                        DeduplicationTestWithParams,
                        testing::Values(std::make_tuple(false, false),
                                        std::make_tuple(true, false),
                                        std::make_tuple(true, true),
                                        std::make_tuple(false, true)));

TEST_P(DeduplicationTestWithParams, LogsConfigTreeCopy) {
  bool destructInThread = std::get<0>(GetParam());
  bool destructCopyFirst = std::get<1>(GetParam());

  auto tree1 = createTestTree();
  ASSERT_EQ(tree1->registrySize(), 0);
  tree1->deduplicateAttributes();
  ASSERT_EQ(tree1->registrySize(), 4);

  auto tree2 = tree1->copy();
  ASSERT_EQ(tree2->registrySize(), 4);

  if (!destructCopyFirst) {
    tree1.reset();
  }

  if (destructInThread) {
    std::thread([&]() { tree2.reset(); }).join();
  } else {
    tree2.reset();
  }

  if (destructCopyFirst) {
    tree1.reset();
  }
}

TEST(LogAttributesTest, AttributeMergeTest) {
  Attribute<int> attr1 = 22;
  Attribute<int> attr2;
  Attribute<int> attr3{18};
  Attribute<int> attr4{24, true};

  ASSERT_FALSE(attr1.isInherited());
  ASSERT_EQ(22, attr1.value());

  ASSERT_FALSE(attr2.isInherited());
  ASSERT_FALSE(attr2.hasValue());
  ASSERT_FALSE(attr2);

  // Parent wins
  auto merged1 = attr2.withParent(attr1);
  ASSERT_TRUE(merged1.isInherited());
  ASSERT_TRUE(merged1.hasValue());
  ASSERT_TRUE(merged1);
  ASSERT_EQ(22, merged1.value());

  // Child wins
  auto merged2 = attr3.withParent(attr1);
  ASSERT_FALSE(merged2.isInherited());
  ASSERT_TRUE(merged2.hasValue());
  ASSERT_EQ(18, merged2.value());

  // New Parent Wins
  auto merged3 = attr4.withParent(attr1);
  ASSERT_TRUE(merged3.isInherited());
  ASSERT_TRUE(merged3.hasValue());
  ASSERT_EQ(22, merged3.value());

  ASSERT_EQ(22, *merged3);
  ASSERT_EQ(25, *Attribute<int>(25));

  ASSERT_TRUE(merged3);

  Attribute<std::string> attr5;
  ASSERT_FALSE(attr5);
}

TEST(LogAttributesTest, LogAttributesMergeTest) {
  LogAttributes::ExtrasMap extras = {{"Key", "Value"}};
  auto shadow = LogAttributes::Shadow{"logdevice.test", 0.1};
  LogAttributes parent =
      LogAttributes()
          .with_replicationFactor(14)
          .with_stickyCopySets(true)
          .with_extras(extras)
          .with_extraCopies(2)
          .with_deliveryLatency(std::chrono::milliseconds(150))
          .with_maxWritesInFlight(15)
          .with_writeToken(folly::Optional<std::string>("Hola"))
          .with_shadow(shadow);

  std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
  permission_array.fill(false);
  permission_array[static_cast<int>(ACTION::READ)] = true;
  permission_array[static_cast<int>(ACTION::TRIM)] = true;

  auto pem =
      LogAttributes::PermissionsMap{std::make_pair("ADMINS", permission_array)};

  LogAttributes attrs = LogAttributes()
                            .with_replicationFactor(22)
                            .with_singleWriter(true)
                            // inherited attributes are not serialized
                            .with_nodeSetSize(22)
                            .with_scdEnabled(false)
                            .with_syncedCopies(1)
                            .with_backlogDuration(std::chrono::seconds(15))
                            .with_syncReplicationScope(NodeLocationScope::ROOT)
                            .with_permissions(pem);

  LogAttributes combined = LogAttributes(attrs, parent);
  ASSERT_EQ(Attribute<int>(22, false), combined.replicationFactor());
  ASSERT_EQ(Attribute<int>(2, true), combined.extraCopies());
  ASSERT_EQ(Attribute<int>(1, false), combined.syncedCopies());
  ASSERT_EQ(Attribute<int>(15, true), combined.maxWritesInFlight());
  ASSERT_EQ(Attribute<bool>(true, false), combined.singleWriter());
  ASSERT_EQ(Attribute<NodeLocationScope>(NodeLocationScope::ROOT, false),
            combined.syncReplicationScope());
  ASSERT_EQ(Attribute<folly::Optional<std::chrono::seconds>>(
                std::chrono::seconds(15), false),
            combined.backlogDuration());
  ASSERT_EQ(combined.nodeSetSize(), Attribute<folly::Optional<int>>(22, false));
  ASSERT_EQ(Attribute<folly::Optional<std::chrono::milliseconds>>(
                std::chrono::milliseconds(150), true),
            combined.deliveryLatency());
  ASSERT_EQ(Attribute<bool>(false, false), combined.scdEnabled());
  ASSERT_EQ(Attribute<folly::Optional<std::string>>(
                folly::Optional<std::string>("Hola"), true),
            combined.writeToken());
  ASSERT_EQ(Attribute<bool>(true, true), combined.stickyCopySets());
  ASSERT_EQ(Attribute<LogAttributes::PermissionsMap>(pem, false),
            combined.permissions());
  ASSERT_EQ(
      Attribute<LogAttributes::ExtrasMap>(extras, true), combined.extras());
  ASSERT_EQ(Attribute<LogAttributes::Shadow>(shadow, true), combined.shadow());
}
TEST(LogAttributesTest, ImmutabilityTest) {
  LogAttributes x1 = LogAttributes().with_replicationFactor(22);
  LogAttributes x2 = x1.with_syncedCopies(55);
  ASSERT_NE(&x1, &x2);
  ASSERT_EQ(22, x2.replicationFactor());
  ASSERT_NE(x1, x2);
  ASSERT_EQ(55, x2.syncedCopies());
  ASSERT_FALSE(x1.syncedCopies());
}

TEST(LogAttributesTest, EqualityTest) {
  Attribute<int> x1;
  Attribute<int> x2;
  ASSERT_TRUE(x1 == x2);
  x2 = 25;
  ASSERT_FALSE(x1 == x2);
  ASSERT_FALSE(x1 == 0);
  x1 = 25;
  ASSERT_TRUE(x1 == x2);
  ASSERT_TRUE(x1 == 25);
  ASSERT_TRUE(x2 == 25);
  ASSERT_TRUE(25 == x2);
  x2 = Attribute<int>{25, true};
  ASSERT_FALSE(x1 == x2);
  ASSERT_TRUE(x2 == 25);
  x1 = Attribute<int>{25, true};
  ASSERT_TRUE(x1 == x2);
}

TEST(LogAttributesTest, ComparisonOpTest) {
  LogAttributes attr1 = DefaultLogAttributes();
  LogAttributes attr2 = DefaultLogAttributes();

  attr1 = attr1.with_syncedCopies(5);
  ASSERT_NE(attr1, attr2);

  attr2 = attr2.with_syncedCopies(attr1.syncedCopies());
  ASSERT_EQ(attr1, attr2);

  attr1 = attr1.with_monitoringTier(
      folly::Optional<monitoring_tier_t>(monitoring_tier_t(17)));
  ASSERT_NE(attr1, attr2);

  attr2 = attr2.with_monitoringTier(attr1.monitoringTier());
  ASSERT_EQ(attr1, attr2);

  attr1 = attr1.with_tailOptimized(false);
  attr2 = attr2.with_tailOptimized(true);
  ASSERT_NE(attr1, attr2);

  attr2 = attr2.with_tailOptimized(false);
  ASSERT_EQ(attr1, attr2);
}

TEST(LogsConfigTreeTest, ImmutabilityTest) {
  auto defaults = DefaultLogAttributes();
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  auto dir2 = tree->addDirectory(
      tree->root(),
      "normal_logs",
      LogAttributes().with_syncedCopies(1).with_replicationFactor(2));
  auto lg1 = tree->addLogGroup(dir2,
                               "normal_log1",
                               logid_range_t{logid_t(1), logid_t(10)},
                               LogAttributes().with_extraCopies(0));

  ASSERT_EQ(1, lg1->attrs().syncedCopies().value());
  std::unique_ptr<LogsConfigTree> snapshot1 = tree->copy();
  ASSERT_TRUE(snapshot1);
  auto dir3 = tree->addDirectory(tree->root(), "abnormal_logs");
  ASSERT_TRUE(dir3);
  ASSERT_TRUE(tree->findDirectory("/abnormal_logs"));
  ASSERT_FALSE(snapshot1->findDirectory("/abnormal_logs"));
  ASSERT_TRUE(tree->findDirectory("/normal_logs"));
  ASSERT_TRUE(snapshot1->findDirectory("/normal_logs"));
}

TEST(LogsConfigTreeTest, TestSnapshottingPerformance) {
  auto defaults = DefaultLogAttributes();
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  auto start1 = std::chrono::high_resolution_clock::now();
  auto start2 = start1;
  for (int i = 1; i <= 500000; i++) {
    tree->addLogGroup("/log-" + std::to_string(i),
                      logid_range_t{logid_t(i), logid_t(i)},
                      LogAttributes().with_replicationFactor(3),
                      false);
    if (i % 10000 == 0) {
      auto end2 = std::chrono::high_resolution_clock::now();
      std::cout << "10k log groups took "
                << std::chrono::duration_cast<std::chrono::milliseconds>(
                       (end2 - start2))
                       .count()
                << "ms i=" << i << std::endl;
      start2 = std::chrono::high_resolution_clock::now();
    }
  }
  auto end1 = std::chrono::high_resolution_clock::now();
  std::cout << "Adding 500k log groups took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   (end1 - start1))
                   .count()
            << "ms" << std::endl;
  auto start3 = std::chrono::high_resolution_clock::now();
  auto tree2 = tree->copy();
  auto end3 = std::chrono::high_resolution_clock::now();
  std::cout << "Cloning the tree took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   (end3 - start3))
                   .count()
            << "ms" << std::endl;
}

TEST(LogsConfigTreeTest, TestFindDirectory) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  auto dir1 = tree->addDirectory(tree->root(), "dir1");
  tree->addDirectory(dir1, "dir1_1");
  tree->addDirectory(dir1, "dir1_2");

  auto root_dir = tree->findDirectory("/");
  ASSERT_TRUE(root_dir);
  ASSERT_EQ(tree->root(), root_dir);

  auto res1 = tree->findDirectory("/dir1");
  ASSERT_TRUE(res1);
  ASSERT_EQ("/dir1/", res1->getFullyQualifiedName());
  ASSERT_EQ(res1, tree->findDirectory("/dir1/"));
  auto res2 = tree->findDirectory("/dir1/dir1_2");
  ASSERT_TRUE(res2);
  ASSERT_EQ("/dir1/dir1_2/", res2->getFullyQualifiedName());
}

TEST(LogsConfigTreeTest, TestFindLogByID) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  LogAttributes base_attrs =
      LogAttributes().with_replicationFactor(2).with_extraCopies(0);
  auto dir1 = tree->addDirectory(
      tree->root(), "super_logs", base_attrs.with_replicationFactor(10));
  auto dir2 = tree->addDirectory(
      tree->root(), "normal_logs", base_attrs.with_syncedCopies(10));
  auto lg1 = tree->addLogGroup(
      dir2, "log_group1", logid_range_t{logid_t(1), logid_t(10)});
  auto lg2 = tree->addLogGroup(
      dir1, "log_group2", logid_range_t{logid_t(20), logid_t(100)});
  ASSERT_TRUE(lg1);
  ASSERT_TRUE(lg2);

  const LogGroupInDirectory* llg1 = tree->getLogGroupByID(logid_t(5));
  ASSERT_TRUE(llg1 != nullptr);
  ASSERT_EQ(lg1, llg1->log_group);
  ASSERT_EQ(*lg2, *tree->getLogGroupByID(logid_t(24))->log_group);
  auto not_found = tree->getLogGroupByID(logid_t(201));
  ASSERT_FALSE(not_found);
}

TEST(LogsConfigTreeTest, TestMetadataLogAddFail) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  LogAttributes base_attrs =
      LogAttributes().with_replicationFactor(2).with_extraCopies(0);
  auto dir1 = tree->addDirectory(
      tree->root(), "normal_logs", base_attrs.with_syncedCopies(10));
  auto lg1 = tree->addLogGroup(
      dir1, "log_group1", logid_range_t{logid_t(1), logid_t(10)});
  ASSERT_TRUE(lg1);
  auto lg2 = tree->addLogGroup(
      dir1,
      "log_group2",
      logid_range_t{MetaDataLog::metaDataLogID(logid_t(20)),
                    MetaDataLog::metaDataLogID(logid_t(100))});
  ASSERT_EQ(nullptr, lg2);
  ASSERT_EQ(E::INVALID_CONFIG, err);
}

TEST(LogsConfigTreeTest, TestDirectoryExists) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  auto dir1 = tree->addDirectory(tree->root(), "super_logs");
  ASSERT_TRUE(dir1);
  ASSERT_TRUE(tree->addDirectory("/my_logs/your_logs/his_logs", true));
  ASSERT_FALSE(tree->addDirectory("/my_logs/your_logs/his_logs", true));
  ASSERT_EQ(E::EXISTS, err);
  ASSERT_FALSE(tree->addDirectory("/my_logs/your_logs/his_logs", false));
  ASSERT_EQ(E::EXISTS, err);
  ASSERT_TRUE(dir1);
  ASSERT_FALSE(tree->addDirectory(tree->root(), "super_logs"));
  ASSERT_EQ(E::EXISTS, err);
  ASSERT_FALSE(tree->addDirectory("/super_logs", true));
  ASSERT_EQ(E::EXISTS, err);
}

TEST(LogsConfigTreeTest, TestLogIDExists) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  auto dir1 = tree->addDirectory(
      tree->root(), "super_logs", LogAttributes().with_replicationFactor(10));
  auto dir2 = tree->addDirectory(
      tree->root(), "normal_logs", LogAttributes().with_syncedCopies(10));
  auto lg1 = tree->addLogGroup(
      dir2, "log_group1", logid_range_t{logid_t(1), logid_t(10)});
  auto lg2 = tree->addLogGroup(
      dir1, "log_group2", logid_range_t{logid_t(20), logid_t(100)});

  // logID overlap
  lg2 = tree->addLogGroup(
      dir2, "normal_log2", logid_range_t{logid_t(5), logid_t(10)});
  ASSERT_FALSE(lg2);
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);
}

TEST(LogsConfigTreeTest, TestLogGroupExists) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  LogAttributes base_attrs =
      LogAttributes().with_replicationFactor(2).with_extraCopies(0);
  tree->addDirectory(
      tree->root(), "super_logs", base_attrs.with_replicationFactor(10));
  auto dir2 = tree->addDirectory(
      tree->root(), "normal_logs", base_attrs.with_syncedCopies(1));
  auto lg1 = tree->addLogGroup(
      dir2, "log_group1", logid_range_t{logid_t(1), logid_t(10)});

  ASSERT_FALSE(tree->findLogGroup("/super_logs"));
  auto res = tree->findLogGroup("/normal_logs/log_group1");
  ASSERT_TRUE(res);
  ASSERT_EQ(*lg1, *res);
}

TEST(LogsConfigTreeTest, TestReplaceLogGroup) {
  LogAttributes base_attrs =
      DefaultLogAttributes()
          .with_replicationFactor(2)
          .with_extraCopies(0)
          .with_syncReplicationScope(NodeLocationScope::REGION);
  std::unique_ptr<LogsConfigTree> tree =
      LogsConfigTree::create("/", base_attrs);
  // /super_logs (replicationFactor = 10)
  ASSERT_TRUE(tree->addDirectory(
      "/super_logs", false, base_attrs.with_replicationFactor(10)));
  // /normal_logs (syncedCopies = 1)
  ASSERT_TRUE(tree->addDirectory("/normal_logs", false));
  ASSERT_TRUE(tree->addDirectory("/normal_logs/not-so-normal"));

  // /normal_logs/log_group1 (inherits all)
  auto group1 = tree->addLogGroup(
      "/normal_logs/log_group1", logid_range_t{logid_t(1), logid_t(10)});
  // /normal_logs/not-so-normal/log_group2 (replicationFactor = 2)
  ASSERT_TRUE(tree->addLogGroup("/normal_logs/not-so-normal/log_group2",
                                logid_range_t{logid_t(11), logid_t(20)},
                                LogAttributes().with_replicationFactor(2)));
  // /normal_logs/not-so-normal/log_group3
  ASSERT_TRUE(tree->addLogGroup("/normal_logs/not-so-normal/log_group3",
                                logid_range_t{logid_t(21), logid_t(40)}));
  LogGroupNode replacement1 = group1->withRange(
      logid_range_t(logid_t(100), logid_t(1ull << LOGID_BITS)));
  ASSERT_FALSE(tree->replaceLogGroup("/normal_logs/log_group1", replacement1));
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);
  ASSERT_NE(nullptr, tree->find("/normal_logs/log_group1"));
  ASSERT_TRUE(tree->getLogGroupByID(logid_t(4)));
  // try a replacement that clashes with others
  auto replacement2 = group1->withRange(logid_range_t(logid_t(1), logid_t(22)));
  ASSERT_FALSE(tree->replaceLogGroup("/normal_logs/log_group1", replacement2));
  ASSERT_EQ(E::ID_CLASH, err);
  ASSERT_NE(nullptr, tree->find("/normal_logs/log_group1"));
  ASSERT_TRUE(tree->getLogGroupByID(logid_t(4)));
  // replace with invalid attributes
  auto replacement3 =
      group1->withLogAttributes(LogAttributes().with_extraCopies(-1));
  ASSERT_FALSE(tree->replaceLogGroup("/normal_logs/log_group1", replacement3));
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);
  ASSERT_NE(nullptr, tree->find("/normal_logs/log_group1"));
  ASSERT_TRUE(tree->getLogGroupByID(logid_t(4)));
  ASSERT_NE(-1,
            tree->getLogGroupByID(logid_t(4))
                ->log_group->attrs()
                .extraCopies()
                .value());
  // actually replacings
  auto replacement4 =
      group1->withLogAttributes(LogAttributes().with_extraCopies(20));
  ASSERT_TRUE(tree->replaceLogGroup("/normal_logs/log_group1", replacement4));
  ASSERT_NE(nullptr, tree->find("/normal_logs/log_group1"));
  ASSERT_EQ(20,
            tree->getLogGroupByID(logid_t(4))
                ->log_group->attrs()
                .extraCopies()
                .value());
}

TEST(LogsConfigTreeTest, TestRename) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  LogAttributes base_attrs =
      LogAttributes().with_replicationFactor(2).with_extraCopies(0);
  // /super_logs
  tree->addDirectory(
      tree->root(), "super_logs", base_attrs.with_replicationFactor(10));
  // /normal_logs
  auto dir2 = tree->addDirectory(
      tree->root(), "normal_logs", base_attrs.with_syncedCopies(1));
  // /normal_logs/log_group1
  auto lg1 = tree->addLogGroup(
      dir2, "log_group1", logid_range_t{logid_t(1), logid_t(10)});

  ASSERT_EQ(-1, tree->rename("/", "/dir2"));
  ASSERT_EQ(E::INVALID_PARAM, err);
  ASSERT_EQ(-1, tree->rename("/", "/"));
  ASSERT_EQ(E::INVALID_PARAM, err);
  ASSERT_EQ(-1, tree->rename("/test_me", "/test_you"));
  ASSERT_EQ(E::NOTFOUND, err);
  ASSERT_EQ(0, tree->rename("/normal_logs", "/not_normal_logs"));
  ASSERT_EQ(nullptr, tree->findDirectory("/normal_logs"));
  ASSERT_EQ(nullptr, tree->find("/normal_logs/log_group1"));
  ASSERT_NE(nullptr, tree->find("/not_normal_logs/log_group1"));
  ASSERT_EQ(0,
            tree->rename(
                "/not_normal_logs/log_group1", "/not_normal_logs/log_group2"));
  ASSERT_EQ(nullptr, tree->find("/not_normal_logs/log_group1"));
  ASSERT_NE(nullptr, tree->find("/not_normal_logs/log_group2"));
}

TEST(LogsConfigTreeTest, TestNarrowestReplication) {
  std::unique_ptr<LogsConfigTree> tree = LogsConfigTree::create();
  LogAttributes base_attrs = LogAttributes().with_extraCopies(0);
  // /super_logs
  ASSERT_TRUE(tree->addDirectory(
      tree->root(),
      "super_logs0",
      base_attrs.with_replicateAcross({{NodeLocationScope::RACK, 4}})));

  ASSERT_TRUE(tree->addDirectory(tree->root(), "super_logs1", LogAttributes()));

  ASSERT_TRUE(tree->addLogGroup(
      "/super_logs0/log_group1",
      logid_range_t{logid_t(1), logid_t(10)},
      LogAttributes().with_replicateAcross({{NodeLocationScope::REGION, 2}})));

  ASSERT_TRUE(tree->addLogGroup(
      "/super_logs1/log_group2",
      logid_range_t{logid_t(20), logid_t(30)},
      LogAttributes().with_replicateAcross({{NodeLocationScope::NODE, 2}})));

  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
            tree->findDirectory("/")->getNarrowestReplication().toString());
  ASSERT_EQ(ReplicationProperty({{NodeLocationScope::NODE, 2}}).toString(),
            tree->getNarrowestReplication().toString());
}

TEST(LogsConfigTreeTest, TestSetAttributes) {
  LogAttributes base_attrs =
      DefaultLogAttributes()
          .with_replicationFactor(2)
          .with_extraCopies(0)
          .with_syncReplicationScope(NodeLocationScope::REGION);
  std::unique_ptr<LogsConfigTree> tree =
      LogsConfigTree::create("/", base_attrs);
  // /super_logs (replicationFactor = 10)
  ASSERT_TRUE(tree->addDirectory(
      "/super_logs", false, base_attrs.with_replicationFactor(10)));
  // /normal_logs (syncedCopies = 1)
  ASSERT_TRUE(tree->addDirectory("/normal_logs", false));
  ASSERT_TRUE(tree->addDirectory("/normal_logs/not-so-normal"));

  // /normal_logs/log_group1 (inherits all)
  ASSERT_TRUE(tree->addLogGroup(
      "/normal_logs/log_group1", logid_range_t{logid_t(1), logid_t(10)}));
  // /normal_logs/not-so-normal/log_group2 (replicationFactor = 2)
  ASSERT_TRUE(tree->addLogGroup("/normal_logs/not-so-normal/log_group2",
                                logid_range_t{logid_t(11), logid_t(20)},
                                LogAttributes().with_replicationFactor(2)));
  // /normal_logs/not-so-normal/log_group3
  ASSERT_TRUE(tree->addLogGroup("/normal_logs/not-so-normal/log_group3",
                                logid_range_t{logid_t(30), logid_t(40)}));

  ASSERT_EQ(2,
            tree->find("/normal_logs/log_group1")
                ->attrs()
                .replicationFactor()
                .value());

  LogAttributes::ExtrasMap extras{
      {"My Key1", "My Value1"}, {"My Key2", "My Value2"}};
  std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
  permission_array.fill(false);
  permission_array[static_cast<int>(ACTION::READ)] = true;
  permission_array[static_cast<int>(ACTION::TRIM)] = true;
  auto pem =
      LogAttributes::PermissionsMap{std::make_pair("ADMINS", permission_array)};
  auto shadow = LogAttributes::Shadow{"logdevice.test", 0.1};

  ASSERT_EQ(0,
            tree->setAttributes("/",
                                base_attrs.with_replicationFactor(24)
                                    .with_permissions(pem)
                                    .with_extras(extras)
                                    .with_shadow(shadow)));
  auto lg2 = tree->find("/normal_logs/not-so-normal/log_group2")->attrs();
  ASSERT_EQ(2, lg2.replicationFactor().value());
  ASSERT_TRUE(lg2.extras().isInherited());
  ASSERT_EQ(extras, lg2.extras().value());
  ASSERT_TRUE(lg2.permissions().isInherited());
  ASSERT_EQ(pem, lg2.permissions().value());
  ASSERT_EQ(shadow, lg2.shadow().value());
  ASSERT_TRUE(lg2.shadow().isInherited());
  ASSERT_EQ(0,
            tree->setAttributes("/normal_logs/not-so-normal",
                                base_attrs.with_replicationFactor(14)));
  ASSERT_EQ(2,
            tree->find("/normal_logs/not-so-normal/log_group2")
                ->attrs()
                .replicationFactor()
                .value());
  ASSERT_EQ(14,
            tree->find("/normal_logs/not-so-normal/log_group3")
                ->attrs()
                .replicationFactor()
                .value());
  ASSERT_EQ(-1,
            tree->setAttributes(
                "/normal_logs", LogAttributes().with_replicationFactor(-1)));
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);

  // test invalid shadow attributes
  shadow = LogAttributes::Shadow{"", 0.1};
  ASSERT_EQ(-1, tree->setAttributes("/", LogAttributes().with_shadow(shadow)));
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);
  shadow = LogAttributes::Shadow{"", -0.01};
  ASSERT_EQ(-1, tree->setAttributes("/", LogAttributes().with_shadow(shadow)));
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);
  shadow = LogAttributes::Shadow{"", 1.01};
  ASSERT_EQ(-1, tree->setAttributes("/", LogAttributes().with_shadow(shadow)));
  ASSERT_EQ(E::INVALID_ATTRIBUTES, err);
}

TEST(LogsConfigTreeTest, TestDelete) {
  LogAttributes base_attrs =
      DefaultLogAttributes()
          .with_replicationFactor(2)
          .with_extraCopies(0)
          .with_syncReplicationScope(NodeLocationScope::REGION);
  std::unique_ptr<LogsConfigTree> tree =
      LogsConfigTree::create("/", base_attrs);
  // /super_logs (replicationFactor = 10)
  ASSERT_TRUE(tree->addDirectory(
      "/super_logs", false, base_attrs.with_replicationFactor(10)));
  // /normal_logs (syncedCopies = 1)
  ASSERT_TRUE(tree->addDirectory("/normal_logs", false));
  ASSERT_TRUE(tree->addDirectory("/normal_logs/not-so-normal"));

  // /normal_logs/log_group1 (inherits all)
  ASSERT_TRUE(tree->addLogGroup(
      "/normal_logs/log_group1", logid_range_t{logid_t(1), logid_t(10)}));
  // /normal_logs/not-so-normal/log_group2 (replicationFactor = 2)
  ASSERT_TRUE(tree->addLogGroup("/normal_logs/not-so-normal/log_group2",
                                logid_range_t{logid_t(11), logid_t(20)},
                                LogAttributes().with_replicationFactor(2)));
  // /normal_logs/not-so-normal/log_group3
  ASSERT_TRUE(tree->addLogGroup("/normal_logs/not-so-normal/log_group3",
                                logid_range_t{logid_t(30), logid_t(40)}));

  // non recursive delete
  ASSERT_EQ(-1, tree->deleteDirectory("/normal_logs", false));
  ASSERT_EQ(E::NOTEMPTY, err);
  ASSERT_NE(nullptr, tree->find("/normal_logs"));
  ASSERT_EQ(0, tree->deleteDirectory("/super_logs", false));
  ASSERT_EQ(nullptr, tree->find("/super_logs"));
  // deleting root
  ASSERT_NE(0, tree->deleteDirectory("/", true));
  ASSERT_EQ(E::INVALID_PARAM, err);
  // delete one log group
  ASSERT_EQ(0, tree->deleteLogGroup("/normal_logs/not-so-normal/log_group3"));
  ASSERT_EQ(nullptr, tree->find("/normal_logs/not-so-normal/log_group3"));
  // not-found
  ASSERT_EQ(-1, tree->deleteLogGroup("/normal_logs/not-so-normal/log_group3"));
  ASSERT_EQ(E::NOTFOUND, err);
  // reursive delete
  ASSERT_EQ(0, tree->deleteDirectory("/normal_logs", true));
  ASSERT_EQ(nullptr, tree->find("/normal_logs"));
  ASSERT_EQ(nullptr, tree->getLogGroupByID(logid_t(4)));
  ASSERT_EQ(nullptr, tree->getLogGroupByID(logid_t(15)));
  ASSERT_EQ(nullptr, tree->getLogGroupByID(logid_t(35)));
}
