/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <unordered_map>

#include <flatbuffers/flatbuffers.h>
#include <gtest/gtest.h>

#include "logdevice/common/MetaDataLog.h"
#include "logdevice/common/SecurityInformation.h"
#include "logdevice/common/configuration/logs/FBuffersLogsConfigCodec.h"
#include "logdevice/include/LogAttributes.h"

using namespace facebook::logdevice::logsconfig;
using namespace facebook::logdevice;

TEST(LogsConfigCodecTest, AttributesSerializeTest1) {
  flatbuffers::FlatBufferBuilder builder;
  LogAttributes attrs;
  attrs = LogAttributes()
              .with_replicationFactor(22)
              .with_singleWriter(true)
              // inherited attributes are not serialized
              .with_nodeSetSize(Attribute<folly::Optional<int>>(22, true))
              .with_scdEnabled(false)
              .with_backlogDuration(folly::Optional<std::chrono::seconds>(
                  std::chrono::seconds(15)))
              .with_syncReplicationScope(NodeLocationScope::REGION);
  flatbuffers::Offset<fbuffers::LogAttrs> buf =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                  fbuffers::LogAttrs>(
          builder, attrs, false);
  builder.Finish(buf);

  // deserialize
  void* out = static_cast<void*>(builder.GetBufferPointer());
  auto recovered = flatbuffers::GetRoot<fbuffers::LogAttrs>(out);
  LogAttributes att =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  ASSERT_EQ(22, att.replicationFactor());
  ASSERT_TRUE(att.singleWriter().hasValue());
  ASSERT_TRUE(att.singleWriter().value());
  ASSERT_TRUE(att.scdEnabled().hasValue());
  ASSERT_FALSE(att.scdEnabled().value());
  ASSERT_EQ(NodeLocationScope::REGION, att.syncReplicationScope());
  ASSERT_EQ(std::chrono::seconds(15), att.backlogDuration().value().value());
  ASSERT_FALSE(att.permissions().hasValue());
  ASSERT_FALSE(att.extras().hasValue());
  ASSERT_FALSE(att.acls().hasValue());
  ASSERT_FALSE(att.aclsShadow().hasValue());
}

TEST(LogsConfigCodecTest, AttributesPermissionsSerialize) {
  flatbuffers::FlatBufferBuilder builder;
  std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
  permission_array.fill(false);
  permission_array[static_cast<int>(ACTION::READ)] = true;
  permission_array[static_cast<int>(ACTION::TRIM)] = true;

  auto pem =
      LogAttributes::PermissionsMap{std::make_pair("ADMINS", permission_array)};
  LogAttributes attrs =
      LogAttributes().with_permissions(pem).with_replicationFactor(25);
  flatbuffers::Offset<fbuffers::LogAttrs> buf =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                  fbuffers::LogAttrs>(
          builder, attrs, false);
  builder.Finish(buf);

  auto recovered =
      flatbuffers::GetRoot<fbuffers::LogAttrs>(builder.GetBufferPointer());
  LogAttributes att =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  ASSERT_EQ(25, att.replicationFactor());
  ASSERT_FALSE(att.extras().hasValue());
  ASSERT_TRUE(att.permissions().hasValue());
  ASSERT_EQ(pem.size(), att.permissions().value().size());
  ASSERT_EQ(pem, att.permissions().value());
}

TEST(LogsConfigCodecTest, AttributesACLsSerialize) {
  flatbuffers::FlatBufferBuilder builder;
  LogAttributes::ACLList acl_list;
  acl_list.push_back("acl_test1");
  acl_list.push_back("acl_test2");

  LogAttributes::ACLList acl_shadow_list;
  acl_shadow_list.push_back("acl_shadow_test1");
  acl_shadow_list.push_back("acl_shadow_test2");

  LogAttributes attrs = LogAttributes()
                            .with_acls(acl_list)
                            .with_aclsShadow(acl_shadow_list)
                            .with_replicationFactor(25);
  flatbuffers::Offset<fbuffers::LogAttrs> buf =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                  fbuffers::LogAttrs>(
          builder, attrs, false);
  builder.Finish(buf);

  auto recovered =
      flatbuffers::GetRoot<fbuffers::LogAttrs>(builder.GetBufferPointer());
  LogAttributes att =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  ASSERT_EQ(25, att.replicationFactor());
  ASSERT_FALSE(att.extras().hasValue());

  ASSERT_TRUE(att.acls().hasValue());
  ASSERT_EQ(acl_list.size(), att.acls().value().size());
  ASSERT_EQ(acl_list, att.acls().value());

  ASSERT_TRUE(att.aclsShadow().hasValue());
  ASSERT_EQ(acl_shadow_list.size(), att.aclsShadow().value().size());
  ASSERT_EQ(acl_shadow_list, att.aclsShadow().value());
}

TEST(LogsConfigCodecTest, AttributesExtrasSerialize) {
  flatbuffers::FlatBufferBuilder builder;
  LogAttributes::ExtrasMap extras{
      {"My Key1", "My Value1"}, {"My Key2", "My Value2"}};
  LogAttributes attrs =
      LogAttributes().with_extras(extras).with_replicationFactor(25);

  flatbuffers::Offset<fbuffers::LogAttrs> buf =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                  fbuffers::LogAttrs>(
          builder, attrs, false);
  builder.Finish(buf);

  auto recovered =
      flatbuffers::GetRoot<fbuffers::LogAttrs>(builder.GetBufferPointer());
  LogAttributes att =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  ASSERT_EQ(25, att.replicationFactor());
  ASSERT_TRUE(att.extras().hasValue());
  ASSERT_FALSE(att.permissions().hasValue());
  ASSERT_EQ(extras, att.extras());
}

TEST(LogsConfigCodecTest, AttibutesReplicateAcross) {
  flatbuffers::FlatBufferBuilder builder;
  LogAttributes::ScopeReplicationFactors replicate_across{
      {NodeLocationScope::NODE, 3}, {NodeLocationScope::RACK, 2}};
  LogAttributes attrs = LogAttributes().with_replicateAcross(replicate_across);

  flatbuffers::Offset<fbuffers::LogAttrs> buf =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                  fbuffers::LogAttrs>(
          builder, attrs, false);
  builder.Finish(buf);

  auto recovered =
      flatbuffers::GetRoot<fbuffers::LogAttrs>(builder.GetBufferPointer());
  LogAttributes att =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  ASSERT_TRUE(att.replicateAcross().hasValue());
  ASSERT_EQ(replicate_across, att.replicateAcross());
}

TEST(LogsConfigCodecTest, AttributesShadowSerialize) {
  // setup
  flatbuffers::FlatBufferBuilder builder;
  LogAttributes::Shadow shadow{"logdevice.test", 0.1};
  LogAttributes attrs =
      LogAttributes().with_shadow(shadow).with_replicationFactor(25);

  // serialize
  flatbuffers::Offset<fbuffers::LogAttrs> buf =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                  fbuffers::LogAttrs>(
          builder, attrs, false);
  builder.Finish(buf);

  // deserialize
  auto recovered =
      flatbuffers::GetRoot<fbuffers::LogAttrs>(builder.GetBufferPointer());
  LogAttributes att =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  ASSERT_EQ(25, att.replicationFactor());
  ASSERT_TRUE(att.shadow().hasValue());
  ASSERT_FALSE(att.extras().hasValue());
  ASSERT_EQ(shadow, att.shadow());
}

TEST(LogsConfigCodecTest, DISABLED_PerformanceTest1) {
  std::vector<flatbuffers::unique_ptr_t> buffers;
  buffers.reserve(135000);
  LogAttributes attrs =
      LogAttributes()
          .with_replicationFactor(22)
          .with_singleWriter(true)
          // inherited attributes are not serialized
          .with_nodeSetSize(Attribute<folly::Optional<int>>{22, true})
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION);
  auto start1 = std::chrono::high_resolution_clock::now();
  for (int i = 0; i < 135000; i++) {
    flatbuffers::FlatBufferBuilder builder;
    flatbuffers::Offset<fbuffers::LogAttrs> buf =
        FBuffersLogsConfigCodec::fbuffers_serialize<const LogAttributes&,
                                                    fbuffers::LogAttrs>(
            builder, attrs, false);
    builder.Finish(buf);
    buffers.push_back(builder.ReleaseBufferPointer());
  }
  auto end1 = std::chrono::high_resolution_clock::now();
  std::cout << "Serializing 135k Log Attributes took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   (end1 - start1))
                   .count()
            << std::endl;
  auto start2 = std::chrono::high_resolution_clock::now();
  for (const auto& ptr : buffers) {
    flatbuffers::FlatBufferBuilder builder;
    auto recovered = flatbuffers::GetRoot<fbuffers::LogAttrs>(ptr.get());
    LogAttributes att =
        FBuffersLogsConfigCodec::fbuffers_deserialize<LogAttributes>(recovered);
  }
  auto end2 = std::chrono::high_resolution_clock::now();
  std::cout << "Deserializing 135k Log Attributes took "
            << std::chrono::duration_cast<std::chrono::milliseconds>(
                   (end2 - start2))
                   .count()
            << std::endl;
}

TEST(LogsConfigCodecTest, LogsConfigTreeCodecTest) {
  std::string delimiter = "/";

  auto defaults =
      DefaultLogAttributes()
          .with_replicationFactor(4)
          .with_singleWriter(true)
          .with_nodeSetSize(Attribute<folly::Optional<int>>{26, true})
          .with_scdEnabled(false)
          .with_extraCopies(0)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION);

  auto tree = LogsConfigTree::create(delimiter, defaults);
  auto child1 = tree->addDirectory(
      "/dir1",
      false,
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION));
  ASSERT_EQ(defaults.replicationFactor().value(),
            child1->attrs().replicationFactor().value());

  LogAttributes::ExtrasMap extras{
      {"My Key1", "My Value1"}, {"My Key2", "My Value2"}};
  std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
  permission_array.fill(false);
  permission_array[static_cast<int>(ACTION::READ)] = true;
  permission_array[static_cast<int>(ACTION::TRIM)] = true;
  auto pem =
      LogAttributes::PermissionsMap{std::make_pair("ADMINS", permission_array)};

  LogAttributes::ACLList acl_list;
  acl_list.push_back("acl_test1");
  acl_list.push_back("acl_test2");

  LogAttributes::ACLList acl_shadow_list;
  acl_shadow_list.push_back("acl_shadow_test1");
  acl_shadow_list.push_back("acl_shadow_test2");

  LogAttributes::Shadow shadow{"logdevice.test", 0.1};

  auto child2_attrs = LogAttributes()
                          .with_replicationFactor(24)
                          .with_scdEnabled(true)
                          .with_permissions(pem)
                          .with_acls(acl_list)
                          .with_aclsShadow(acl_shadow_list)
                          .with_extras(extras)
                          .with_shadow(shadow);
  auto child2 = tree->addDirectory("/dir2", false, child2_attrs);

  tree->addDirectory(
      "/dir2/dir2_2", false, LogAttributes().with_scdEnabled(true));

  tree->addLogGroup(child2, "log1", logid_range_t{logid_t(15), logid_t(66)});
  ASSERT_TRUE(tree->addLogGroup(
      child1,
      "log1",
      logid_range_t{logid_t(67), logid_t(75)},
      LogAttributes().with_replicationFactor(6).with_extraCopies(0)));

  flatbuffers::FlatBufferBuilder builder;
  auto buffer =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogsConfigTree&,
                                                  fbuffers::LogsConfig>(
          builder, *tree, false);

  builder.Finish(buffer);
  auto recovered =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogsConfigTree>(
          flatbuffers::GetRoot<fbuffers::LogsConfig>(
              builder.GetBufferPointer()),
          "/");

  ASSERT_EQ(tree->version(), recovered->version());
  ASSERT_EQ("/", recovered->delimiter());
  auto d1 = recovered->findDirectory("/dir1");
  ASSERT_TRUE(d1);
  ASSERT_EQ("dir1", d1->name());
  ASSERT_EQ("/dir1/", d1->getFullyQualifiedName());
  ASSERT_TRUE(d1->attrs().replicationFactor());
  ASSERT_EQ(defaults.replicationFactor().value(),
            d1->attrs().replicationFactor().value());
  ASSERT_TRUE(d1->attrs().replicationFactor().isInherited());
  auto d2 = recovered->findDirectory("/dir2");
  ASSERT_TRUE(d2);
  ASSERT_EQ("dir2", d2->name());
  ASSERT_EQ("/dir2/", d2->getFullyQualifiedName());
  ASSERT_EQ(24, d2->attrs().replicationFactor().value());
  ASSERT_EQ(extras, d2->attrs().extras().value());
  ASSERT_EQ(pem, d2->attrs().permissions().value());
  ASSERT_EQ(acl_list.size(), d2->attrs().acls().value().size());
  ASSERT_EQ(acl_list, d2->attrs().acls().value());
  ASSERT_FALSE(d2->attrs().shadow().isInherited());
  ASSERT_EQ(shadow, d2->attrs().shadow().value());

  auto d2_2 = recovered->findDirectory("/dir2/dir2_2/");
  ASSERT_TRUE(d2_2);
  ASSERT_EQ("dir2_2", d2_2->name());
  ASSERT_EQ("/dir2/dir2_2/", d2_2->getFullyQualifiedName());
  ASSERT_EQ(24, d2_2->attrs().replicationFactor().value());
  ASSERT_TRUE(d2_2->attrs().replicationFactor().isInherited());
  ASSERT_TRUE(d2_2->attrs().permissions().isInherited());
  ASSERT_EQ(pem, d2_2->attrs().permissions().value());
  ASSERT_EQ(acl_list.size(), d2_2->attrs().acls().value().size());
  ASSERT_EQ(acl_list, d2_2->attrs().acls().value());
  ASSERT_TRUE(d2_2->attrs().extras().isInherited());
  ASSERT_EQ(extras, d2_2->attrs().extras().value());
  ASSERT_TRUE(d2_2->attrs().shadow().isInherited());
  ASSERT_EQ(shadow, d2_2->attrs().shadow().value());

  ASSERT_EQ(nullptr, tree->findLogGroup("/dir1/mylog"));
  auto lg1 = tree->findLogGroup("/dir1/log1");
  ASSERT_TRUE(lg1);
  ASSERT_EQ("log1", lg1->name());
  auto range = logid_range_t{logid_t(67l), logid_t(75l)};
  ASSERT_EQ(range, lg1->range());
  ASSERT_EQ(6, lg1->attrs().replicationFactor().value());
  ASSERT_FALSE(lg1->attrs().scdEnabled().value());
  ASSERT_TRUE(lg1->attrs().scdEnabled().isInherited());
  auto lg1_restored = tree->getLogGroupByID(logid_t{55});
  ASSERT_TRUE(lg1_restored != nullptr);
  ASSERT_EQ("/dir2/log1", lg1_restored->getFullyQualifiedName());
  auto lg2_restored = tree->getLogGroupByID(logid_t{67});
  ASSERT_TRUE(lg2_restored != nullptr);
  ASSERT_EQ("/dir1/log1", lg2_restored->getFullyQualifiedName());
  // corresponding metadata log should be not available in the tree.
  auto lg3_restored =
      tree->getLogGroupByID(MetaDataLog::metaDataLogID(logid_t{67}));
  ASSERT_TRUE(lg3_restored == nullptr);
}

TEST(LogsConfigCodecTest, LogsConfigTreeCodecTest2) {
  std::string delimiter = "/";

  auto defaults =
      DefaultLogAttributes()
          .with_replicationFactor(4)
          .with_singleWriter(true)
          .with_nodeSetSize(Attribute<folly::Optional<int>>{26, true})
          .with_scdEnabled(false)
          .with_extraCopies(0)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION);

  auto tree = LogsConfigTree::create(delimiter, defaults);
  auto child1 = tree->addDirectory(
      "/dir1",
      false,
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION));
  auto child2 = tree->addDirectory(
      "/dir2",
      false,
      LogAttributes().with_replicationFactor(24).with_scdEnabled(true));

  tree->addDirectory(
      "/dir2/dir2_2", false, LogAttributes().with_scdEnabled(true));

  tree->addLogGroup(child2, "log1", logid_range_t{logid_t(15), logid_t(66)});
  ASSERT_TRUE(tree->addLogGroup(
      child1,
      "log1",
      logid_range_t{logid_t(67), logid_t(75)},
      LogAttributes().with_replicationFactor(6).with_extraCopies(0)));

  PayloadHolder payload =
      FBuffersLogsConfigCodec::serialize<LogsConfigTree>(*tree, false);
  ASSERT_TRUE(payload.valid());

  std::unique_ptr<LogsConfigTree> recovered =
      FBuffersLogsConfigCodec::deserialize<LogsConfigTree>(
          payload.getPayload(), "/");
  ASSERT_TRUE(recovered);
  ASSERT_EQ(tree->version(), recovered->version());
  ASSERT_EQ("/", recovered->delimiter());
  auto d1 = recovered->findDirectory("/dir1");
  ASSERT_TRUE(d1);
  ASSERT_EQ("dir1", d1->name());
  ASSERT_EQ("/dir1/", d1->getFullyQualifiedName());
  ASSERT_TRUE(d1->attrs().replicationFactor());
  ASSERT_EQ(defaults.replicationFactor().value(),
            d1->attrs().replicationFactor().value());
  ASSERT_EQ(true, d1->attrs().replicationFactor().isInherited());
  auto d2 = recovered->findDirectory("/dir2");
  ASSERT_TRUE(d2);
  ASSERT_EQ("dir2", d2->name());
  ASSERT_EQ("/dir2/", d2->getFullyQualifiedName());
  ASSERT_EQ(24, d2->attrs().replicationFactor().value());
  auto d2_2 = recovered->findDirectory("/dir2/dir2_2/");
  ASSERT_TRUE(d2_2);
  ASSERT_EQ("dir2_2", d2_2->name());
  ASSERT_EQ("/dir2/dir2_2/", d2_2->getFullyQualifiedName());
  ASSERT_EQ(24, d2_2->attrs().replicationFactor().value());
  ASSERT_TRUE(d2_2->attrs().replicationFactor().isInherited());

  ASSERT_EQ(nullptr, tree->findLogGroup("/dir1/mylog"));
  auto lg1 = tree->findLogGroup("/dir1/log1");
  ASSERT_TRUE(lg1);
  ASSERT_EQ("log1", lg1->name());
  auto range = logid_range_t{logid_t(67l), logid_t(75l)};
  ASSERT_EQ(range, lg1->range());
  ASSERT_EQ(6, lg1->attrs().replicationFactor().value());
  ASSERT_FALSE(lg1->attrs().scdEnabled().value());
  ASSERT_TRUE(lg1->attrs().scdEnabled().isInherited());
  auto lg1_restored = tree->getLogGroupByID(logid_t{55});
  ASSERT_TRUE(lg1_restored != nullptr);
  ASSERT_EQ("/dir2/log1", lg1_restored->getFullyQualifiedName());
  auto lg2_restored = tree->getLogGroupByID(logid_t{67});
  ASSERT_TRUE(lg2_restored != nullptr);
  ASSERT_EQ("/dir1/log1", lg2_restored->getFullyQualifiedName());
  // corresponding metadata log should be not available in the tree.
  auto lg3_restored =
      tree->getLogGroupByID(MetaDataLog::metaDataLogID(logid_t{67}));
  ASSERT_TRUE(lg3_restored == nullptr);
}

TEST(LogsConfigCodecTest, LogsConfigTreeCodecTest3) {
  std::string delimiter = "/";

  auto defaults =
      DefaultLogAttributes()
          .with_replicationFactor(2)
          .with_nodeSetSize(Attribute<folly::Optional<int>>{3, true})
          .with_scdEnabled(false)
          .with_extraCopies(0)
          .with_backlogDuration(
              std::chrono::seconds(7 * 24 * 60 * 60)) // 7 days
          .with_syncReplicationScope(NodeLocationScope::REGION);

  auto tree = LogsConfigTree::create(delimiter, defaults);
  ASSERT_NE(nullptr,
            tree->addLogGroup("/parent1/parent2/0",
                              logid_range_t{logid_t(15), logid_t(66)},
                              LogAttributes(),
                              true));
  auto lg1 = tree->findLogGroup("/parent1/parent2/0");
  ASSERT_NE(nullptr, lg1);
  ASSERT_TRUE(lg1->attrs().replicationFactor().hasValue());
  ASSERT_EQ(2, lg1->attrs().replicationFactor().value());
  ASSERT_TRUE(lg1->attrs().scdEnabled().hasValue());
  ASSERT_FALSE(lg1->attrs().scdEnabled().value());

  flatbuffers::FlatBufferBuilder builder;
  auto buffer =
      FBuffersLogsConfigCodec::fbuffers_serialize<const LogsConfigTree&,
                                                  fbuffers::LogsConfig>(
          builder, *tree, false);

  builder.Finish(buffer);
  auto recovered =
      FBuffersLogsConfigCodec::fbuffers_deserialize<LogsConfigTree>(
          flatbuffers::GetRoot<fbuffers::LogsConfig>(
              builder.GetBufferPointer()),
          "/");

  ASSERT_TRUE(recovered);
  ASSERT_EQ(tree->version(), recovered->version());
  ASSERT_EQ("/", recovered->delimiter());
  auto dir2 = recovered->findDirectory("/parent1");
  ASSERT_NE(nullptr, dir2);
  ASSERT_TRUE(dir2->attrs().replicationFactor().hasValue());
  ASSERT_EQ(2, dir2->attrs().replicationFactor().value());
  ASSERT_TRUE(dir2->attrs().scdEnabled().hasValue());
  ASSERT_FALSE(dir2->attrs().scdEnabled().value());
  auto dir3 = recovered->findDirectory("/parent1/parent2");
  ASSERT_NE(nullptr, dir3);
  ASSERT_TRUE(dir3->attrs().replicationFactor().hasValue());
  ASSERT_EQ(2, dir3->attrs().replicationFactor().value());
  ASSERT_TRUE(dir3->attrs().scdEnabled().hasValue());
  ASSERT_FALSE(dir3->attrs().scdEnabled().value());
  auto lg2 = recovered->findLogGroup("/parent1/parent2/0");
  ASSERT_NE(nullptr, lg2);
  ASSERT_TRUE(lg2->attrs().replicationFactor().hasValue());
  ASSERT_EQ(2, lg2->attrs().replicationFactor().value());
  ASSERT_TRUE(lg2->attrs().scdEnabled().hasValue());
  ASSERT_FALSE(lg2->attrs().scdEnabled().value());
}

// This is disabled as it's covered by an assertion in the code
TEST(LogsConfigCodecTest, DISABLED_InvalidPayload) {
  std::string invalid_payload = "BAD DATA IN PAYLOAD";
  Payload payload(invalid_payload.data(), invalid_payload.size());
  auto tree =
      FBuffersLogsConfigCodec::deserialize<LogsConfigTree>(payload, "/");
  ASSERT_EQ(nullptr, tree);
  ASSERT_EQ(E::BADPAYLOAD, err);
}

TEST(LogsConfigCodecTest, SerializeDirectory) {
  std::string delimiter = "/";

  auto defaults =
      DefaultLogAttributes()
          .with_replicationFactor(4)
          .with_singleWriter(true)
          .with_nodeSetSize(Attribute<folly::Optional<int>>{26, true})
          .with_scdEnabled(false)
          .with_extraCopies(0)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION);

  auto tree = LogsConfigTree::create(delimiter, defaults);
  auto child1 = tree->addDirectory(
      "/dir1",
      false,
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION));
  auto child2 = tree->addDirectory(
      "/dir2",
      false,
      LogAttributes().with_replicationFactor(24).with_scdEnabled(true));

  tree->addDirectory(
      "/dir2/dir2_2", false, LogAttributes().with_scdEnabled(true));

  tree->addLogGroup(child2, "log1", logid_range_t{logid_t(15), logid_t(66)});
  ASSERT_TRUE(tree->addLogGroup(
      child1,
      "log1",
      logid_range_t{logid_t(67), logid_t(75)},
      LogAttributes().with_replicationFactor(6).with_extraCopies(0)));

  PayloadHolder payload = FBuffersLogsConfigCodec::serialize(*child2, false);
  ASSERT_TRUE(payload.valid());

  std::unique_ptr<DirectoryNode> recovered =
      FBuffersLogsConfigCodec::deserialize<DirectoryNode>(
          payload.getPayload(), "/");
  ASSERT_TRUE(recovered);
  ASSERT_EQ("dir2", recovered->name());
  ASSERT_EQ(24, recovered->attrs().replicationFactor().value());
  ASSERT_TRUE(recovered->attrs().scdEnabled().value());
  ASSERT_FALSE(recovered->attrs().replicationFactor().isInherited());

  ASSERT_EQ(1, recovered->logs().size());
  ASSERT_EQ("log1", recovered->logs().at("log1")->name());
}

TEST(LogsConfigCodecTest, SerializeLogGroup) {
  std::unique_ptr<LogGroupNode> lgn = std::make_unique<LogGroupNode>(
      "mylog",
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION),
      logid_range_t(logid_t(1), logid_t(100)));
  PayloadHolder payload = FBuffersLogsConfigCodec::serialize(*lgn, false);
  ASSERT_TRUE(payload.valid());

  std::unique_ptr<LogGroupNode> recovered =
      FBuffersLogsConfigCodec::deserialize<LogGroupNode>(
          payload.getPayload(), "/");

  ASSERT_TRUE(recovered);
  ASSERT_EQ("mylog", recovered->name());
  ASSERT_FALSE(recovered->attrs().replicationFactor().hasValue());
  ASSERT_FALSE(recovered->attrs().scdEnabled().value());
  ASSERT_EQ(NodeLocationScope::REGION,
            recovered->attrs().syncReplicationScope().value());
  ASSERT_EQ(logid_range_t(logid_t(1), logid_t(100)), recovered->range());
}

TEST(LogsConfigCodecTest, SerializeLogAttributesNormal) {
  LogAttributes attrs =
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION);
  PayloadHolder payload = FBuffersLogsConfigCodec::serialize(attrs, false);
  ASSERT_TRUE(payload.valid());

  std::unique_ptr<LogAttributes> recovered =
      FBuffersLogsConfigCodec::deserialize<LogAttributes>(
          payload.getPayload(), "/");

  ASSERT_TRUE(recovered);
  ASSERT_FALSE(recovered->replicationFactor().hasValue());
  ASSERT_FALSE(recovered->scdEnabled().value());
  ASSERT_EQ(std::chrono::seconds(15), recovered->backlogDuration().value());
  ASSERT_EQ(
      NodeLocationScope::REGION, recovered->syncReplicationScope().value());
}

TEST(LogsConfigCodecTest, SerializeLogAttributesFlattened) {
  std::array<bool, static_cast<int>(ACTION::MAX)> permission_array;
  permission_array.fill(false);
  permission_array[static_cast<int>(ACTION::READ)] = true;
  permission_array[static_cast<int>(ACTION::TRIM)] = true;

  LogAttributes::ACLList acl_list;
  acl_list.push_back("acl_test1");
  acl_list.push_back("acl_test2");

  LogAttributes::ACLList acl_shadow_list;
  acl_shadow_list.push_back("acl_shadow_test1");
  acl_shadow_list.push_back("acl_shadow_test2");

  auto pem =
      LogAttributes::PermissionsMap{std::make_pair("ADMINS", permission_array)};

  LogAttributes::Shadow shadow{"logdevice.test", 0.1};

  LogAttributes attrs =
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION)
          .with_permissions(pem)
          .with_acls(acl_list)
          .with_aclsShadow(acl_shadow_list)
          .with_shadow(shadow);

  // We are testing inherited attributes deserialization, most of the
  // attributes are set in the parent (attrs)
  LogAttributes child_attr = LogAttributes().with_scdEnabled(true);
  LogAttributes combined{child_attr, attrs};

  PayloadHolder payload = FBuffersLogsConfigCodec::serialize(combined, true);
  ASSERT_TRUE(payload.valid());

  std::unique_ptr<LogAttributes> recovered =
      FBuffersLogsConfigCodec::deserialize<LogAttributes>(
          payload.getPayload(), "/");

  ASSERT_TRUE(recovered);
  ASSERT_FALSE(recovered->replicationFactor().hasValue());
  ASSERT_TRUE(recovered->scdEnabled().value());
  ASSERT_FALSE(recovered->scdEnabled().isInherited());
  ASSERT_EQ(std::chrono::seconds(15), recovered->backlogDuration().value());
  ASSERT_TRUE(recovered->backlogDuration().isInherited());
  ASSERT_EQ(
      NodeLocationScope::REGION, recovered->syncReplicationScope().value());
  ASSERT_TRUE(recovered->syncReplicationScope().isInherited());
  ASSERT_EQ(pem, recovered->permissions().value());
  ASSERT_EQ(acl_list, recovered->acls().value());
  ASSERT_EQ(acl_shadow_list, recovered->aclsShadow().value());
  ASSERT_EQ(shadow, recovered->shadow().value());

  // Now let's see if we have permissions set (not inherited) are we going to
  // receive it correctly after deserialization or not.
  permission_array[static_cast<int>(ACTION::READ)] = false;
  pem =
      LogAttributes::PermissionsMap{std::make_pair("ADMINS", permission_array)};

  combined = combined.with_permissions(pem);

  LogAttributes::ACLList acl_list_comb;
  acl_list_comb.push_back("acl_test_comb1");

  LogAttributes::ACLList acl_shadow_list_comb;

  combined = combined.with_acls(acl_list_comb);
  combined = combined.with_aclsShadow(LogAttributes::ACLList());

  payload = FBuffersLogsConfigCodec::serialize(combined, true);
  ASSERT_TRUE(payload.valid());

  recovered = FBuffersLogsConfigCodec::deserialize<LogAttributes>(
      payload.getPayload(), "/");
  ASSERT_EQ(pem, recovered->permissions().value());
  ASSERT_EQ(acl_list_comb, recovered->acls().value());
  ASSERT_FALSE(recovered->aclsShadow().hasValue());
}

TEST(LogsConfigPayloadCodec, MkDirectoryDeltaTest) {
  LogAttributes attrs =
      LogAttributes()
          .with_scdEnabled(false)
          .with_backlogDuration(std::chrono::seconds(15))
          .with_syncReplicationScope(NodeLocationScope::REGION);

  std::string path{"/mylogs/super_logs"};
  DeltaHeader header{config_version_t(220), ConflictResolutionMode::AUTO};
  MkDirectoryDelta delta{header, path, true, attrs};

  PayloadHolder payload = FBuffersLogsConfigCodec::serialize(delta, false);
  ASSERT_TRUE(payload.valid());

  std::unique_ptr<Delta> recovered =
      FBuffersLogsConfigCodec::deserialize<Delta>(payload.getPayload(), "/");

  ASSERT_EQ(
      header.base_version().val(), recovered->header().base_version().val());
  ASSERT_EQ(header.resolution_mode(), recovered->header().resolution_mode());

  ASSERT_EQ(DeltaOpType::MK_DIRECTORY, delta.type());
  // after verifying the type, we can safely cast down to the correct delta type
  MkDirectoryDelta* mk_dir_delta =
      static_cast<MkDirectoryDelta*>(recovered.get());
  ASSERT_EQ(path, mk_dir_delta->path);
  ASSERT_TRUE(mk_dir_delta->should_make_intermediates);
  ASSERT_EQ(attrs, mk_dir_delta->attrs);
}
