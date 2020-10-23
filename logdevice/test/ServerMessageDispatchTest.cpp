// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/server/ServerMessageDispatch.h"

#include <memory>

#include <gtest/gtest.h>

#include "logdevice/common/PrincipalIdentity.h"
#include "logdevice/common/Processor.h"
#include "logdevice/common/configuration/SecurityConfig.h"
#include "logdevice/common/protocol/Message.h"
#include "logdevice/common/protocol/MessageDispatch.h"
#include "logdevice/common/protocol/TEST_Message.h"
// #include "logdevice/common/settings/UpdateableSettings.h"
#include "logdevice/common/test/TestUtil.h"

using namespace facebook::logdevice;

TEST(ServerMessageDispatchTest, ServerMessageFromClientTest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  settings.require_permission_message_types.insert(MessageType::TEST);
  auto updateable_settings = UpdateableSettings<Settings>(settings);

  auto security_info = std::make_unique<UpdateableSecurityInfo>(
      UpdateableConfig::createEmpty()->updateableServerConfig(),
      make_test_plugin_registry());

  auto stats = nullptr;

  ServerMessageDispatch messageDispatcher{
      security_info.get(), updateable_settings, stats};

  auto msg = std::make_unique<TEST_Message>();
  PermissionParams params;
  params.requiresPermission = true;
  params.action = ACTION::SERVER_INTERNAL;
  msg->setPermissionParams(params);

  Address from;
  PrincipalIdentity principal{Principal::DEFAULT};

  Message::Disposition result =
      messageDispatcher.onReceivedImpl(msg.get(), from, principal);

  EXPECT_EQ(Message::Disposition::ERROR, result);
}

TEST(ServerMessageDispatchTest, WhitelistedServerMessageFromClientTest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  settings.require_permission_message_types = {};
  auto updateable_settings = UpdateableSettings<Settings>(settings);

  auto security_info = std::make_unique<UpdateableSecurityInfo>(
      UpdateableConfig::createEmpty()->updateableServerConfig(),
      make_test_plugin_registry());

  auto stats = nullptr;

  ServerMessageDispatch messageDispatcher{
      security_info.get(), updateable_settings, stats};

  auto msg = std::make_unique<TEST_Message>();
  PermissionParams params;
  params.requiresPermission = true;
  params.action = ACTION::SERVER_INTERNAL;
  msg->setPermissionParams(params);

  Address from;
  PrincipalIdentity principal{Principal::AUTHENTICATED};

  Message::Disposition result =
      messageDispatcher.onReceivedImpl(msg.get(), from, principal);

  EXPECT_EQ(Message::Disposition::NORMAL, result);
}

TEST(ServerMessageDispatchTest, UncheckedServerMessageFromClientTest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  settings.require_permission_message_types.insert(MessageType::TEST);
  auto updateable_settings = UpdateableSettings<Settings>(settings);

  auto security_info = std::make_unique<UpdateableSecurityInfo>(
      UpdateableConfig::createEmpty()->updateableServerConfig(),
      make_test_plugin_registry());

  auto stats = nullptr;

  ServerMessageDispatch messageDispatcher{
      security_info.get(), updateable_settings, stats};

  auto msg = std::make_unique<TEST_Message>();
  PermissionParams params;
  params.requiresPermission = false;
  params.action = ACTION::SERVER_INTERNAL;
  msg->setPermissionParams(params);

  Address from;
  PrincipalIdentity principal{Principal::UNAUTHENTICATED};

  Message::Disposition result =
      messageDispatcher.onReceivedImpl(msg.get(), from, principal);

  EXPECT_EQ(Message::Disposition::NORMAL, result);
}

TEST(ServerMessageDispatchTest, ClientMessageFromClientTest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto updateable_settings = UpdateableSettings<Settings>(settings);

  auto security_info = std::make_unique<UpdateableSecurityInfo>(
      UpdateableConfig::createEmpty()->updateableServerConfig(),
      make_test_plugin_registry());

  auto stats = nullptr;

  ServerMessageDispatch messageDispatcher{
      security_info.get(), updateable_settings, stats};

  auto msg = std::make_unique<TEST_Message>();
  PermissionParams params;
  params.requiresPermission = true;
  params.action = ACTION::READ;
  msg->setPermissionParams(params);

  Address from;
  PrincipalIdentity principal{Principal::INVALID};

  Message::Disposition result =
      messageDispatcher.onReceivedImpl(msg.get(), from, principal);

  EXPECT_EQ(Message::Disposition::NORMAL, result);
}

TEST(ServerMessageDispatchTest, ServerMessageFromServerTest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto updateable_settings = UpdateableSettings<Settings>(settings);

  auto security_info = std::make_unique<UpdateableSecurityInfo>(
      UpdateableConfig::createEmpty()->updateableServerConfig(),
      make_test_plugin_registry());

  auto stats = nullptr;

  ServerMessageDispatch messageDispatcher{
      security_info.get(), updateable_settings, stats};

  auto msg = std::make_unique<TEST_Message>();
  PermissionParams params;
  params.requiresPermission = true;
  params.action = ACTION::SERVER_INTERNAL;
  msg->setPermissionParams(params);

  Address from;
  PrincipalIdentity principal{Principal::CLUSTER_NODE};

  Message::Disposition result =
      messageDispatcher.onReceivedImpl(msg.get(), from, principal);

  EXPECT_EQ(Message::Disposition::NORMAL, result);
}

TEST(ServerMessageDispatchTest, ClientMessageFromServerTest) {
  Settings settings = create_default_settings<Settings>();
  settings.num_workers = 1;
  auto updateable_settings = UpdateableSettings<Settings>(settings);

  auto security_info = std::make_unique<UpdateableSecurityInfo>(
      UpdateableConfig::createEmpty()->updateableServerConfig(),
      make_test_plugin_registry());

  auto stats = nullptr;

  ServerMessageDispatch messageDispatcher{
      security_info.get(), updateable_settings, stats};

  auto msg = std::make_unique<TEST_Message>();
  PermissionParams params;
  params.requiresPermission = true;
  params.action = ACTION::TRIM;
  msg->setPermissionParams(params);

  Address from;
  PrincipalIdentity principal{Principal::CLUSTER_NODE};

  Message::Disposition result =
      messageDispatcher.onReceivedImpl(msg.get(), from, principal);

  EXPECT_EQ(Message::Disposition::NORMAL, result);
}
