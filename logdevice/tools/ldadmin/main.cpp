// Copyright 2004-present Facebook. All Rights Reserved.

#include "logdevice/tools/ldadmin/LDAdmin.h"

using namespace facebook::logdevice;

int main(int argc, const char* argv[]) {
  LDAdmin admin;

  HostOptionPlugin host_option(
      "host", "Hostname of the admin server", "localhost:6440");
  UnixPathOptionPlugin unix_path_option(
      "unix-path", "Unix path of the admin server.", std::string{});
  auto result = admin.registerTransportOptionPlugin(host_option) &&
      admin.registerTransportOptionPlugin(unix_path_option) &&
      admin.parse_command_line(argc, argv) && admin.run();
  return result ? EXIT_SUCCESS : EXIT_FAILURE;
}
