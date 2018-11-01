/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/FileUtil.h>
#include <gtest/gtest.h>

//#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice::IntegrationTestUtils;

// Output of running `logdeviced(_nofb) --markdown-settings` must match contents
// of (public_tld/)docs/settings.md. Regenerate the settings.md file if there
// is a mismatch
TEST(ServerSettingsTest, SettingsMatch) {
  auto binary_path = findBinary(defaultLogdevicedPath());
  ASSERT_FALSE(binary_path.empty());
  folly::Subprocess ldrun(
      {binary_path, "--markdown-settings"},
      folly::Subprocess::Options().pipeStdout().pipeStderr());
  auto outputs = ldrun.communicate();
  auto status = ldrun.wait();
  if (!status.exited()) {
    ld_error("logdeviced did not exit properly: %s", status.str().c_str());
    ASSERT_TRUE(false);
  }
  auto settings_path = findFile("docs/settings.md");
  if (settings_path.empty()) {
    settings_path = findFile("logdevice/public_tld/docs/settings.md");
  }
  ASSERT_FALSE(settings_path.empty());
  std::string settings_file;
  bool rv = folly::readFile(settings_path.c_str(), settings_file);
  ASSERT_TRUE(rv);

  // Comparing line by line to make the difference clearer
  std::vector<std::string> lines_generated;
  folly::split('\n', outputs.first, lines_generated);
  std::vector<std::string> lines_file;
  folly::split('\n', settings_file, lines_file);

  std::string emptystr;
  for (int i = 0; i < lines_generated.size() || i < lines_file.size(); ++i) {
    const std::string& generated_line =
        i < lines_generated.size() ? lines_generated[i] : emptystr;
    const std::string& file_line =
        i < lines_file.size() ? lines_file[i] : emptystr;
    if (generated_line != file_line) {
      ld_error("Mismatch on line %d", i);
    }
    EXPECT_EQ(generated_line, file_line);
  }
  ASSERT_EQ(lines_generated, lines_file);
}
