/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/FileUtil.h>
#include <gtest/gtest.h>

#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

struct TestParams {
  std::string doc;
  std::string command;
  std::vector<std::string> args;
};

class DocumentationInSyncTest
    : public IntegrationTestBase,
      public ::testing::WithParamInterface<TestParams> {};

// Output of running markdown generator must match contents markdown document,
// ensuring published documentation kept in-sync with latest changes
// `logdeviced(_nofb) --markdown-settings` matches (public_tld/)docs/settings.md
// `markdown-ldquery` matches (public_tld/)docs/ldquery.md
// Regenerate the appropriate document if there is a mismatch.
TEST_P(DocumentationInSyncTest, RegenAndCompare) {
  auto params = GetParam();

  auto binary_path = findBinary(params.command);
  ASSERT_FALSE(binary_path.empty());
  params.args.insert(params.args.begin(), binary_path);
  folly::Subprocess ldrun(
      params.args, folly::Subprocess::Options().pipeStdout().pipeStderr());
  auto outputs = ldrun.communicate();
  auto status = ldrun.wait();
  if (!status.exited()) {
    ld_error("logdeviced did not exit properly: %s", status.str().c_str());
    ASSERT_TRUE(false);
  }
  auto settings_path = findFile("docs/" + params.doc);
  if (settings_path.empty()) {
    settings_path = findFile("logdevice/public_tld/docs/" + params.doc);
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

std::vector<TestParams> settings_test_params{
    {"settings.md", defaultLogdevicedPath(), {"--markdown-settings"}},
    {"ldquery.md", defaultMarkdownLDQueryPath(), {}}};

INSTANTIATE_TEST_CASE_P(ServerSettingsTest,
                        DocumentationInSyncTest,
                        ::testing::ValuesIn(settings_test_params));
