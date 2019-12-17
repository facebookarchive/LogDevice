/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include <folly/FileUtil.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "logdevice/test/utils/IntegrationTestBase.h"
#include "logdevice/test/utils/IntegrationTestUtils.h"

using namespace facebook::logdevice;
using namespace facebook::logdevice::IntegrationTestUtils;

DEFINE_bool(update,
            false,
            "Overwrite the documentation file if it's out of date.");

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
  TestParams params = GetParam();
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

  if (outputs.first == settings_file) {
    ld_info("%s is up to date", settings_path.c_str());
    return;
  }

  if (FLAGS_update) {
    ld_info("Overwriting %s", settings_path.c_str());
    bool ok = folly::writeFile(outputs.first, settings_path.c_str());
    if (!ok) {
      ld_error("Failed to write %s", settings_path.c_str());
    }
  } else {
    ld_error("%s doesn't match!", settings_path.c_str());
    ld_error("To update it, please either:");
    ld_error(" a. re-run this test with --update flag, or");
    ld_error(" b. run %s > %s",
             folly::join(" ", params.args).c_str(),
             settings_path.c_str());
    ADD_FAILURE() << "Documentation not in sync";

    // Comparing line by line to make the difference clearer
    std::vector<std::string> lines_generated;
    folly::split('\n', outputs.first, lines_generated);
    std::vector<std::string> lines_file;
    folly::split('\n', settings_file, lines_file);

    // Typically the mismatch touches only a few nearby lines (when one or a few
    // nearby settings were added/removed/updated). Let's print everything
    // between the longest matching prefix and the longest matching suffix.

    size_t matching_lines_at_start = 0;
    while (matching_lines_at_start <
               std::min(lines_generated.size(), lines_file.size()) &&
           lines_generated.at(matching_lines_at_start) ==
               lines_file.at(matching_lines_at_start)) {
      ++matching_lines_at_start;
    }

    size_t matching_lines_at_end = 0;
    while (matching_lines_at_end <
               std::min(lines_generated.size(), lines_file.size()) &&
           lines_generated.at(lines_generated.size() - 1 -
                              matching_lines_at_end) ==
               lines_file.at(lines_file.size() - 1 - matching_lines_at_end)) {
      ++matching_lines_at_end;
    }

    auto print_lines = [](const std::vector<std::string>& lines,
                          size_t omit_first,
                          size_t omit_last) {
      for (size_t i = 0; i < lines.size() - omit_first - omit_last; ++i) {
        if (i > 20) {
          ld_error("[... %lu more lines ...]",
                   lines.size() - omit_first - omit_last - i);
          break;
        }
        ld_error("%s", lines.at(omit_first + i).c_str());
      }
    };

    ld_error("Mismatch starting from line %lu.", matching_lines_at_start + 1);
    ld_error("Expected lines:");
    print_lines(
        lines_generated, matching_lines_at_start, matching_lines_at_end);
    ld_error("Found lines:");
    print_lines(lines_file, matching_lines_at_start, matching_lines_at_end);
  }
}

std::vector<TestParams> settings_test_params{
    {"settings.md", defaultLogdevicedPath(), {"--markdown-settings"}},
    {"ldquery.md", defaultMarkdownLDQueryPath(), {}}};

INSTANTIATE_TEST_CASE_P(ServerSettingsTest,
                        DocumentationInSyncTest,
                        ::testing::ValuesIn(settings_test_params));
