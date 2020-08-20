/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/test/utils/util.h"

#include "logdevice/common/debug.h"

namespace facebook { namespace logdevice { namespace IntegrationTestUtils {

int dump_file_to_stderr(const char* path) {
  FILE* fp = std::fopen(path, "r");
  if (fp == nullptr) {
    ld_error("fopen(\"%s\") failed with errno %d (%s)",
             path,
             errno,
             strerror(errno));
    return -1;
  }
  fprintf(stderr, "=== begin %s\n", path);
  std::vector<char> buf(16 * 1024);
  while (std::fgets(buf.data(), buf.size(), fp) != nullptr) {
    fprintf(stderr, "    %s", buf.data());
  }
  fprintf(stderr, "=== end %s\n", path);
  std::fclose(fp);
  return 0;
}

}}} // namespace facebook::logdevice::IntegrationTestUtils
