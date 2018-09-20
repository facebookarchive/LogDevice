/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include "logdevice/server/LocalLogFile.h"

namespace facebook { namespace logdevice {

int LocalLogFile::open(const std::string& path) {
  return file_.open(path.c_str(),
                    O_APPEND | O_CREAT | O_WRONLY,
                    S_IRUSR | S_IWUSR |     // RW user
                        S_IRGRP | S_IWGRP | // RW group
                        S_IROTH             // R others
  );
}

void LocalLogFile::reopen() {
  file_.reopen();
}
}} // namespace facebook::logdevice
