/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/FileEpochStore.h"
#include "logdevice/server/admincommands/AdminCommand.h"

namespace facebook { namespace logdevice { namespace commands {

class PauseOrUnpauseFileEpochStore : public AdminCommand {
 private:
  bool pause_;

 public:
  explicit PauseOrUnpauseFileEpochStore(bool pause) : pause_(pause) {}

  std::string getUsage() override {
    return "This command has a very narrow usage and is used in tests. "
           "pause_file_epoch_store makes sure that no file locks are held by "
           "this process until unpause_file_epoch_store is called. This is "
           "useful to avoid a deadlock when using SIGSTOP: you would "
           "pause_file_epoch_store, then send SIGSTOP, then send SIGCONT, then "
           "unpause_file_epoch_store. This ensures that a stopped logdeviced "
           "won't hold file locks preventing other logdeviced's from using "
           "epoch store.";
  }

  void run() override {
    auto& epoch_store =
        server_->getProcessor()->allSequencers().getEpochStore();
    FileEpochStore* file_epoch_store =
        dynamic_cast<FileEpochStore*>(&epoch_store);
    bool ok = true;
    if (file_epoch_store) {
      if (pause_) {
        ok = file_epoch_store->pause();
      } else {
        ok = file_epoch_store->unpause();
      }
    }
    if (ok) {
      out_.printf("OK");
    } else {
      out_.printf(
          "Error: the store was already %s", pause_ ? "paused" : "unpaused");
    }
  }
};

}}} // namespace facebook::logdevice::commands
