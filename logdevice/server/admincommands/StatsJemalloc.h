/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/config.h"
#include "logdevice/server/AdminCommand.h"

#ifdef LOGDEVICE_USING_JEMALLOC

// Although the above macro suggests that we are being compiled with jemalloc
// available as a dependency, the final binary may not be linked with it.
// Instead of including jemalloc.h directly, these weak symbol declarations
// allow us to check this at runtime (the function pointers will be null if
// jemalloc is not present).
extern "C" int mallctl(const char*, void*, size_t*, void*, size_t)
    __attribute__((__weak__));
extern "C" void malloc_stats_print(void (*)(void*, const char*),
                                   void*,
                                   const char*) __attribute__((__weak__));

namespace facebook { namespace logdevice { namespace commands {

inline void jemalloc_message(void* cbopaque, const char* p) {
  EvbufferTextOutput* out = (EvbufferTextOutput*)cbopaque;
  out->write(p);
}

inline void statsJemallocPrint(EvbufferTextOutput& out, const char* opt) {
  if (malloc_stats_print != nullptr) {
    malloc_stats_print(jemalloc_message, &out, opt);
  } else {
    out.write("FAILED\r\n");
  }
}

class StatsJemalloc : public AdminCommand {
 public:
  void run() override {
    /* Only display general stats:
     * 'm': omit merged arena statistics;
     * 'a': omit per arena statistics;
     * 'b': omit per size class statistics for bins;
     * 'l': omit per size class statistics for large objects. */
    statsJemallocPrint(out_, "mabl");
  }
};

class StatsJemallocFull : public AdminCommand {
 public:
  void run() override {
    statsJemallocPrint(out_, nullptr);
  }
};

class StatsJemallocProfActive : public AdminCommand {
 private:
  std::string active_;

 public:
  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "active", boost::program_options::value<std::string>(&active_));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("active", 1);
  }
  std::string getUsage() override {
    return "stats jemalloc prof.active [true|false]";
  }

  void run() override {
    if (active_.empty()) {
      bool val = false;
      size_t val_sz = sizeof(val);
      const int err = mallctl != nullptr
          ? mallctl("prof.active", &val, &val_sz, nullptr, 0)
          : -1;
      if (err != 0) {
        out_.printf("FAILED\r\n");
      } else {
        out_.printf(val ? "true\r\n" : "false\r\n");
      }
    } else if (active_ == "true" || active_ == "false") {
      bool val = active_ == "true";
      const int err = mallctl != nullptr
          ? mallctl("prof.active", nullptr, nullptr, &val, sizeof(bool))
          : -1;
      out_.printf(err == 0 ? "OK\r\n" : "FAILED\r\n");
    } else {
      out_.printf("USAGE %s\r\n", getUsage().c_str());
    }
  }
};

class StatsJemallocProfDump : public AdminCommand {
 public:
  void run() override {
    char filename[1024];
    snprintf(filename,
             sizeof(filename),
             "/tmp/logdevice.heap.%lu.%d",
             (unsigned long)time(nullptr),
             getpid());
    char* fp = filename;
    const int err = mallctl != nullptr
        ? mallctl("prof.dump", nullptr, nullptr, &fp, sizeof(const char*))
        : -1;
    if (err == 0) {
      out_.printf("DUMPED %s\r\n", filename);
    } else {
      out_.printf("FAILED\r\n");
    }
  }
};

}}} // namespace facebook::logdevice::commands

#endif // LOGDEVICE_USING_JEMALLOC
