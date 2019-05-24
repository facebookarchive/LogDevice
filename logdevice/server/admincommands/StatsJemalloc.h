/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include "logdevice/common/config.h"
#include "logdevice/server/admincommands/AdminCommand.h"

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

struct MallctlDataType {
  // Human readable string -> mallctl format.
  // If invalid format, returns false.
  using parse_func_t = bool (*)(const std::string& val, char* out_buf);
  // mallctl format -> human readable string.
  using format_func_t = std::string (*)(Slice buf);

  size_t size;
  parse_func_t parse;
  format_func_t format;

  MallctlDataType(size_t size, parse_func_t parse, format_func_t format)
      : size(size), parse(parse), format(format) {}
};

class JemallocGetSet : public AdminCommand {
 private:
  const bool set_;
  std::string name_;
  folly::Optional<std::string> value_;
  folly::Optional<std::string> type_;

  static std::map<std::string, MallctlDataType>& dataTypes() {
    static std::map<std::string, MallctlDataType> types{
        {"bool",
         {1,
          +[](const std::string& val, char* out_buf) {
            if (lowerCase(val) == "true" || val == "1") {
              out_buf[0] = 1;
            } else if (lowerCase(val) == "false" || val == "0") {
              out_buf[0] = 0;
            } else {
              return false;
            }
            return true;
          },
          +[](Slice buf) -> std::string {
            ld_check_eq(buf.size, 1);
            return buf.ptr()[0] ? "true" : "false";
          }}},
        {"int64",
         {8,
          +[](const std::string& val, char* out_buf) {
            int64_t x;
            try {
              x = folly::to<int64_t>(val);
            } catch (std::range_error&) {
              return false;
            }
            memcpy(out_buf, &x, sizeof(x));
            return true;
          },
          +[](Slice buf) -> std::string {
            ld_check_eq(buf.size, 8);
            int64_t x;
            memcpy(&x, buf.data, sizeof(x));
            return std::to_string(x);
          }}},
        {"int32",
         {4,
          +[](const std::string& val, char* out_buf) {
            int32_t x;
            try {
              x = folly::to<int32_t>(val);
            } catch (std::range_error&) {
              return false;
            }
            memcpy(out_buf, &x, sizeof(x));
            return true;
          },
          +[](Slice buf) -> std::string {
            ld_check_eq(buf.size, 4);
            int32_t x;
            memcpy(&x, buf.data, sizeof(x));
            return std::to_string(x);
          }}},
        {"str",
         {8,
          +[](const std::string& val, char* out_buf) {
            // This is special-cased.
            ld_check(false);
            return false;
          },
          +[](Slice buf) -> std::string {
            ld_check_eq(buf.size, 8);
            const char* p;
            memcpy(&p, buf.data, sizeof(p));
            return std::string(p);
          }}},
        {"void",
         {0,
          +[](const std::string& val, char* out_buf) {
            // This is special-cased.
            ld_check(false);
            return false;
          },
          +[](Slice buf) -> std::string { return ""; }}},
    };
    return types;
  }

  static std::string dataTypeNames() {
    auto& types = dataTypes();
    std::string s;
    for (const auto& t : types) {
      if (!s.empty()) {
        s += "|";
      }
      s += t.first;
    }
    return s;
  }

  static std::map<std::string, std::string>& knownNameToType() {
    static std::map<std::string, std::string> map{
        {"prof.active", "bool"},
        {"prof.dump", "str"},
        {"prof.gdump", "bool"},
        {"prof.lg_sample", "int64"},
        {"prof.interval", "int64"},
    };
    return map;
  }

 public:
  explicit JemallocGetSet(bool set) : set_(set) {}

  void getOptions(
      boost::program_options::options_description& out_options) override {
    out_options.add_options()(
        "name", boost::program_options::value<std::string>(&name_)->required());
    if (set_) {
      out_options.add_options()(
          "value",
          boost::program_options::value<std::string>()->notifier(
              [this](const std::string& val) { value_ = val; }));
    }
    out_options.add_options()(
        "type",
        boost::program_options::value<std::string>()->notifier(
            [this](const std::string& val) { type_ = val; }));
  }
  void getPositionalOptions(
      boost::program_options::positional_options_description& out_options)
      override {
    out_options.add("name", 1);
    if (set_) {
      out_options.add("value", 1);
    }
    out_options.add("type", 1);
  }
  std::string getUsage() override {
    std::string s;
    if (set_) {
      s = "jemalloc set <name> [<value>] [--type=" + dataTypeNames() + "]";
    } else {
      s = "jemalloc get <name> [" + dataTypeNames() + "]";
    }

    s += "\r\n\r\nGet/set jemalloc options using mallctl(). See "
         "http://jemalloc.net/jemalloc.3.html for the list of available "
         "options.";
    return s;
  }

  void run() override {
    if (mallctl == nullptr) {
      out_.printf("ERROR: not linked with jemalloc\r\n");
      return;
    }

    if (type_.hasValue() && !dataTypes().count(type_.value())) {
      out_.printf("ERROR: unknown type: %s; expected: %s\r\n",
                  type_.value().c_str(),
                  dataTypeNames().c_str());
      return;
    }

    if (!type_.hasValue()) {
      if (knownNameToType().count(name_)) {
        type_ = knownNameToType().at(name_);
      } else {
        out_.printf("ERROR: need to specify data type\r\n");
        return;
      }
    }

    if (type_.value() == "void") {
      if (!set_) {
        out_.printf("ERROR: get with void type is invalid, use set\r\n");
        return;
      }
      if (value_.hasValue()) {
        out_.printf("ERROR: no value required for void type\r\n");
        return;
      }
    } else {
      if (set_ && !value_.hasValue()) {
        out_.printf("ERROR: value required\r\n");
        return;
      }
    }

    std::array<char, 8> buf;

    if (set_) {
      char* ptr = &buf[0];
      size_t size = dataTypes().at(type_.value()).size;
      ld_check_le(size, buf.size());
      if (type_ == "str") {
        // For string, point directly into value_.
        ptr = value_->empty() ? nullptr : &value_.value()[0];
      } else if (!value_.hasValue()) {
        // Need to pass nullptr for void type.
        ptr = nullptr;
      } else if (!dataTypes().at(type_.value()).parse(value_.value(), ptr)) {
        out_.printf("ERROR: invalid value %s of type %s\r\n",
                    value_->c_str(),
                    type_->c_str());
        return;
      }

      int rv = mallctl(name_.c_str(), nullptr, nullptr, ptr, size);
      if (rv == 0) {
        out_.printf("OK\r\n");
      } else {
        out_.printf("FAILED: %s\r\n", strerror(rv));
      }
    } else {
      ld_check(!value_.hasValue());
      size_t size = dataTypes().at(type_.value()).size;
      ld_check_le(size, buf.size());
      int rv = mallctl(name_.c_str(), &buf[0], &size, nullptr, 0);
      if (rv == 0) {
        std::string s =
            dataTypes().at(type_.value()).format(Slice(&buf[0], size));
        out_.printf("%s\r\nOK\r\n", s.c_str());
      } else {
        out_.printf("FAILED: %s\r\n", strerror(rv));
      }
    }
  }
};

}}} // namespace facebook::logdevice::commands

#endif // LOGDEVICE_USING_JEMALLOC
