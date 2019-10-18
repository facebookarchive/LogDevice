/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/UpdateableSettings.h"

#include <boost/program_options.hpp>

namespace facebook { namespace logdevice {

void UpdateableSettingsBase::setDefaultValue(const char* name,
                                             const char* value,
                                             const char* docs_override) {
  namespace style = boost::program_options::command_line_style;

  std::string dash_name = std::string("--") + name;
  int argc = 3;
  const char* argv[3] = {"UpdateableSettingsBase", dash_name.c_str(), value};

  auto it = settings_.find(name);
  ld_check(it != settings_.end());
  SettingDescriptor& setting = it->second;
  ld_check(setting.default_value.empty());

  boost::program_options::options_description desc;
  desc.add(setting.boost_description);

  boost::program_options::command_line_parser parser(argc, argv);
  auto parsed_options = parser.options(desc)
                            .style(style::unix_style & ~style::allow_guessing)
                            .run();
  boost::program_options::variables_map parsed;
  boost::program_options::store(parsed_options, parsed);
  boost::program_options::notify(parsed);

  ld_check_eq(1, parsed_options.options.size());
  auto& opt = parsed_options.options[0];
  ld_check(!opt.unregistered);
  ld_check_eq(opt.string_key, name);

  setting.default_value.insert(
      setting.default_value.end(), opt.value.begin(), opt.value.end());

  if (docs_override) {
    setting.default_value_docs_override.assign(docs_override);
  }
}

std::string SettingFlag::toHelpString(flag_t flag) {
  std::string res;

  auto c = [&](flag_t t, std::string s) {
    if (flag & t) {
      if (!res.empty()) {
        res += "|";
      }
      res += s;
    }
  };

  // Not including SERVER or CLIENT because --help is already contextual
  // (e.g. for the server binary's --help, only settings with the SERVER flag
  // are printed)
  c(CLI_ONLY, "CLI_ONLY");
  c(REQUIRES_RESTART, "REQUIRES_RESTART");
  c(INTERNAL_ONLY, "INTERNAL_ONLY");
  c(EXPERIMENTAL, "EXPERIMENTAL");
  c(DEPRECATED, "DEPRECATED");

  return res;
}

std::string SettingFlag::toMarkdown(flag_t flag) {
  std::string res;

  auto c = [&](flag_t t, std::string s) {
    if (flag & t) {
      if (!res.empty()) {
        res += ", ";
      }
      res += s;
    }
  };

  c(CLI_ONLY, "CLI&nbsp;only");
  c(REQUIRES_RESTART, "requires&nbsp;restart");
  c(INTERNAL_ONLY, "internal");
  c(EXPERIMENTAL, "**experimental**");
  c(DEPRECATED, "**deprecated**");
  if (!(flag & SERVER)) {
    c(CLIENT, "client&nbsp;only");
  }
  if (!(flag & CLIENT)) {
    c(SERVER, "server&nbsp;only");
  }

  return res;
}

}} // namespace facebook::logdevice
