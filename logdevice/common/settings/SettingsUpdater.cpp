/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/settings/SettingsUpdater.h"

#include <boost/program_options.hpp>
#include <folly/Memory.h>

#include "logdevice/common/commandline_util_chrono.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

bool fails_check(const SettingsUpdater::setting_checker_t& checker,
                 const SettingsUpdater::SettingState& setting) {
  return checker && checker(setting, "").hasValue();
}

#define ENSURES_FALSE(condition, description)                                \
  ((const setting_checker_t)(                                                \
      [&](const SettingState& setting,                                       \
          const std::string& setting_name) -> folly::Optional<std::string> { \
        if ((condition)) {                                                   \
          return folly::sformat(                                             \
              "'{}' cannot be set in this context because " description,     \
              setting_name);                                                 \
        }                                                                    \
        return folly::none;                                                  \
      }))

#define MAKE_CHECKER_FROM_SEQ(seq)                                       \
  [&](const SettingState& setting,                                       \
      const std::string& setting_name) -> folly::Optional<std::string> { \
    for (const auto& c : seq) {                                          \
      if (c) {                                                           \
        folly::Optional<std::string> e = c(setting, setting_name);       \
        if (e.hasValue()) {                                              \
          return e;                                                      \
        }                                                                \
      }                                                                  \
    }                                                                    \
    return folly::none;                                                  \
  }

std::string SettingsUpdater::help(SettingFlag::flag_t flag,
                                  folly::Optional<std::string> bundle,
                                  unsigned line_length,
                                  unsigned min_description_length) const {
  using namespace boost::program_options;

  // Create one options_description per bundle.
  std::map<std::string, std::unique_ptr<options_description>> bundles;
  for (auto& opt : settings_) {
    std::string name = opt.second.bundle_name;
    if (bundle.hasValue() && name != bundle.value()) {
      continue;
    }
    if (opt.second.descriptor.flags & SettingFlag::INTERNAL_ONLY) {
      continue;
    }
    if (flag && !(opt.second.descriptor.flags & flag)) {
      continue;
    }
    if (!bundles.count(name)) {
      bundles[name] = std::make_unique<options_description>(
          name, line_length, min_description_length);
    }
    bundles[name]->add(opt.second.descriptor.boost_description_validate_only);
  }

  std::ostringstream str;
  options_description flag_description(
      "Meaning of each flag", line_length, min_description_length);
  flag_description.add_options()(
      "REQUIRES_RESTART",
      "If the setting is passed via the config file, logdeviced (or the client)"
      " must be restarted in order for the change to take effect.")(
      "CLI_ONLY", "This setting cannot be set via the config file.");
  str << flag_description;
  for (auto& p : bundles) {
    str << std::endl << std::endl;
    str << *p.second;
  }

  return str.str();
}

// Group settings by category, for each category generate a Markdown table
std::string SettingsUpdater::markdownDoc(bool include_deprecated) const {
  std::ostringstream doc;

  // category name -> setting name -> setting descriptor
  std::map<std::string,
           std::map<std::string, UpdateableSettingsBase::SettingDescriptor>>
      cats;

  for (const auto& opt : settings_) {
    cats[opt.second.descriptor.category].emplace(
        opt.first, opt.second.descriptor);
  }

  using std::endl;

  // Markdown header
  doc << "---" << endl;
  doc << "id: Settings" << endl;
  doc << "title: Configuration settings" << endl;
  doc << "sidebar_label: Settings" << endl;
  doc << "---" << endl;
  doc << endl;

  for (const auto& cat : cats) {
    doc << "## " << cat.first << endl;
    doc << "|   Name    |   Description   |  Default  |   Notes   |" << endl;
    doc << "|-----------|-----------------|:---------:|-----------|" << endl;
    for (const auto& setting : cat.second) {
      const auto& desc = setting.second;
      const std::string& setting_name = setting.first;

      if (desc.flags & SettingFlag::INTERNAL_ONLY ||
          ((desc.flags & SettingFlag::DEPRECATED) && !include_deprecated)) {
        continue; // do not document internal or deprecated settings
      }
      std::string default_value =
          desc.default_value_docs_override.value_or(desc.default_value.at(0));
      doc << "| " << markdown_sanitize(setting_name) << " | "
          << markdown_sanitize(desc.description) << " | "
          << markdown_sanitize(default_value) << " | "
          << SettingFlag::toMarkdown(desc.flags) << " |" << endl;
    }
    doc << endl;
  }

  return doc.str();
}

/**
 * Get the value for a setting considering the precedence rules for all
 * sources.
 */
static std::vector<std::string>
computeValue(const SettingsUpdater::SettingState& state,
             folly::Optional<SettingsUpdater::Source>* out_source = nullptr) {
  const auto& sources = state.sources;

  for (int src = static_cast<int>(SettingsUpdater::Source::CURRENT) - 1;
       src >= 0;
       --src) {
    auto it_src = sources.find(src);
    if (it_src == sources.end()) {
      continue;
    }
    const auto& val = it_src->second;
    if (!val.empty()) {
      if (out_source) {
        *out_source = static_cast<SettingsUpdater::Source>(src);
      }
      return val;
    }
  }
  if (out_source) {
    *out_source = folly::none;
  }
  return state.descriptor.default_value;
}

// Print value as human-readable string.
static std::string formatValue(const std::vector<std::string>& v) {
  if (v.empty()) {
    return "[none]";
  } else if (v.size() == 1) {
    return v[0];
  } else {
    return "\"" + folly::join(" ", v) + "\"";
  }
}

// Print the effective value and its source.
static std::string
formatEffectiveValue(const SettingsUpdater::SettingState& state) {
  folly::Optional<SettingsUpdater::Source> source;
  auto v = computeValue(state, &source);
  return formatValue(v) + " (from " +
      (source.hasValue() ? SettingsUpdater::sourceNames()[source.value()]
                         : std::string("defaults")) +
      ")";
}

void SettingsUpdater::parse(int argc,
                            const char** argv,
                            Source source,
                            bool store,
                            setting_checker_t checker,
                            const std::vector<std::string>* remove,
                            std::vector<std::string>* unrecognized,
                            std::map<std::string, std::string>* not_accepted) {
  using namespace SettingFlag;
  namespace style = boost::program_options::command_line_style;

  boost::program_options::options_description desc;
  for (auto& opt : settings_) {
    if (store) {
      desc.add(opt.second.descriptor.boost_description);
    } else {
      desc.add(opt.second.descriptor.boost_description_validate_only);
    }
  }

  auto parser = [&]() {
    using clp = boost::program_options::command_line_parser;
    return (argc > 0) ? clp(argc, argv) : clp({});
  }();

  parser.options(desc);
  parser.style(style::unix_style & ~style::allow_guessing);
  if (unrecognized) {
    parser.allow_unregistered();
  }

  auto parsed_options = parser.run();

  boost::program_options::variables_map parsed;
  boost::program_options::store(parsed_options, parsed);

  decltype(parsed) accepted_parsed;

  if (not_accepted) {
    // don't throw, just fill vector `not_accepted'
    not_accepted->clear();
    for (auto& opt : parsed) {
      try {
        if (checker && settings_.count(opt.first)) {
          auto e = checker(settings_.at(opt.first), opt.first);
          if (e.hasValue()) {
            throw boost::program_options::error(e.value());
          }
        }
        accepted_parsed.insert(opt);
      } catch (const boost::program_options::error& e) {
        (*not_accepted)[opt.first] = e.what();
      }
    }
  } else {
    // throw the first error that appears
    for (auto& opt : parsed) {
      if (settings_.count(opt.first)) {
        if (checker) {
          auto e = checker(settings_.at(opt.first), opt.first);
          if (e.hasValue()) {
            throw boost::program_options::error(e.value());
          }
        }
        accepted_parsed.insert(opt);
      }
    }
  }

  boost::program_options::notify(accepted_parsed);

  if (unrecognized) {
    *unrecognized = boost::program_options::collect_unrecognized(
        parsed_options.options, boost::program_options::include_positional);
  }
  if (remove) {
    for (const std::string& name : *remove) {
      ld_check(!parsed.count(name));
      ld_check(settings_.count(name));
      ld_assert(!fails_check(checker, settings_.at(name)));
      // Pretend that this setting was parsed from argv with empty vector value.
      parsed_options.options.emplace_back(name, std::vector<std::string>());
      accepted_parsed.emplace(name, boost::program_options::variable_value());
    }
  }

  if (store) {
    for (auto& opt : parsed_options.options) {
      if (opt.unregistered) {
        continue;
      }
      if (opt.string_key.empty()) {
        continue;
      }
      if (!settings_.count(opt.string_key)) {
        continue;
      }
      if (!accepted_parsed.count(opt.string_key)) {
        continue;
      }

      SettingState& state = settings_.at(opt.string_key);
      auto& src_val = state.sources[static_cast<int>(source)];
      std::string old_source_value = formatValue(src_val);
      std::string old_effective_value = formatEffectiveValue(state);

      src_val.clear();

      if (opt.value.empty()) {
        const bool* b;
        if (parsed.count(opt.string_key) &&
            (b = boost::any_cast<bool>(&parsed.at(opt.string_key).value()))) {
          // This setting is a bool which was defined with an implicit value.
          // Ensure we store the explicit value here.
          src_val.push_back(*b ? "true" : "false");
        }
      } else {
        src_val.insert(src_val.end(), opt.value.begin(), opt.value.end());
      }

      if (initial_load_done_ && source != Source::CURRENT) {
        std::string new_source_value = formatValue(src_val);
        if (new_source_value != old_source_value) {
          std::string new_effective_value = formatEffectiveValue(state);
          ld_info("Setting %s was changed in %s from %s to %s, effective value "
                  "changed from %s to %s",
                  opt.string_key.c_str(),
                  sourceNames()[source].c_str(),
                  old_source_value.c_str(),
                  new_source_value.c_str(),
                  old_effective_value.c_str(),
                  new_effective_value.c_str());
        }
      }
    }
  }
}

std::vector<std::string>&
SettingsUpdater::valueFromSourceRef(const std::string& setting, Source src) {
  const int src_int = static_cast<int>(src);
  ld_assert(settings_.count(setting));
  return settings_[setting].sources[src_int];
}

folly::Optional<std::string>
SettingsUpdater::getValueFromSource(const std::string& setting,
                                    Source src) const {
  const int src_int = static_cast<int>(src);
  auto it = settings_.find(setting);
  ld_check(it != settings_.end());
  auto it_src = it->second.sources.find(src_int);
  if (it_src == it->second.sources.end()) {
    return folly::none;
  }
  return folly::join(" ", it_src->second);
}

folly::Optional<std::string>
SettingsUpdater::getValue(const std::string& setting) const {
  const auto setting_pair = settings_.find(setting);
  if (setting_pair == settings_.end()) {
    return folly::none;
  }
  const auto new_val = computeValue(setting_pair->second);
  return folly::join(" ", new_val);
}

std::vector<std::pair<std::string, std::string>>
SettingsUpdater::getAllValues() const {
  std::vector<std::pair<std::string, std::string>> values;
  for (const auto& setting : settings_) {
    values.emplace_back(
        std::make_pair(setting.first, *getValue(setting.first)));
  }

  return values;
}

void SettingsUpdater::update(setting_checker_t checker) {
  using namespace SettingFlag;
  // After data was updated from a source, we need to recompute what needs to be
  // the new effective value for each setting by applying priority rules across
  // sources. This map holds what are the updates we need to apply for this.
  std::unordered_map<std::string, std::string> new_vals;

  for (auto& opt : settings_) {
    if (fails_check(checker, opt.second)) {
      continue;
    }
    new_vals[opt.first] = *getValue(opt.first);
  }

  try {
    parse(new_vals.begin(), new_vals.end(), Source::CURRENT, true, checker);
  } catch (const boost::program_options::error& ex) {
    // We are re-applying settings that were already validated. Validation
    // should not have failed.
    ld_error("%s", ex.what());
    ld_check(false);
  }

  // Tell each bundle to update their FastUpdateableSharedPtr.
  for (const auto& bundle : bundles_) {
    bundle->update();
  }
}

void SettingsUpdater::setFromClient(std::string name, std::string value) {
  std::lock_guard<std::mutex> guard(mutex_);
  using namespace SettingFlag;
  auto checker = ENSURES_FALSE(
      !(setting.descriptor.flags & CLIENT), "it is not a client option");
  std::vector<std::pair<std::string, std::string>> v = {{name, value}};

  try {
    parse(v.begin(), v.end(), Source::CLIENT, true, checker);
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }

  update(checker);
}

void SettingsUpdater::parseFromCLI(int argc,
                                   const char** argv,
                                   setting_checker_t checker,
                                   fallback_parser_t fallback_parser) {
  std::lock_guard<std::mutex> guard(mutex_);
  std::vector<std::string> unrecognized;
  std::map<std::string, std::string> not_accepted;

  auto checkers = {
      checker,
      ENSURES_FALSE(setting.descriptor.flags & SettingFlag::INTERNAL_ONLY,
                    "it is an internal only option")};

  auto augmented_checker = MAKE_CHECKER_FROM_SEQ(checkers);

  try {
    parse(argc,
          argv,
          Source::CLI,
          true,
          augmented_checker,
          nullptr,
          &unrecognized,
          &not_accepted);
    if (unrecognized.size() > 0 && !fallback_parser) {
      throw boost::program_options::error("Unknown option \"" +
                                          unrecognized[0] + "\"");
    }
    if (not_accepted.size() > 0) {
      auto na = not_accepted.begin();
      throw boost::program_options::error(folly::to<std::string>(
          "Option '", na->first, "' cannot be set. ", na->second));
    }
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }

  if (fallback_parser) {
    // Fill the args unrecognized by this parser into a const char* array
    std::vector<const char*> legacy_argv;
    legacy_argv.push_back(argv[0]);
    for (const auto& legacy_arg : unrecognized) {
      legacy_argv.push_back(legacy_arg.c_str());
    }
    fallback_parser(legacy_argv.size(), legacy_argv.data());
  }

  update(checker);
}

void SettingsUpdater::setInternalSetting(std::string name, std::string value) {
  std::lock_guard<std::mutex> guard(mutex_);
  std::vector<std::pair<std::string, std::string>> v = {{name, value}};
  try {
    parse(v.begin(), v.end(), Source::INTERNAL, true, nullptr);
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }

  update();
}

void SettingsUpdater::setFromCLI(
    std::unordered_map<std::string, std::string> settings) {
  std::lock_guard<std::mutex> guard(mutex_);
  using namespace SettingFlag;
  auto checker = ENSURES_FALSE(
      !(setting.descriptor.flags & SERVER), "it is not a server option");
  try {
    parse(settings.begin(), settings.end(), Source::CONFIG, true, checker);
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }

  update(checker);
}

void SettingsUpdater::validateFromConfig(
    const std::unordered_map<std::string, std::string>& settings,
    SettingFlag::flag_t match_flag) {
  using namespace SettingFlag;
  // TODO: Tell which flags to match in the description of checker below
  auto checkers = {
      ENSURES_FALSE(!(setting.descriptor.flags & match_flag),
                    "it does not match the predetermined flag"),
      ENSURES_FALSE(setting.descriptor.flags & CLI_ONLY,
                    "it must be passed via the command line interface"),
      ENSURES_FALSE(
          setting.descriptor.flags & INTERNAL_ONLY, "it is internal only")};
  auto checker = MAKE_CHECKER_FROM_SEQ(checkers);
  try {
    parse(settings.begin(), settings.end(), Source::CONFIG, false, checker);
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }
}

void SettingsUpdater::setFromConfig(
    std::unordered_map<std::string, std::string> settings) {
  std::lock_guard<std::mutex> guard(mutex_);
  using namespace SettingFlag;

  std::unordered_set<std::string> removed_because_requires_restart;

  if (initial_load_done_) {
    // It's not the first time we load from the config, filter out the options
    // that require a restart so we don't process them but at the same time
    // don't trigger an error.
    for (auto it = settings.begin(); it != settings.end();) {
      if (settings_.count(it->first) &&
          settings_[it->first].descriptor.flags & REQUIRES_RESTART) {
        auto cur_val =
            folly::join(" ", valueFromSourceRef(it->first, Source::CONFIG));
        if (cur_val != it->second) {
          ld_warning("Setting \"%s\" is changed from \"%s\" to \"%s\" in the "
                     "config but this change will not take effect until "
                     "logdeviced is restarted.",
                     it->first.c_str(),
                     cur_val.c_str(),
                     it->second.c_str());
        }
        removed_because_requires_restart.insert(it->first);
        it = settings.erase(it);
      } else {
        ++it;
      }
    }
  }

  // Find all the args that are not in the map and unset them for this source.
  std::vector<std::string> to_unset;
  for (auto& p : settings_) {
    const auto& val = p.second.sources[static_cast<int>(Source::CONFIG)];
    if (!val.empty() && !settings.count(p.first)) {
      // This setting has a value from the config but is not in `settings`.
      // If the setting does not have the REQUIRES_RESTART flag, just clear the
      // value for the CONFIG source, update() will then take care of reapplying
      // the precedence rules. If the setting does have the REQUIRES_RESTART,
      // and if we did not remove it earlier in this function, log a warning.
      ld_check(initial_load_done_);
      if (!(p.second.descriptor.flags & REQUIRES_RESTART)) {
        to_unset.push_back(p.first);
      } else if (!removed_because_requires_restart.count(p.first)) {
        ld_warning("Setting \"%s\" with value \"%s\" is removed from the "
                   "config but this change will not take effect until "
                   "logdeviced is restarted.",
                   p.first.c_str(),
                   folly::join(" ", val).c_str());
      }
    }
  }

  auto checkers = {
      ENSURES_FALSE(setting.descriptor.flags & CLI_ONLY,
                    "it must be passed via the command line interface"),
      ENSURES_FALSE(
          setting.descriptor.flags & INTERNAL_ONLY, "it is internal only")};
  auto checker = MAKE_CHECKER_FROM_SEQ(checkers);

  try {
    std::vector<std::string> unrecognized;
    std::map<std::string, std::string> not_accepted;
    parse(settings.begin(),
          settings.end(),
          Source::CONFIG,
          true,
          checker,
          &to_unset,
          &unrecognized,
          &not_accepted);
    if (unrecognized.size() > 0) {
      ld_warning("Unrecognized settings in the config file: [\"%s\"]",
                 folly::join("\", \"", unrecognized).c_str());
    }
    if (not_accepted.size() > 0) {
      std::vector<std::string> errors;
      for (auto& p : not_accepted) {
        errors.push_back(p.second);
      }
      ld_warning("Inadequate settings in the config file: [%s]",
                 folly::join("; ", errors).c_str());
    }
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }

  update(checker);
  initial_load_done_ = true;
}

void SettingsUpdater::setFromAdminCmd(std::string name, std::string value) {
  std::lock_guard<std::mutex> guard(mutex_);
  using namespace SettingFlag;

  if (boost::starts_with(name, "--")) {
    name.erase(0, 2);
  }

  auto checkers = {
      ENSURES_FALSE(
          !(setting.descriptor.flags & SERVER), "it is not a server option"),
      ENSURES_FALSE(setting.descriptor.flags & CLI_ONLY,
                    "it must be passed via the command line interface"),
      ENSURES_FALSE(setting.descriptor.flags & REQUIRES_RESTART,
                    "it requires a server restart to take effect"),
      ENSURES_FALSE(
          setting.descriptor.flags & INTERNAL_ONLY, "it is internal only")};
  auto checker = MAKE_CHECKER_FROM_SEQ(checkers);

  std::vector<std::pair<std::string, std::string>> v = {{name, value}};

  try {
    parse(v.begin(), v.end(), Source::ADMIN_CMD, true, checker);
  } catch (const boost::program_options::error& ex) {
    ld_error("%s", ex.what());
    throw;
  }

  update(checker);
}

void SettingsUpdater::unsetFromAdminCmd(std::string name) {
  std::lock_guard<std::mutex> guard(mutex_);
  using namespace SettingFlag;

  if (boost::starts_with(name, "--")) {
    name.erase(0, 2);
  }

  auto checkers = {
      ENSURES_FALSE(
          !(setting.descriptor.flags & SERVER), "it is not a server option"),
      ENSURES_FALSE(setting.descriptor.flags & CLI_ONLY,
                    "it must be passed via the command line interface"),
      ENSURES_FALSE(
          setting.descriptor.flags & INTERNAL_ONLY, "it is internal only")};
  auto checker = MAKE_CHECKER_FROM_SEQ(checkers);

  if (!settings_.count(name)) {
    throw boost::program_options::error("Unknown option \"" + name + "\"");
  } else {
    auto e = checker(settings_[name], name);
    if (e.hasValue()) {
      throw boost::program_options::error(folly::to<std::string>(
          "Option '", name, "' cannot be used: ", e.value()));
    }

    std::vector<std::string> to_unset = {name};
    parse(0, nullptr, Source::ADMIN_CMD, /* store */ true, checker, &to_unset);
    update(checker);
  }
}

const std::unordered_map<std::string, SettingsUpdater::SettingState>
SettingsUpdater::getState() const {
  std::lock_guard<std::mutex> guard(mutex_);
  return settings_;
}

// SourceNameMap boilerplate below.

SettingsUpdater::SourceNameMap& SettingsUpdater::sourceNames() {
  static SettingsUpdater::SourceNameMap map;
  return map;
}

template <>
/* static */
const std::string& SettingsUpdater::SourceNameMap::invalidValue() {
  static const std::string v("INVALID");
  return v;
}

template <>
void SettingsUpdater::SourceNameMap::setValues() {
  static_assert(
      (int)SettingsUpdater::Source::CURRENT == 5,
      "If you added a SettingsUpdater::Source, update the EnumMap here");
#define SET_SOURCE_NAME(x) set(SettingsUpdater::Source::x, lowerCase(#x));
  SET_SOURCE_NAME(INTERNAL);
  SET_SOURCE_NAME(CLI);
  SET_SOURCE_NAME(CLIENT);
  SET_SOURCE_NAME(CONFIG);
  SET_SOURCE_NAME(ADMIN_CMD);
  SET_SOURCE_NAME(CURRENT);
#undef SET_SOURCE_NAME
}

}} // namespace facebook::logdevice
