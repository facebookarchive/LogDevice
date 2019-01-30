/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <chrono>
#include <list>
#include <type_traits>
#include <unordered_map>

#include <boost/make_shared.hpp>
#include <boost/noncopyable.hpp>
#include <boost/program_options.hpp>

#include "logdevice/common/UpdateableSharedPtr.h"
#include "logdevice/common/checks.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/types_internal.h"
#include "logdevice/common/util.h"

namespace facebook { namespace logdevice {

namespace SettingFlag {
typedef uint32_t flag_t;
std::string toHelpString(flag_t flags);
std::string toMarkdown(flag_t flags);
} // namespace SettingFlag

class UpdateableSettingsBase {
 public:
  // State kept for each setting.
  struct SettingDescriptor {
    // textual description of the setting
    std::string description;
    // category to which this setting belongs
    std::string category;
    // full help string to be output by boost::progam_options routines.
    // This includes description, default value, flags.
    std::string help;
    // option_description object that is used for validating and applying
    // changes.
    boost::shared_ptr<boost::program_options::option_description>
        boost_description;
    // option_description object that is used only for validating.
    boost::shared_ptr<boost::program_options::option_description>
        boost_description_validate_only;
    // Flags for the setting.
    uint32_t flags;
    // Default value for the setting.
    std::vector<std::string> default_value;
    // Default value to show in documentation. If not set, `default_value` will
    // be used
    folly::Optional<std::string> default_value_docs_override;
  };

  UpdateableSettingsBase() {}
  virtual ~UpdateableSettingsBase() {}
  // Retrieve the name of the bundle.
  virtual std::string getName() const = 0;

 private:
  // Inform boost::program_options of the default value for a settng.
  void setDefaultValue(const char* name,
                       const char* value,
                       const char* docs_override);
  // Return a mapping from setting names to their state.
  const std::unordered_map<std::string, SettingDescriptor> getSettings() const {
    return settings_;
  }

 protected:
  virtual void update() = 0;
  std::unordered_map<std::string, SettingDescriptor> settings_;
  friend class SettingEasyInit;
  friend class SettingsUpdater;
};

/**
 * A helper function that transparently forwards a functor if it only has one
 * argument or binds the property name if there are 2 arguments
 */
namespace {
template <typename T>
std::function<void(T)> bind_function(std::function<void(T)> fn, const char*) {
  return fn;
}
template <typename T>
std::function<void(T)> bind_function(std::function<void(const char*, T)> fn,
                                     const char* name) {
  // `name` may be a temporary. Copy it into an std::string captured by value.
  return [fn, name_str = std::string(name)](T t) {
    return fn(name_str.c_str(), t);
  };
}
template <typename T>
std::function<T(const std::string&)>
bind_function(std::function<T(const std::string&)> fn, const char*) {
  return fn;
}
template <typename T>
std::function<T(const std::string&)>
bind_function(std::function<T(const char*, const std::string&)> fn,
              const char* name) {
  return [fn, name_str = std::string(name)](const std::string& val) {
    return fn(name_str.c_str(), val);
  };
}
template <typename T>
std::function<void(T)> bind_function(std::nullptr_t, const char* /*name*/) {
  return nullptr;
}
} // namespace

/**
 * Useful functor for adding settings to a UpdateableSettingsBase.
 */
class SettingEasyInit : boost::noncopyable {
 public:
  explicit SettingEasyInit(
      UpdateableSettingsBase* owner,
      const std::unordered_map<std::string, std::string>& override_defaults)
      : owner_(owner), overrideDefaults_(override_defaults) {}

  // TODO(T8584641): explore the possibility to have storage be a pointer to
  // data member of D. If we can at compile time ensure D is the type of the
  // bundle, we can then prevent the user from passing unexpected pointers
  // inside storage.  Another advantage is that we can then later on change the
  // implementation of UpdateableSettings to leverage that and decide to write
  // to different memory locations.
  //
  // Here is what it would look like:
  //
  // class MySettings : public SettingsBundle {
  //  public:
  //    int setting;
  //    void defineSettings(SettingEasyInit& init) {
  //      init
  //        ("setting", &MySettings::setting, "42",
  //         nullptr,
  //         "This is a test setting",
  //         SettingFlag::SERVER)
  //    }
  // };
  //
  // template<typename T, typename D, typename F>
  // SettingEasyInit& operator()(const char *name,
  //                             T D::* storage, // pointer to member of bundle
  //                             const char *default_value,
  //                             const F validate_fn,
  //                             const char *description,
  //                             uint32_t flags) {
  //   [..]
  //   static_assert(std::is_base_of<UpdateableSettingsBase, D>::value, "");
  //   T val = (*static_cast<D*>(owner_.data)).*storage;
  //   [..]

  //   return *this;
  // }

  // Trait used to figure out whether the validator provided by the user for
  // operator() is also the parser for the value to be stored in `storage`. If
  // that's the case, it is expected the validator returns the parsed value.
  // If the validator does not return a value or if it is nullptr, we expect
  // there exist a function istream& operator>>(istream& in, T& val) to parse
  // the value to be stored in `storage`.
  template <typename T, typename F>
  struct validator_is_parser
      : std::integral_constant<
            bool,
            !std::is_same<std::nullptr_t,
                          typename std::remove_cv<F>::type>::value &&
                std::is_convertible<
                    F,
                    typename std::function<T(const std::string&)>>::value> {};

  // Utility that provides a validate() and notify() function. Depending on
  // whether the validator is also the parser, the Implementation differs.
  template <typename T, typename F, bool D = validator_is_parser<T, F>::value>
  struct SettingValidator;

  // Implementation for when the validator is also the parser.
  template <typename T, typename F>
  struct SettingValidator<T, F, true> {
    T* storage;
    F validator;
    using val_type_t = std::string;
    SettingValidator(T* s, F v) : storage(s), validator(v) {}
    void validate(const std::string& val) {
      validator(val);
    }
    void notify(const std::string& val) {
      ld_check(storage != nullptr);
      *storage = validator(val);
    }
  };

  // Implementation for when the validator is not the parser, and may be
  // nullptr.
  template <typename T, typename F>
  struct SettingValidator<T, F, false> {
    T* storage;
    F validator;
    using val_type_t = T;
    SettingValidator(T* s, F v) : storage(s), validator(v) {}
    void validate(T val) {
      // We wrap with std::function to avoid a build error if F is nullptr_t.
      std::function<void(T)> raw = validator;
      if (raw) {
        raw(val);
      }
    }
    void notify(T val) {
      validate(val);
      if (storage) { // storage can be null for deprecated args
        *storage = val;
      }
    }
  };

  // Set the implicit value on a po::value. If the value is a boolean, set the
  // implicit value to true, otherwise set no implicit value.
  template <typename V, typename T>
  struct set_implicit_value {
    void operator()(boost::program_options::typed_value<V>* /*v*/) {}
  };
  template <typename V>
  struct set_implicit_value<V, bool> {
    void operator()(boost::program_options::typed_value<V>* v) {
      v->implicit_value(folly::to<V>("true"));
    }
  };

  /**
   * Define a setting of type T.
   *
   * @param name of the setting
   * @param storage       Pointer to a member of the bundle where the setting's
   *                      value must be written. Can only be be nullptr if
   *                      validate_fn returns void (this is used for deprecated
   *                      settings)
   * @param default_value Default value for the setting.
   * @param validate_fn   Function that is used to validate a new value for the
   *                      setting. The function should throw a
   *                      boost::program_options::error on failure. If this
   *                      function returns a value of type T, then it is used to
   *                      parse the value of the option and the returned value
   *                      will be stored in `storage`. If the function does not
   *                      return a value of type T or if it is nullptr, it is
   *                      expected there exists a function istream>
   *                      opretaor<<(isstream& in, T& val) to parse the value.
   * @param description   A description of the setting.
   * @param flags         Flags for the setting. @see SettingFlag.
   * @param category      Name of the category to which the setting belongs.
   */
  template <typename T, typename F>
  SettingEasyInit& operator()(const char* name,
                              T* storage,
                              const char* default_value,
                              const F validate_fn,
                              const char* description,
                              uint32_t flags,
                              const char* category = "Uncategorized",
                              const char* default_doc_override = nullptr) {
    // Assert no duplicates.
    ld_check(!owner_->settings_.count(name));

    // Override default value if requested.
    {
      auto it = overrideDefaults_.find(name);
      if (it != overrideDefaults_.end()) {
        auto inserted = defaultOverridesApplied_.insert(name);
        ld_check(inserted.second);
        default_value = it->second.c_str();
      }
    }

    auto actual_validate_fn = bind_function<T>(validate_fn, name);
    namespace po = boost::program_options;
    using validator_t = SettingValidator<T, decltype(actual_validate_fn)>;
    using val_type_t = typename validator_t::val_type_t;
    auto v = validator_t(storage, actual_validate_fn);
    auto validate = [=](val_type_t val) mutable { v.validate(val); };
    auto notifier = [=](val_type_t val) mutable { v.notify(val); };
    auto v_notif = po::value<val_type_t>()->notifier(notifier);
    auto v_valid = po::value<val_type_t>()->notifier(validate);

    set_implicit_value<val_type_t, T>()(v_valid);
    set_implicit_value<val_type_t, T>()(v_notif);

    UpdateableSettingsBase::SettingDescriptor opt;

    opt.description = description;
    opt.category = category ? category : "Uncategorized";

    opt.help = "Default: \"";
    opt.help += default_doc_override ? default_doc_override : default_value;
    opt.help += "\".";
    const std::string flags_str = SettingFlag::toHelpString(flags);
    if (!flags_str.empty()) {
      opt.help += " Flags: " + flags_str + ".";
    }
    opt.help += "\n";
    opt.help += description;

    opt.boost_description = boost::make_shared<po::option_description>(
        name, v_notif, opt.help.c_str());
    opt.boost_description_validate_only =
        boost::make_shared<po::option_description>(
            name, v_valid, opt.help.c_str());
    opt.flags = flags;

    owner_->settings_[name] = std::move(opt);
    owner_->setDefaultValue(name, default_value, default_doc_override);
    return *this;
  }

  const std::unordered_set<std::string>& getDefaultOverridesApplied() {
    return defaultOverridesApplied_;
  }

 private:
  UpdateableSettingsBase* owner_;
  const std::unordered_map<std::string, std::string>& overrideDefaults_;
  std::unordered_set<std::string> defaultOverridesApplied_;
};

// The actual object that owns a FastUpdateableSharedPtr. UpdateableSettings is
// just a tiny wrapper around a shared_ptr<UpdateableSettingsRaw<T>>.
template <typename T>
class UpdateableSettingsRaw : public UpdateableSettingsBase {
 public:
  /**
   * Create the UpdateableSettingsRaw object. Default initialize settings
   * according to their definition in defineSettings, except for settings listed
   * in override_defaults.
   */
  UpdateableSettingsRaw(
      const std::unordered_map<std::string, std::string>& override_defaults) {
    SettingEasyInit init(this, override_defaults);
    data_.defineSettings(init);

    if (init.getDefaultOverridesApplied().size() != override_defaults.size()) {
      ld_critical("Unexpected setting names in override_defaults. "
                  "override_defaults = %s, recognised names: %s.",
                  toString(override_defaults).c_str(),
                  toString(init.getDefaultOverridesApplied()).c_str());
      ld_check(false);
    }

    // Make sure the default values are propagated over to the
    // FastUpdateableSharedPtr.
    update();
  }

  UpdateableSettingsRaw()
      : UpdateableSettingsRaw(std::unordered_map<std::string, std::string>()) {}

  /**
   * Create the UpdateableSettingsRaw with initial values for the settings that
   * don't necessarily match their defaults. For testing only as values should
   * always be set through SettingsUpdater, but sometimes tests don't have
   * access to one.
   * TODO(T8584641): add compile time check that verifies we don't register this
   * object to SettingsUpdater if it was created with this constructor.
   */
  explicit UpdateableSettingsRaw(T data) : UpdateableSettingsRaw<T>() {
    data_ = data;
    update();
  }

  /**
   * @return name of the bundle.
   */
  std::string getName() const override {
    return data_.getName();
  }

  /**
   * @return a std::shared_ptr of the underlying bundle.
   */
  std::shared_ptr<const T> get() const {
    return std::const_pointer_cast<const T>(ptr_.get());
  }
  std::shared_ptr<const T> operator->() {
    return get();
  }

  /**
   * Called by SettingsUpdater after it made some updates to data_ by exercising
   * the option_description objects in SettingDescriptor so that readers can
   * start seeing the new version of the data.
   */
  void update() override {
    // This should not be re-entered from the callbacks
    ld_check(!running_callback_);

    ptr_.update(std::make_shared<T>(data_));
    std::lock_guard<std::mutex> guard(subscriber_mutex_);
    running_callback_ = true;
    for (auto& callback : subscriber_callbacks_) {
      callback();
    }
    running_callback_ = false;
  }

  // Type of handle returned by subscribeToUpdates().
  class SubscriptionHandle : boost::noncopyable {
   public:
    SubscriptionHandle() : updateable_settings_(nullptr) {}
    SubscriptionHandle(SubscriptionHandle&& src) noexcept {
      *this = std::move(src);
    }
    SubscriptionHandle& operator=(SubscriptionHandle&& src) {
      this->updateable_settings_ = src.updateable_settings_;
      this->it_ = std::move(src.it_);
      src.updateable_settings_ = nullptr;
      return *this;
    }
    void unsubscribe() {
      if (updateable_settings_) {
        updateable_settings_->unsubscribeFromUpdates(*this);
        // the line above should've reset the handle's pointer to the
        // UpdateableSettings instance
        ld_check(updateable_settings_ == nullptr);
      }
    }
    ~SubscriptionHandle() {
      unsubscribe();
    }

   private:
    SubscriptionHandle(UpdateableSettingsRaw<T>* updateable_settings,
                       std::list<std::function<void()>>::iterator it)
        : updateable_settings_(updateable_settings), it_(it) {
      ld_check(updateable_settings_);
    }

    UpdateableSettingsRaw<T>* updateable_settings_;
    std::list<std::function<void()>>::iterator it_;
    friend class UpdateableSettingsRaw<T>;
  };

  // See UpdateableSettings.h
  SubscriptionHandle subscribeToUpdates(std::function<void()> callback) {
    std::lock_guard<std::mutex> guard(subscriber_mutex_);
    // Insert at end of list and return iterator as handle
    auto it = subscriber_callbacks_.insert(
        subscriber_callbacks_.end(), std::move(callback));
    return SubscriptionHandle(this, it);
  }

  // See UpdateableSettings.h
  SubscriptionHandle callAndSubscribeToUpdates(std::function<void()> callback) {
    std::lock_guard<std::mutex> guard(subscriber_mutex_);
    callback();
    // Insert at end of list and return iterator as handle
    auto it = subscriber_callbacks_.insert(
        subscriber_callbacks_.end(), std::move(callback));
    return SubscriptionHandle(this, it);
  }

  // Removes the callback that the handle points to from the subscriber list.
  void unsubscribeFromUpdates(SubscriptionHandle& handle) {
    std::lock_guard<std::mutex> guard(subscriber_mutex_);
    ld_check(handle.updateable_settings_ == this);
    subscriber_callbacks_.erase(handle.it_);
    handle.updateable_settings_ = nullptr;
  }

 private:
  // Written to by boost::program_options through SettingsUpdater.
  T data_;
  // Written to when SettingsUpdater calls update() after it modified `data_`.
  FastUpdateableSharedPtr<T> ptr_;
  // Subscribers to setting changes
  std::list<std::function<void()>> subscriber_callbacks_;

  // Mutex for subscriber_callbacks_
  std::mutex subscriber_mutex_;

  // Used for checking re-entrance between setting updates and callbacks from
  // subscriptions
  bool running_callback_{false};
};

}} // namespace facebook::logdevice
