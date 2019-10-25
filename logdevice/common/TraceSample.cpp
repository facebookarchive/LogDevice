/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#include "logdevice/common/TraceSample.h"

#include <stdexcept>

#include <folly/dynamic.h>
#include <folly/json.h>

namespace facebook { namespace logdevice {

const std::vector<std::string>& TraceSample::empty_vector_() const {
  // Implemented that way because of
  // https://isocpp.org/wiki/faq/ctors#static-init-order
  static std::vector<std::string>* empty_vec = new std::vector<std::string>();
  return *empty_vec;
};

const std::map<std::string, std::string>& TraceSample::empty_map_() const {
  // Implemented that way because of
  // https://isocpp.org/wiki/faq/ctors#static-init-order
  static std::map<std::string, std::string>* empty_map =
      new std::map<std::string, std::string>();
  return *empty_map;
}

const std::set<std::string>& TraceSample::empty_set_() const {
  // Implemented that way because of
  // https://isocpp.org/wiki/faq/ctors#static-init-order
  static std::set<std::string>* empty_set = new std::set<std::string>();
  return *empty_set;
}

void TraceSample::reset() {
  ints_.clear();
  strs_.clear();
  vectors_.clear();
  maps_.clear();
  sets_.clear();
}

void TraceSample::addIntValue(const std::string& key, int64_t value) {
  ints_[key] = value;
}

void TraceSample::addNormalValue(const std::string& key, std::string value) {
  strs_[key] = std::move(value);
}

void TraceSample::addNormVectorValue(const std::string& key,
                                     std::vector<std::string> array) {
  vectors_[key] = std::move(array);
}

void TraceSample::addMapValue(const std::string& key,
                              std::map<std::string, std::string> map) {
  maps_[key] = std::move(map);
}

void TraceSample::addSetValue(const std::string& key,
                              std::set<std::string> set) {
  sets_[key] = std::move(set);
}

// this will return 0 if the key does not exist.
int64_t TraceSample::getIntValue(const std::string& key) const {
  if (!isIntValueSet(key)) {
    return 0;
  }
  return ints_.at(key);
}

// this will return empty string if the key does not exist.
const std::string& TraceSample::getNormalValue(const std::string& key) const {
  if (!isNormalValueSet(key)) {
    return empty_str_;
  }
  return strs_.at(key);
}

// returns an empty vector if key does not exist.
const std::vector<std::string>&
TraceSample::getNormVectorValue(const std::string& key) const {
  if (!isNormVectorValueSet(key)) {
    return empty_vector_();
  }
  return vectors_.at(key);
}

const std::map<std::string, std::string>&
TraceSample::getMapValue(const std::string& key) const {
  if (!isMapValueSet(key)) {
    return empty_map_();
  }
  return maps_.at(key);
}

const std::set<std::string>&
TraceSample::getSetValue(const std::string& key) const {
  try {
    return sets_.at(key);
  } catch (const std::out_of_range& _) {
    return empty_set_();
  }
}

folly::Optional<int64_t>
TraceSample::getOptionalIntValue(const std::string& key) const {
  if (!isIntValueSet(key)) {
    return folly::none;
  }
  return getIntValue(key);
}

folly::Optional<std::string>
TraceSample::getOptionalNormalValue(const std::string& key) const {
  if (!isNormalValueSet(key)) {
    return folly::none;
  }
  return getNormalValue(key);
}

folly::Optional<std::vector<std::string>>
TraceSample::getOptionalNormVectorValue(const std::string& key) const {
  if (!isNormVectorValueSet(key)) {
    return folly::none;
  }
  return getNormVectorValue(key);
}

folly::Optional<std::map<std::string, std::string>>
TraceSample::getOptionalMapValue(const std::string& key) const {
  if (!isMapValueSet(key)) {
    return folly::none;
  }
  return getMapValue(key);
}

folly::Optional<std::set<std::string>>
TraceSample::getOptionalSetValue(const std::string& key) const {
  if (!isSetValueSet(key)) {
    return folly::none;
  }
  return getSetValue(key);
}

bool TraceSample::isNormalValueSet(const std::string& key) const {
  return strs_.count(key) > 0;
}

bool TraceSample::isIntValueSet(const std::string& key) const {
  return ints_.count(key) > 0;
}

bool TraceSample::isMapValueSet(const std::string& key) const {
  return maps_.count(key) > 0;
}

bool TraceSample::isNormVectorValueSet(const std::string& key) const {
  return vectors_.count(key) > 0;
}

bool TraceSample::isSetValueSet(const std::string& key) const {
  return sets_.count(key) > 0;
}

std::string TraceSample::toJson() const {
  folly::dynamic json(folly::dynamic::object);

  if (!ints_.empty()) {
    json[INT_KEY] = folly::dynamic::object;
    for (auto& kv : ints_) {
      json[INT_KEY][kv.first] = kv.second;
    }
  }
  if (!strs_.empty()) {
    json[NORMAL_KEY] = folly::dynamic::object;
    for (auto& kv : strs_) {
      json[NORMAL_KEY][kv.first] = kv.second;
    }
  }
  if (!vectors_.empty()) {
    json[NORMVECTOR_KEY] = folly::dynamic::object;
    for (auto& kv : vectors_) {
      json[NORMVECTOR_KEY][kv.first] =
          folly::dynamic(kv.second.begin(), kv.second.end());
    }
  }
  if (!maps_.empty()) {
    json[MAP_KEY] = folly::dynamic::object;

    for (const auto& km : maps_) {
      folly::dynamic map = folly::dynamic::object;
      for (const auto& p : km.second) {
        map[p.first] = p.second;
      }

      json[MAP_KEY][km.first] = map;
    }
  }

  if (!sets_.empty()) {
    json[SET_KEY] = folly::dynamic::object;
    for (auto& kv : sets_) {
      json[SET_KEY][kv.first] =
          folly::dynamic(kv.second.begin(), kv.second.end());
    }
  }

  return folly::toJson(json);
}

}} // namespace facebook::logdevice
