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

void TraceSample::reset() {
  ints_.clear();
  strs_.clear();
  vectors_.clear();
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

// this will return 0 if the key does not exist.
int64_t TraceSample::getIntValue(const std::string& key) const {
  try {
    return ints_.at(key);
  } catch (const std::out_of_range& _) {
    return 0;
  }
}

// this will return empty string if the key does not exist.
const std::string& TraceSample::getNormalValue(const std::string& key) const {
  try {
    return strs_.at(key);
  } catch (const std::out_of_range& _) {
    return empty_str_;
  }
}

// returns an empty vector if key does not exist.
const std::vector<std::string>&
TraceSample::getNormVectorValue(const std::string& key) const {
  try {
    return vectors_.at(key);
  } catch (const std::out_of_range& _) {
    return empty_vector_();
  }
}

const std::map<std::string, std::string>&
TraceSample::getMapValue(const std::string& key) const {
  try {
    return maps_.at(key);
  } catch (const std::out_of_range& _) {
    return empty_map_();
  }
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

  return folly::toJson(json);
}

}} // namespace facebook::logdevice
