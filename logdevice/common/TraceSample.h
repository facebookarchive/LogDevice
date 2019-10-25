/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/Optional.h>

namespace facebook { namespace logdevice {
class TraceSample {
 public:
  void reset();
  void addIntValue(const std::string& key, int64_t value);
  void addNormalValue(const std::string& key, std::string value);
  void addNormVectorValue(const std::string& key,
                          std::vector<std::string> array);

  void addMapValue(const std::string& key,
                   std::map<std::string, std::string> map);
  void addSetValue(const std::string& key, std::set<std::string> set);

  int64_t getIntValue(const std::string& key) const;
  const std::string& getNormalValue(const std::string& key) const;
  const std::vector<std::string>&
  getNormVectorValue(const std::string& key) const;
  const std::map<std::string, std::string>&
  getMapValue(const std::string& key) const;
  const std::set<std::string>& getSetValue(const std::string& key) const;

  // folly::Optional alternatives
  folly::Optional<int64_t> getOptionalIntValue(const std::string& key) const;
  folly::Optional<std::string>
  getOptionalNormalValue(const std::string& key) const;
  folly::Optional<std::vector<std::string>>
  getOptionalNormVectorValue(const std::string& key) const;
  folly::Optional<std::map<std::string, std::string>>
  getOptionalMapValue(const std::string& key) const;
  folly::Optional<std::set<std::string>>
  getOptionalSetValue(const std::string& key) const;

  bool isNormalValueSet(const std::string& key) const;
  bool isIntValueSet(const std::string& key) const;
  bool isMapValueSet(const std::string& key) const;
  bool isNormVectorValueSet(const std::string& key) const;
  bool isSetValueSet(const std::string& key) const;

  std::string toJson() const;

 private:
  const std::string INT_KEY = "int";
  const std::string NORMAL_KEY = "normal";
  const std::string NORMVECTOR_KEY = "normvector";
  const std::string MAP_KEY = "map";
  const std::string SET_KEY = "set";
  const std::vector<std::string>& empty_vector_() const;
  const std::map<std::string, std::string>& empty_map_() const;
  const std::set<std::string>& empty_set_() const;
  const std::string empty_str_ = "";
  std::unordered_map<std::string, int64_t> ints_;
  std::unordered_map<std::string, std::string> strs_;
  std::unordered_map<std::string, std::vector<std::string>> vectors_;
  // Using std::map because thrift accepts it
  std::unordered_map<std::string, std::map<std::string, std::string>> maps_;
  std::unordered_map<std::string, std::set<std::string>> sets_;
};

}} // namespace facebook::logdevice
