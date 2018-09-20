/**
 * Copyright (c) 2017-present, Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace facebook { namespace logdevice {
class TraceSample {
 public:
  void reset();
  void addIntValue(const std::string& key, int64_t value);
  void addNormalValue(const std::string& key, std::string value);
  void addNormVectorValue(const std::string& key,
                          std::vector<std::string> array);

  int64_t getIntValue(const std::string& key) const;
  const std::string& getNormalValue(const std::string& key) const;
  const std::vector<std::string>&
  getNormVectorValue(const std::string& key) const;

  bool isNormalValueSet(const std::string& key) const;
  bool isIntValueSet(const std::string& key) const;

  std::string toJson() const;

 private:
  const std::string INT_KEY = "int";
  const std::string NORMAL_KEY = "normal";
  const std::string NORMVECTOR_KEY = "normvector";
  const std::vector<std::string>& empty_vector_() const;
  const std::string empty_str_ = "";
  std::unordered_map<std::string, int64_t> ints_;
  std::unordered_map<std::string, std::string> strs_;
  std::unordered_map<std::string, std::vector<std::string>> vectors_;
};

}} // namespace facebook::logdevice
