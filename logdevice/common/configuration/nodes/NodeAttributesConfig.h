/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
#pragma once

#include <map>
#include <unordered_map>

#include <folly/Format.h>
#include <folly/Optional.h>

#include "logdevice/common/ShardID.h"
#include "logdevice/common/configuration/nodes/NodeRole.h"
#include "logdevice/common/debug.h"
#include "logdevice/common/util.h"
#include "logdevice/include/Err.h"

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

template <typename Attributes>
class NodeAttributesConfig {
 public:
  enum class UpdateType { PROVISION = 0, REMOVE, RESET };

  struct NodeUpdate {
    UpdateType transition{};
    std::unique_ptr<Attributes> attributes{nullptr};

    bool isValid() const;
    std::string toString() const;
  };

  struct Update {
   public:
    using NodeMap = std::map<node_index_t, NodeUpdate>;
    NodeMap node_updates;

    int addNode(node_index_t node, NodeUpdate update) {
      auto res = node_updates.emplace(node, std::move(update));
      return res.second ? 0 : -1;
    }

    bool isValid() const;
    std::string toString() const;
  };

  int applyUpdate(const Update& update,
                  NodeAttributesConfig* new_config_out) const;

  // perform validation specific to the type of attributes, return true if the
  // config passed validation. declared but not defined, we expect each
  // instantiated class to provide their own definitions
  bool attributeSpecificValidate() const;

  bool validate() const;
  void dcheckConsistency() const;

  folly::Optional<Attributes> getNodeAttributes(node_index_t node) const;
  const Attributes* getNodeAttributesPtr(node_index_t node) const;

  // caller must ensure that node exists in the configuration
  const Attributes& nodeAttributesAt(node_index_t node) const;

  bool hasNode(node_index_t node) const {
    return node_states_.count(node) > 0;
  }

  size_t count(node_index_t node) const {
    return node_states_.count(node);
  }

  bool isEmpty() const {
    return node_states_.empty();
  }

  size_t numNodes() const {
    return node_states_.size();
  }

  bool operator==(const NodeAttributesConfig& rhs) const;

  using MapType = std::unordered_map<node_index_t, Attributes>;
  using const_iterator = typename MapType::const_iterator;

  const_iterator begin() const {
    return node_states_.cbegin();
  }
  const_iterator end() const {
    return node_states_.cend();
  }
  const_iterator find(node_index_t node) const {
    return node_states_.find(node);
  }

 private:
  MapType node_states_{};

  void setNodeAttributes(node_index_t node, Attributes state);
  bool eraseNodeAttribute(node_index_t node);

  friend class NodesConfigLegacyConverter;
  friend class NodesConfigurationThriftConverter;
};

}}}} // namespace facebook::logdevice::configuration::nodes

#include "NodeAttributesConfig-inl.h"
