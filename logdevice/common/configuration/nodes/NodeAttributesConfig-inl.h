/**
 * Copyright (c) 2017-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */
// override-include-guard

namespace facebook { namespace logdevice { namespace configuration {
namespace nodes {

template <typename Attributes, bool Mutable>
bool NodeAttributesConfig<Attributes, Mutable>::NodeUpdate::isValid() const {
  switch (transition) {
    case UpdateType::PROVISION:
      return attributes != nullptr && attributes->isValid();
    case UpdateType::REMOVE:
      return attributes == nullptr;
    case UpdateType::RESET:
      // only `mutable' type of NodeAttributesConfig support the RESET update
      return Mutable && attributes != nullptr && attributes->isValid();
  }
  ld_check(false);
  return false;
}

template <typename Attributes, bool Mutable>
bool NodeAttributesConfig<Attributes, Mutable>::Update::isValid() const {
  return !node_updates.empty() &&
      std::all_of(node_updates.cbegin(),
                  node_updates.cend(),
                  [](const auto& kv) { return kv.second.isValid(); });
}

template <typename Attributes, bool Mutable>
std::pair<bool, Attributes>
NodeAttributesConfig<Attributes, Mutable>::getNodeAttributes(
    node_index_t node) const {
  const auto nit = node_states_.find(node);
  if (nit == node_states_.cend()) {
    return std::make_pair(false, Attributes{});
  }
  return std::make_pair(true, nit->second);
}

template <typename Attributes, bool Mutable>
const Attributes*
NodeAttributesConfig<Attributes, Mutable>::getNodeAttributesPtr(
    node_index_t node) const {
  const auto nit = node_states_.find(node);
  return nit == node_states_.cend() ? nullptr : &nit->second;
}

template <typename Attributes, bool Mutable>
const Attributes& NodeAttributesConfig<Attributes, Mutable>::nodeAttributesAt(
    node_index_t node) const {
  ld_check(hasNode(node));
  return node_states_.at(node);
}

template <typename Attributes, bool Mutable>
void NodeAttributesConfig<Attributes, Mutable>::setNodeAttributes(
    node_index_t node,
    Attributes state) {
  node_states_[node] = state;
}

template <typename Attributes, bool Mutable>
bool NodeAttributesConfig<Attributes, Mutable>::eraseNodeAttribute(
    node_index_t node) {
  return node_states_.erase(node) > 0;
}

template <typename Attributes, bool Mutable>
int NodeAttributesConfig<Attributes, Mutable>::applyUpdate(
    const Update& update,
    NodeAttributesConfig* new_config_out) const {
  if (!update.isValid()) {
    err = E::INVALID_PARAM;
    return -1;
  }

  NodeAttributesConfig target_config(*this);
  for (const auto& kv : update.node_updates) {
    const node_index_t node = kv.first;
    const NodeUpdate& node_update = kv.second;

    bool node_exist;
    Attributes current_node_attributes;
    bool erased;
    std::tie(node_exist, current_node_attributes) = getNodeAttributes(node);

    if (!node_exist && node_update.transition != UpdateType::PROVISION) {
      err = E::NOTINCONFIG;
      return -1;
    }

    if (node_exist && node_update.transition == UpdateType::PROVISION) {
      err = E::EXISTS;
      return -1;
    }

    switch (node_update.transition) {
      case UpdateType::REMOVE:
        ld_check(node_exist);
        erased = target_config.eraseNodeAttribute(node);
        ld_check(erased);
        break;
      case UpdateType::RESET:
        ld_check(node_exist);
        ld_check(node_update.attributes != nullptr);
        ld_check(node_update.attributes->isValid());
        FOLLY_FALLTHROUGH;
      case UpdateType::PROVISION:
        target_config.setNodeAttributes(node, *node_update.attributes);
        break;
    }
  }

  if (!target_config.validate()) {
    err = E::INVALID_CONFIG;
    return -1;
  }

  if (new_config_out != nullptr) {
    *new_config_out = target_config;
  }

  dcheckConsistency();
  return 0;
}

template <typename Attributes, bool Mutable>
bool NodeAttributesConfig<Attributes, Mutable>::validate() const {
  for (const auto& kv : node_states_) {
    if (!kv.second.isValid()) {
      return false;
    }
  }
  return attributeSpecificValidate();
}

template <typename Attributes, bool Mutable>
void NodeAttributesConfig<Attributes, Mutable>::dcheckConsistency() const {
  ld_assert(validate());
}

template <typename Attributes, bool Mutable>
bool NodeAttributesConfig<Attributes, Mutable>::
operator==(const NodeAttributesConfig& rhs) const {
  return node_states_ == rhs.node_states_;
}

}}}} // namespace facebook::logdevice::configuration::nodes
