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

template <typename Attributes>
bool NodeAttributesConfig<Attributes>::NodeUpdate::isValid() const {
  switch (transition) {
    case UpdateType::PROVISION:
      return attributes != nullptr && attributes->isValid();
    case UpdateType::REMOVE:
      return attributes == nullptr;
    case UpdateType::RESET:
      return attributes != nullptr && attributes->isValid();
  }
  ld_check(false);
  return false;
}

template <typename Attributes>
std::string NodeAttributesConfig<Attributes>::NodeUpdate::toString() const {
  std::string t_str;
  switch (transition) {
    case UpdateType::PROVISION:
      t_str = "PR";
      break;
    case UpdateType::REMOVE:
      t_str = "RM";
      break;
    case UpdateType::RESET:
      t_str = "RS";
      break;
  }
  return folly::sformat(
      "[T:{},A:{}]", t_str, attributes ? attributes->toString() : "");
}

template <typename Attributes>
bool NodeAttributesConfig<Attributes>::Update::isValid() const {
  return !node_updates.empty() &&
      std::all_of(node_updates.cbegin(),
                  node_updates.cend(),
                  [](const auto& kv) { return kv.second.isValid(); });
}

template <typename Attributes>
std::string NodeAttributesConfig<Attributes>::Update::toString() const {
  return logdevice::toString(node_updates);
}

template <typename Attributes>
folly::Optional<Attributes>
NodeAttributesConfig<Attributes>::getNodeAttributes(node_index_t node) const {
  const auto nit = node_states_.find(node);
  if (nit == node_states_.cend()) {
    return folly::none;
  }
  return nit->second;
}

template <typename Attributes>
const Attributes* NodeAttributesConfig<Attributes>::getNodeAttributesPtr(
    node_index_t node) const {
  const auto nit = node_states_.find(node);
  return nit == node_states_.cend() ? nullptr : &nit->second;
}

template <typename Attributes>
const Attributes&
NodeAttributesConfig<Attributes>::nodeAttributesAt(node_index_t node) const {
  ld_check(hasNode(node));
  return node_states_.at(node);
}

template <typename Attributes>
void NodeAttributesConfig<Attributes>::setNodeAttributes(node_index_t node,
                                                         Attributes state) {
  node_states_[node] = state;
}

template <typename Attributes>
bool NodeAttributesConfig<Attributes>::eraseNodeAttribute(node_index_t node) {
  return node_states_.erase(node) > 0;
}

template <typename Attributes>
int NodeAttributesConfig<Attributes>::applyUpdate(
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

    bool erased;
    auto current_node_attributes = getNodeAttributes(node);
    bool node_exist = current_node_attributes.hasValue();

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
        if (!node_update.attributes->isValidForReset(
                *current_node_attributes)) {
          err = E::INVALID_PARAM;
          return -1;
        }
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

template <typename Attributes>
bool NodeAttributesConfig<Attributes>::validate() const {
  for (const auto& kv : node_states_) {
    if (!kv.second.isValid()) {
      return false;
    }
  }
  return attributeSpecificValidate();
}

template <typename Attributes>
void NodeAttributesConfig<Attributes>::dcheckConsistency() const {
  ld_assert(validate());
}

template <typename Attributes>
bool NodeAttributesConfig<Attributes>::
operator==(const NodeAttributesConfig& rhs) const {
  return node_states_ == rhs.node_states_;
}

}}}} // namespace facebook::logdevice::configuration::nodes
